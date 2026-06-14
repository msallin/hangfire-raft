<#
.SYNOPSIS
  End-to-end test of Hangfire.Raft on a local minikube cluster.

.DESCRIPTION
  Builds the sample image, deploys a fresh 3-node cluster, and drives real scenarios against it,
  asserting the storage guarantees rather than eyeballing output:

    1. Exactly-once under load  - enqueue N jobs, assert the replicated execution counter rises by
                                  exactly N (a larger rise would mean double-processing).
    2. Cross-pod distributed lock - enqueue jobs guarded by DisableConcurrentExecution and assert
                                  their run intervals never overlap, even across pods.
    3. Leader failover          - kill the leader and assert writes still succeed.
    4. Reschedule re-resolution - (with -IncludeReschedule) reschedule a pod, wait past the DNS TTL,
                                  then kill a different pod and assert quorum holds.

  Requires: docker, minikube (running), kubectl, and the .NET SDK. The committed manifest is not
  modified; the chosen image tag is injected into a temporary copy.

.EXAMPLE
  pwsh deploy/kubernetes/e2e-test.ps1
  pwsh deploy/kubernetes/e2e-test.ps1 -IncludeReschedule -Teardown
  pwsh deploy/kubernetes/e2e-test.ps1 -SkipBuild        # reuse an already-loaded image
#>
[CmdletBinding()]
param(
    [string]$Tag = 'e2e',
    [string]$Namespace = 'jobs',
    [int]$JobCount = 50,
    [int]$LockJobs = 6,
    [switch]$SkipBuild,
    [switch]$IncludeReschedule,
    [switch]$Teardown
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
# Let native tools (kubectl/minikube/docker) signal status via their exit code, which this script
# checks where it matters, rather than throwing on every non-zero (e.g. a benign `kubectl wait` timeout).
$PSNativeCommandUseErrorActionPreference = $false

# Repo root is two levels up from this script (deploy/kubernetes/).
$repo = (Resolve-Path (Join-Path $PSScriptRoot '..' '..')).Path
$manifest = Join-Path $PSScriptRoot 'minikube.yaml'
$image = "hangfire-raft-sample:$Tag"
$results = [System.Collections.Generic.List[object]]::new()

function Find-Minikube {
    $cmd = Get-Command minikube -ErrorAction SilentlyContinue
    if ($cmd) { return $cmd.Source }
    $candidate = Join-Path $env:ProgramFiles 'Kubernetes\minikube\minikube.exe'
    if (Test-Path $candidate) { return $candidate }
    throw 'minikube not found on PATH or in Program Files.'
}
$minikube = Find-Minikube

function Step($msg) { Write-Host "`n=== $msg ===" -ForegroundColor Cyan }
function Record($name, $passed, $detail) {
    $results.Add([pscustomobject]@{ Scenario = $name; Passed = $passed; Detail = $detail })
    $tag = if ($passed) { 'PASS' } else { 'FAIL' }
    $color = if ($passed) { 'Green' } else { 'Red' }
    Write-Host ("[{0}] {1} - {2}" -f $tag, $name, $detail) -ForegroundColor $color
}
function Assert-LastExit($step) {
    # Native tools signal failure via exit code (PSNativeCommandUseErrorActionPreference is off), so the
    # critical deploy steps check it explicitly and stop instead of pressing on against a broken cluster.
    if ($LASTEXITCODE -ne 0) { throw "$step failed (exit code $LASTEXITCODE)." }
}

# --- HTTP helpers (drive the cluster through a port-forward) ---
function Start-Forward($target, $localPort) {
    $pf = Start-Process kubectl -ArgumentList "-n $Namespace port-forward $target $localPort`:8080" -PassThru -WindowStyle Hidden
    Start-Sleep -Seconds 4
    return $pf
}
function Post($localPort, $path, $timeoutSec = 30) {
    try { return (Invoke-WebRequest -Uri "http://127.0.0.1:$localPort$path" -Method Post -UseBasicParsing -TimeoutSec $timeoutSec).Content }
    catch { return $null } # the server completes the request even if the client disconnects
}
function Stats($localPort) {
    try { return (Invoke-WebRequest -Uri "http://127.0.0.1:$localPort/stats" -UseBasicParsing -TimeoutSec 6).Content | ConvertFrom-Json }
    catch { return $null }
}
function Wait-Stats($localPort, $timeoutSec = 30) {
    # Returns the first reachable /stats payload, tolerating a port-forward that is still warming up.
    $deadline = (Get-Date).AddSeconds($timeoutSec)
    do { $s = Stats $localPort; if ($s) { return $s }; Start-Sleep -Seconds 2 } while ((Get-Date) -lt $deadline)
    return $null
}
function Wait-PodReady($pod, $timeoutSec = 120) {
    # Polls instead of `kubectl wait`, which races a just-deleted pod ("not found") before the
    # StatefulSet recreates it.
    $deadline = (Get-Date).AddSeconds($timeoutSec)
    while ((Get-Date) -lt $deadline) {
        $ready = kubectl -n $Namespace get pod $pod -o 'jsonpath={.status.conditions[?(@.type=="Ready")].status}' 2>$null
        if ($ready -eq 'True') { return $true }
        Start-Sleep -Seconds 3
    }
    return $false
}

# --- deploy ---
if (-not $SkipBuild) {
    Step "Build solution"
    dotnet build (Join-Path $repo 'Hangfire.Raft.slnx') -c Release | Out-Null
    Assert-LastExit 'dotnet build'

    Step "Build image $image"
    docker build -f (Join-Path $repo 'samples\Hangfire.Raft.K8sSample\Dockerfile') -t $image $repo | Out-Null
    Assert-LastExit 'docker build'
}

Step "Reset namespace $Namespace"
kubectl delete namespace $Namespace --ignore-not-found --wait=true | Out-Null
Assert-LastExit 'kubectl delete namespace'

Step "Load image into minikube"
# Loading after the namespace is gone guarantees no running pod is pinning an older copy of the tag.
& $minikube image load $image | Out-Null
Assert-LastExit 'minikube image load'

Step "Deploy (image -> $image)"
# Inject the tag into a temporary copy so the committed manifest keeps its default :local image.
$rendered = (Get-Content $manifest -Raw) -replace 'image: hangfire-raft-sample:\S+', "image: $image"
$tempManifest = Join-Path ([System.IO.Path]::GetTempPath()) "hangfire-raft-e2e-$Tag.yaml"
Set-Content -Path $tempManifest -Value $rendered
kubectl apply -f $tempManifest | Out-Null
Assert-LastExit 'kubectl apply'
kubectl -n $Namespace rollout status statefulset/hangfire --timeout=180s
Assert-LastExit 'kubectl rollout status'

$pods = (kubectl -n $Namespace get pods -o jsonpath='{.items[*].metadata.name}').Split(' ')
Write-Host "pods: $($pods -join ', ')"

try {
    $pf = Start-Forward 'svc/hangfire-dashboard' 18080
    try {
        # --- Scenario 1: exactly-once under load ---
        Step "Scenario 1: exactly-once (enqueue $JobCount)"
        $s0 = Wait-Stats 18080
        $before = if ($s0) { $s0.executions } else { 0 }
        Post 18080 "/enqueue/$JobCount" 60 | Out-Null
        $target = $before + $JobCount
        $exec = $before
        $deadline = (Get-Date).AddSeconds(90)
        do { Start-Sleep -Seconds 3; $s = Stats 18080; if ($s) { $exec = $s.executions } } while ($exec -lt $target -and (Get-Date) -lt $deadline)
        Record 'exactly-once' ($exec -eq $target) "executions rose $before -> $exec (expected $target; > would mean double-processing)"

        # --- Scenario 2: cross-pod distributed lock ---
        Step "Scenario 2: distributed lock ($LockJobs contending jobs)"
        Post 18080 "/exclusive/$LockJobs" | Out-Null
        Start-Sleep -Seconds ([Math]::Max(10, $LockJobs * 2))
        $intervals = foreach ($p in $pods) {
            $log = kubectl -n $Namespace logs $p --since=60s 2>$null
            $enter = $null
            foreach ($line in $log) {
                if ($line -match 'exclusive 1\] enter (\S+)') { $enter = [datetime]::Parse($Matches[1], $null, [System.Globalization.DateTimeStyles]::RoundtripKind) }
                elseif ($line -match 'exclusive 1\] exit\s+(\S+)' -and $enter) {
                    [pscustomobject]@{ Enter = $enter; Exit = [datetime]::Parse($Matches[1], $null, [System.Globalization.DateTimeStyles]::RoundtripKind) }; $enter = $null
                }
            }
        }
        $sorted = @($intervals | Sort-Object Enter)
        $overlap = $false
        for ($i = 1; $i -lt $sorted.Count; $i++) { if ($sorted[$i].Enter -lt $sorted[$i - 1].Exit) { $overlap = $true } }
        Record 'distributed-lock' ($sorted.Count -ge $LockJobs -and -not $overlap) "$($sorted.Count) runs, overlap=$overlap (must be False)"

        # --- Scenario 3: leader failover ---
        Step "Scenario 3: leader failover"
        $leader = $null
        foreach ($p in $pods) {
            $ppf = Start-Forward "pod/$p" 18081
            try { $st = Stats 18081; if ($st -and $st.leader) { $leader = $p } } finally { Stop-Process -Id $ppf.Id -Force -ErrorAction SilentlyContinue }
            if ($leader) { break }
        }
        if ($leader) {
            # Drive the test through a survivor's own port-forward; the shared service forward would
            # break if it happened to target the leader we are about to kill.
            $survivor = $pods | Where-Object { $_ -ne $leader } | Select-Object -First 1
            $spf = Start-Forward "pod/$survivor" 18082
            try {
                $s = Wait-Stats 18082
                $before = if ($s) { $s.executions } else { 0 }
                kubectl -n $Namespace delete pod $leader --wait=false | Out-Null
                $deadline = (Get-Date).AddSeconds(40)
                while ((Get-Date) -lt $deadline) { if (Post 18082 '/enqueue/3' 8) { break }; Start-Sleep -Seconds 2 }
                $target = $before + 3; $exec = $before
                $deadline = (Get-Date).AddSeconds(30)
                do { Start-Sleep -Seconds 2; $s = Stats 18082; if ($s) { $exec = $s.executions } } while ($exec -lt $target -and (Get-Date) -lt $deadline)
                Record 'leader-failover' ($exec -eq $target) "killed leader $leader; executions $before -> $exec (expected $target)"
            }
            finally { Stop-Process -Id $spf.Id -Force -ErrorAction SilentlyContinue }
            Wait-PodReady $leader | Out-Null
        }
        else { Record 'leader-failover' $false 'could not identify a leader' }

        # --- Scenario 4 (optional): reschedule re-resolution ---
        if ($IncludeReschedule) {
            Step "Scenario 4: reschedule re-resolution (waits past the DNS TTL)"
            $victim = $pods[0]
            $other = $pods | Where-Object { $_ -ne $victim } | Select-Object -First 1
            $survivor = $pods | Where-Object { $_ -ne $victim -and $_ -ne $other } | Select-Object -First 1
            kubectl -n $Namespace delete pod $victim --wait=false | Out-Null
            Wait-PodReady $victim | Out-Null
            Start-Sleep -Seconds 60 # let peers re-resolve the new IP (bounded by the headless-service DNS TTL)
            $spf = Start-Forward "pod/$survivor" 18082
            try {
                $s = Wait-Stats 18082
                $before = if ($s) { $s.executions } else { 0 }
                kubectl -n $Namespace delete pod $other --wait=false | Out-Null
                $deadline = (Get-Date).AddSeconds(25); $exec = $before
                do { Start-Sleep -Seconds 2; Post 18082 '/enqueue/2' 6 | Out-Null; $s = Stats 18082; if ($s) { $exec = $s.executions } } while ($exec -lt ($before + 2) -and (Get-Date) -lt $deadline)
                Record 'reschedule-reresolution' ($exec -ge $before + 2) "after re-resolving $victim, killing $other kept quorum: executions $before -> $exec"
            }
            finally { Stop-Process -Id $spf.Id -Force -ErrorAction SilentlyContinue }
            Wait-PodReady $other | Out-Null
        }
    }
    finally { Stop-Process -Id $pf.Id -Force -ErrorAction SilentlyContinue }
}
finally {
    if ($Teardown) { Step "Teardown"; kubectl delete namespace $Namespace --ignore-not-found --wait=false | Out-Null }
    Remove-Item $tempManifest -ErrorAction SilentlyContinue
}

# --- summary ---
Step "Summary"
$results | Format-Table -AutoSize
$failed = @($results | Where-Object { -not $_.Passed }).Count
if ($failed -gt 0) { Write-Host "$failed scenario(s) FAILED" -ForegroundColor Red; exit 1 }
Write-Host "all scenarios passed" -ForegroundColor Green
