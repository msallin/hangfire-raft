# Running Hangfire.Raft on Kubernetes

Run the cluster as a **StatefulSet** (not a Deployment) behind a **headless Service**, with a
**per-pod PersistentVolume** for the write-ahead log and an **odd replica count**. Each pod becomes
both a Hangfire client/server and a Raft cluster member.

This directory ships a working example:

- [`samples/Hangfire.Raft.K8sSample`](../samples/Hangfire.Raft.K8sSample) — an ASP.NET host that
  runs a Hangfire server + dashboard on top of the Raft storage and derives its identity from the
  pod environment (it also runs as a single node with a plain `dotnet run` outside Kubernetes).
- [`samples/Hangfire.Raft.K8sSample/Dockerfile`](../samples/Hangfire.Raft.K8sSample/Dockerfile) — image build.
- [`deploy/kubernetes/hangfire-raft.yaml`](../deploy/kubernetes/hangfire-raft.yaml) — Namespace,
  headless Service, StatefulSet, dashboard Service and PodDisruptionBudget.
- [`deploy/kubernetes/minikube.yaml`](../deploy/kubernetes/minikube.yaml) — a single-node variant for
  local testing on minikube (see "Local testing with minikube" below).

## How the library maps onto Kubernetes

| The library needs | Kubernetes primitive |
|---|---|
| Stable per-node identity (`SelfEndpoint`), every node reachable by name | StatefulSet + headless Service → `hangfire-0.hangfire.<ns>.svc.cluster.local` |
| Node-local, persistent WAL (`WalPath`) | `volumeClaimTemplates` (one PVC per pod) |
| Static membership, identical `Members` list on every node | derived from the replica count, baked into config |
| Two ports per node (Raft, and forwarding = Raft + `RpcPortOffset`) | two `containerPort`s, both pod-to-pod |
| Odd node count for quorum | `replicas: 3` |
| All nodes elect a leader together at boot | `podManagementPolicy: Parallel` |

## Deploy

```bash
# 1. Build and push the image (from the repository root).
docker build -f samples/Hangfire.Raft.K8sSample/Dockerfile -t <your-registry>/hangfire-raft-sample:latest .
docker push <your-registry>/hangfire-raft-sample:latest

# 2. Set image: in deploy/kubernetes/hangfire-raft.yaml, then apply.
kubectl apply -f deploy/kubernetes/hangfire-raft.yaml

# 3. Watch the cluster form.
kubectl -n jobs get pods -w

# 4. Open the dashboard.
kubectl -n jobs port-forward svc/hangfire-dashboard 8080:8080
#   then browse http://localhost:8080/dashboard
```

## Local testing with minikube

[`deploy/kubernetes/minikube.yaml`](../deploy/kubernetes/minikube.yaml) is a single-node variant: it
uses a locally built image (`imagePullPolicy: IfNotPresent`) and **soft** anti-affinity so all three
Raft nodes fit on one minikube node. Co-locating the replicas is not a real fault-tolerance test — a
single node failure takes the whole cluster — but it exercises bootstrap, DNS, per-pod WAL, election,
leader forwarding and job processing.

```bash
# Build the image and load it into minikube (host Docker required).
docker build -f samples/Hangfire.Raft.K8sSample/Dockerfile -t hangfire-raft-sample:local .
minikube image load hangfire-raft-sample:local

kubectl apply -f deploy/kubernetes/minikube.yaml
kubectl -n jobs rollout status statefulset/hangfire

# Confirm jobs are processing (the sample runs a per-minute "heartbeat" job).
kubectl -n jobs logs hangfire-0 | grep heartbeat

# Dashboard: port-forward, then browse http://localhost:8080/dashboard
kubectl -n jobs port-forward svc/hangfire-dashboard 8080:8080
```

For real node spreading, use `minikube start --nodes 3` with the production manifest instead. Tear
down with `kubectl delete -f deploy/kubernetes/minikube.yaml` (the per-pod PVCs persist; remove them
with `kubectl -n jobs delete pvc -l app=hangfire`).

### Automated end-to-end test

[`deploy/kubernetes/e2e-test.ps1`](../deploy/kubernetes/e2e-test.ps1) builds and deploys the sample,
then drives real scenarios against the cluster and asserts the storage guarantees (exactly-once under
load, cross-pod `DisableConcurrentExecution` serialization, leader failover, and, with
`-IncludeReschedule`, reschedule re-resolution). It exits non-zero if any scenario fails.

```powershell
pwsh deploy/kubernetes/e2e-test.ps1                       # core scenarios
pwsh deploy/kubernetes/e2e-test.ps1 -IncludeReschedule -Teardown
```

Requires docker, a running minikube, kubectl and the .NET SDK; it leaves the committed manifest
untouched (the image tag is injected into a temporary copy).

## How a pod finds its identity

The StatefulSet injects the pod name and namespace via the downward API; the app builds its endpoint
and the full member list from them (see `BuildRaftOptions` in the sample):

```text
SelfEndpoint = {POD_NAME}.{RAFT_SERVICE}.{POD_NAMESPACE}.svc.cluster.local:5000
Members      = {STATEFULSET}-{0..RAFT_REPLICAS-1}.{RAFT_SERVICE}.{POD_NAMESPACE}.svc.cluster.local:5000
               # STATEFULSET = POD_NAME up to its ordinal (hangfire-0 -> hangfire), which may differ from RAFT_SERVICE
```

`SelfEndpoint` matches exactly one `Members` entry (compared by host name, with no DNS lookup), which
the library validates at startup. Host names are kept as `DnsEndPoint`s that the Raft transport
resolves lazily and re-resolves on every reconnect.

## Load-bearing settings (do not drop these)

- **`publishNotReadyAddresses: true`** on the headless Service. Without it the pods cannot resolve
  each other until they are Ready, but they cannot become Ready until the cluster forms — a deadlock.
- **`podManagementPolicy: Parallel`** on the StatefulSet, so all replicas start together and can hold
  the initial election (the default `OrderedReady` waits for pod-0 to be Ready first, which never
  happens with a single node that needs a quorum).
- **`volumeClaimTemplates`**, not a shared volume. Each node must own its WAL; never point multiple
  pods at the same volume.
- **`podAntiAffinity`** so replicas land on different nodes; otherwise one node failure can take out
  the quorum.

## Resilience (handled automatically)

- **Rescheduled pods rejoin on their own.** Host names in `SelfEndpoint`/`Members` are kept as
  `DnsEndPoint`s rather than resolved to fixed IPs, and both the Raft transport and the
  command-forwarding channel re-resolve them on every reconnection. When a pod reschedules — same
  StatefulSet name, new IP — its peers reach it again at the new address with no rolling restart, and
  member identity (derived from the host name) is unchanged.

  The reintegration is not instant: a peer keeps reaching the **old** IP until its DNS cache expires,
  bounded by the headless service's record TTL — **30 seconds by default in CoreDNS**. Until the peers
  re-resolve, the rescheduled pod does not count toward quorum from their point of view, so the
  cluster's fault tolerance is reduced for up to ~one TTL after a reschedule: a *second* failure
  inside that window can pause writes until the TTL expires (verified on a 3-node cluster — writes
  resume on their own once peers re-resolve). For faster recovery, lower the CoreDNS `ttl` (or stage
  rolling updates so reschedules are at least one TTL apart).

- **Bootstrap tolerates not-yet-resolvable peers.** Members are resolved lazily, so a pod that starts
  before its peers have DNS records does not crash; the transport logs the unreachable member and
  retries, and the cluster converges once the records appear. (Keep `publishNotReadyAddresses: true`
  so those records are published promptly — it is what lets the nodes find each other before any of
  them is Ready.)

## Limitations and sharp edges

- **Scaling is a deliberate config change, not `kubectl scale`.** Membership is static and lives in
  each pod's `Members` list (driven by `RAFT_REPLICAS`). To change the cluster size, update both
  `replicas` and `RAFT_REPLICAS` (and `PodDisruptionBudget.minAvailable`) and roll the StatefulSet.
  Do not autoscale it.

- **Readiness reflects write-availability.** The sample's `/ready` endpoint (backed by
  `RaftJobStorage.GetHealth()`) returns 200 only when the node knows a leader — so it can submit or
  forward writes — and 503 otherwise; the `readinessProbe` points at it. Liveness is a separate TCP
  check on the Raft port, and `/health` stays a plain "host is up" signal.

- **Clocks.** Expirations compare the submitting node's UTC timestamp, so keep node clocks reasonably
  synchronized (kubelet nodes normally run NTP).

- **Dashboard auth.** The sample exposes the dashboard with an allow-all authorization filter so it is
  viewable through a Service; replace it with real authentication before exposing it to anyone
  untrusted.
