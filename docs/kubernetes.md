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

Expect one or two pod restarts on the first apply (see "Pods may restart once at first boot" below);
the cluster stabilizes after them. For real node spreading, use `minikube start --nodes 3` with the
production manifest instead. Tear down with `kubectl delete -f deploy/kubernetes/minikube.yaml` (the
per-pod PVCs persist; remove them with `kubectl -n jobs delete pvc -l app=hangfire`).

## How a pod finds its identity

The StatefulSet injects the pod name and namespace via the downward API; the app builds its endpoint
and the full member list from them (see `BuildRaftOptions` in the sample):

```
SelfEndpoint = {POD_NAME}.{RAFT_SERVICE}.{POD_NAMESPACE}.svc.cluster.local:5000
Members      = {RAFT_SERVICE}-{0..RAFT_REPLICAS-1}.{RAFT_SERVICE}.{POD_NAMESPACE}.svc.cluster.local:5000
```

`SelfEndpoint` resolves to the pod's own IP and matches exactly one `Members` entry, which is what the
library validates at startup.

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

## Limitations and sharp edges

- **Pod IP changes are the main caveat.** The library resolves each member's hostname to an IP once,
  at startup, and hands those fixed IPs to the Raft transport. Kubernetes gives StatefulSet pods
  stable *names* but not stable *IPs* — a pod that reschedules (node failure, rolling update,
  eviction) keeps its name and gets a new IP. The surviving peers keep dialing the old IP, so the
  rescheduled pod stays out of the cluster until **those peers** are themselves restarted. A 3-node
  cluster keeps serving on the remaining two (quorum holds), but its fault tolerance is gone until you
  rolling-restart (`kubectl -n jobs rollout restart statefulset/hangfire`) to re-resolve. The
  PodDisruptionBudget and anti-affinity make reschedules rare and one-at-a-time; for low-churn
  clusters and dev/test this is fine, but it is not self-healing. The proper fix is a library change
  to re-resolve member DNS on reconnect instead of pinning at startup.

- **Pods may restart once at first boot.** Member resolution happens once at startup and throws if a
  peer's DNS name is not resolvable yet. During parallel bootstrap a pod can finish starting before
  its peers have published DNS records, crash on the failed lookup, and be restarted by Kubernetes;
  it succeeds on the next attempt once the records exist. A fresh cluster typically shows one or two
  pods with `RESTARTS 1` that then stay stable. `publishNotReadyAddresses` shrinks this window but
  does not close it, and because the crash is an unhandled exception (the process exits) no probe
  tuning prevents it — the real fix is the same as above: retry/re-resolve member DNS instead of
  resolving once and throwing.

- **Scaling is a deliberate config change, not `kubectl scale`.** Membership is static and lives in
  each pod's `Members` list (driven by `RAFT_REPLICAS`). To change the cluster size, update both
  `replicas` and `RAFT_REPLICAS` (and `PodDisruptionBudget.minAvailable`) and roll the StatefulSet.
  Do not autoscale it.

- **Readiness is shallow.** There is no built-in leader-aware health endpoint yet, so `/health`
  returning 200 means "the host is up," not "the cluster has a leader." A readiness check that
  reflects write-availability needs a small health surface in the library.

- **Clocks.** Expirations compare the submitting node's UTC timestamp, so keep node clocks reasonably
  synchronized (kubelet nodes normally run NTP).

- **Dashboard auth.** The sample exposes the dashboard with an allow-all authorization filter so it is
  viewable through a Service; replace it with real authentication before exposing it to anyone
  untrusted.
