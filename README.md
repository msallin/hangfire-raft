# Hangfire.Raft

[![CI](https://github.com/msallin/hangfire-raft/actions/workflows/ci.yml/badge.svg)](https://github.com/msallin/hangfire-raft/actions/workflows/ci.yml)
[![NuGet](https://img.shields.io/nuget/v/Hangfire.Community.Raft.svg)](https://www.nuget.org/packages/Hangfire.Community.Raft)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/msallin/hangfire-raft/blob/main/LICENSE)

Hangfire job storage backed by a [DotNext](https://github.com/dotnet/dotNext) Raft cluster.
Job state lives in replicated memory; durability comes from a per-node write-ahead log and
snapshots on local disk. No SQL Server, no Redis, no external database.

Each application node is simultaneously a Hangfire client/server and a Raft cluster member.
A cluster of one works too and still survives restarts through the WAL.

## Quick start

```csharp
var options = new RaftStorageOptions
{
    SelfEndpoint = "10.0.0.1:7000",                  // this node's Raft endpoint
    WalPath = "/var/lib/myapp/hangfire-raft",        // node-local persistent directory
};
options.Members.Add("10.0.0.1:7000");                // identical list on every node,
options.Members.Add("10.0.0.2:7000");                // including the node itself
options.Members.Add("10.0.0.3:7000");

await using var storage = await RaftJobStorage.StartAsync(options);
GlobalConfiguration.Configuration.UseStorage(storage);

using var server = new BackgroundJobServer(storage);
BackgroundJob.Enqueue(() => Console.WriteLine("Hello from the cluster"));
```

Every node needs two reachable TCP ports: the Raft port you configure and the command
forwarding port right above it (`port + RpcPortOffset`, default `+1`).

The dashboard works as usual (`app.UseHangfireDashboard()` after `UseStorage`); it reads from
the local node's replica.

Try it locally with the sample:

```bash
dotnet run --project samples/Hangfire.Raft.Sample              # single node
dotnet run --project samples/Hangfire.Raft.Sample -- 0         # three terminals: nodes 0, 1, 2
```

## How it works

```text
Hangfire API call (enqueue, state change, fetch, lock, ...)
        |
        v
   Command (binary-serialized batch of ops)
        |
        |  leader?  ->  append to Raft log  ->  replicate to majority  ->  commit
        |  follower? -> forward over TCP to the leader, which appends/commits
        v
   every node applies the committed entry to its in-memory store (deterministic)
        |
        v
   the submitting node waits for ITS OWN apply, then returns the result
```

* **Writes** are Raft log entries. A write returns only after it is committed by a majority
  and applied by the local node, which gives every node read-your-writes consistency.
* **Reads** (job data, sets, monitoring) are served from the local replica without consensus.
  They can trail the leader by a replication heartbeat; Hangfire's components are designed
  for storages with this property.
* **Fetching a job is a consensus operation**, so a job is handed to exactly one worker across
  the whole cluster. Fetched jobs are held under a lease that the worker renews in the
  background; if a node dies mid-processing, the lease expires and the next leader maintenance
  pass requeues the job (so after `FetchInvisibilityTimeout` plus up to `MaintenanceInterval`,
  and only while a leader has quorum) — at-least-once execution, the same model as the SQL
  storage's sliding invisibility timeout.
* **Distributed locks** are replicated leases renewed by the holder. A crashed holder's lock
  frees itself when the lease expires.
* **Time**: the state machine never reads the local clock. Every command carries the
  submitter's UTC timestamp, so every replica applies the same updates and converges to the
  same logical state (snapshot byte streams may differ in map ordering; the replicated data does
  not). Keep node clocks reasonably synchronized (NTP) because expirations compare those timestamps.
* **Durability**: a write is flushed to the submitting node's write-ahead log on `WalPath` before
  it is acknowledged, so an acknowledged write survives a crash of that node; the log is
  periodically compacted into snapshots, and on restart a node replays snapshot + log before
  serving, then catches up from the leader. On a multi-node cluster the synchronous flush covers
  the node that handled the write, while its peers persist the entry through a background flush a
  moment later, so a simultaneous crash of the whole committing majority before that background
  flush (for example a single-rack power loss) can still lose a just-committed entry — spread
  members across failure domains.
* **Maintenance**: the current leader periodically evicts expired jobs/sets/hashes/lists/
  counters, drops expired lock leases and requeues stale fetches.

## Configuration

| Option | Default | Meaning |
|---|---|---|
| `SelfEndpoint` | required | This node's Raft endpoint (`host:port`). |
| `Members` | required | Raft endpoints of all members, identical on every node, including self. |
| `WalPath` | `<app>/hangfire-raft` | Node-local directory for log + snapshots. |
| `RpcPortOffset` | `1` | Forwarding port = Raft port + offset. |
| `SubmitTimeout` | 30 s | Max time for a single write (replication + local apply). |
| `LockLeaseTimeout` | 2 min | Distributed lock lease; renewed at a third of it. |
| `FetchInvisibilityTimeout` | 5 min | A crashed worker's job becomes fetchable again on the first maintenance pass after this (so up to `+ MaintenanceInterval`, and only with quorum). |
| `MaintenanceInterval` | 30 s | Leader cleanup cadence. |
| `SnapshotInterval` | 4096 | Applied log entries between state-machine snapshots; the log compacts up to each snapshot. A tuning/testing knob. |
| `LowerElectionTimeoutMs` / `UpperElectionTimeoutMs` | 1500 / 3000 | Raft election timeouts. |
| `LoggerFactory` | none | Diagnostics for the cluster and storage. |

## Operational notes

* Run an **odd number of nodes** (1, 3, 5). Writes need a majority: a 3-node cluster
  tolerates one node down; with two down, writes (including job processing) pause until
  quorum returns, then resume. Reads and the dashboard keep working on live nodes.
* **Membership is static**: every node lists the same `Members`. Replacing a node means
  restarting the cluster processes with the updated list (the WAL keeps the data).
* The full job state must **fit in memory** on every node. The dataset is bounded by your
  job retention (`ExpireJob` defaults to 24 h in Hangfire) plus recurring job metadata.
* Throughput: every write is a consensus round-trip, and a fetch is a write. This is
  comfortable for typical background-job workloads (hundreds of writes/second on a LAN), but
  it is not a Redis replacement for six-figure jobs-per-minute setups.
* Hangfire's storage API is synchronous, so each write blocks a worker thread while the cluster
  replicates, applies and flushes it on the thread pool. Under many concurrent workers, raise the
  thread-pool floor (`ThreadPool.SetMinThreads`) so those continuations are not starved and a write
  does not stall to `SubmitTimeout`; the default floor grows by only ~1 thread/second.
* Job execution is **at-least-once**, like other Hangfire storages: a worker that dies after
  performing a job but before acknowledging it leads to a retry after the invisibility
  timeout. For the same reason a write whose commit is ambiguous (the acknowledgement was lost)
  is retried by Hangfire under a fresh command, so non-idempotent effects — including the
  dashboard's success/failure **stat counters** — can be applied twice and drift under outages.
  Keep jobs idempotent.
* **Observability**: `GetHealth()` reports `AppliedIndex` and `CommitIndex`; their difference is the
  local apply lag, which lets a readiness probe detect a node serving stale reads even while it
  still sees a leader. A `Hangfire.Raft` meter publishes counters for ambiguous writes, fetch-lease
  reclaims (possible duplicate executions) and lock losses, for an OpenTelemetry pipeline or
  `dotnet-counters`.
* Locks are not reentrant: acquiring the same resource twice from the same connection
  deadlocks until the timeout, as with most Hangfire storages.
* A distributed lock is a **lease, not a fence**: a holder that cannot reach the cluster for
  longer than `LockLeaseTimeout` may lose the lock to another owner while still executing its
  critical section. The renewal loop logs a warning when this happens, but there is no fencing
  token, so do not rely on the lock for correctness of non-idempotent external side effects.

## Security and network trust model

**Both cluster ports must be confined to a trusted private network.** Each node listens on its
Raft port and on the command-forwarding port (`Raft port + RpcPortOffset`). Neither is
authenticated or encrypted (this matches the default posture of the underlying DotNext Raft
transport), so any host that can reach them can participate in consensus and submit storage
writes.

This matters more than for a typical service because **Hangfire executes serialized job
payloads**: anyone who can submit a write can enqueue a job that runs arbitrary code on a worker
— the same exposure as write access to any Hangfire storage (SQL Server, Redis, …). Run the
cluster on a private subnet, VPC, or overlay network, and never expose these ports to untrusted
clients. Undecodable forwarded commands are rejected before they enter the log, but that is a
robustness guard, not an authentication boundary.

## Kubernetes

Run the cluster as a StatefulSet behind a headless Service with a per-pod PersistentVolume for the
WAL. Host names are kept as `DnsEndPoint`s and re-resolved on reconnect, so rescheduled pods rejoin on
their own (within ~one DNS TTL) and startup tolerates not-yet-resolvable peers. See [docs/kubernetes.md](docs/kubernetes.md)
for the full guide. Ready-to-use pieces:

- [`deploy/kubernetes/hangfire-raft.yaml`](deploy/kubernetes/hangfire-raft.yaml) — Service,
  StatefulSet and PodDisruptionBudget for a 3-node cluster.
- [`samples/Hangfire.Raft.K8sSample`](samples/Hangfire.Raft.K8sSample) — a Hangfire server +
  dashboard host that derives its identity from the pod environment, with a Dockerfile.

## Project layout

```text
src/Hangfire.Raft             the storage implementation
  Commands/                   replicated op set + binary wire format
  State/                      deterministic in-memory store + snapshot format
  Cluster/                    DotNext state machine, Raft host, leader forwarding
  Monitoring/                 dashboard read API
tests/Hangfire.Raft.Tests     unit tests (store, serializer) + cluster integration tests
samples/Hangfire.Raft.Sample      runnable console demo (single node or 3-node localhost cluster)
samples/Hangfire.Raft.K8sSample   Kubernetes-ready ASP.NET host (Hangfire server + dashboard)
deploy/kubernetes             example manifests
docs/kubernetes.md            Kubernetes deployment guide
```

`dotnet test` runs everything, including tests that boot real single- and three-node clusters
on loopback ports.
