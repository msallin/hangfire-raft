// Kubernetes-ready ASP.NET host for Hangfire.Raft: a Hangfire server + dashboard on top of a Raft
// cluster, no database. Each pod derives its stable identity from the StatefulSet pod name and the
// headless service (see deploy/kubernetes/hangfire-raft.yaml). Run outside Kubernetes with a plain
// `dotnet run` and it falls back to a durable single-node cluster on loopback.
//
// Trigger endpoints (POST unless noted) let you exercise different job kinds against the cluster:
//   /enqueue/{n}     n fire-and-forget jobs, each incrementing a replicated "executions" counter
//   /delayed/{secs}  a job scheduled secs into the future
//   /flaky           a job that fails twice then succeeds (exercises automatic retry)
//   /boom            a job that always throws (lands in the Failed state, no retry)
//   /exclusive/{n}   n jobs contending on one DisableConcurrentExecution lock (must serialize)
//   /stats   (GET)   replicated demo counters + Hangfire statistics, served from the local node
using System.Net;
using Hangfire;
using Hangfire.Dashboard;
using Hangfire.Raft;
using Hangfire.Server;
using Hangfire.Storage;

var builder = WebApplication.CreateBuilder(args);

var raftOptions = BuildRaftOptions();
var nodeName = Environment.GetEnvironmentVariable("POD_NAME") ?? Dns.GetHostName();
Console.WriteLine($"Starting Raft node {raftOptions.SelfEndpoint} (WAL: {raftOptions.WalPath})");
var storage = await RaftJobStorage.StartAsync(raftOptions);

builder.Services.AddHangfire(cfg => cfg.UseStorage(storage));
builder.Services.AddHangfireServer(o => o.WorkerCount = 4);

var app = builder.Build();

// Liveness: the host process is up and the local state machine has not faulted. A fault (a committed
// entry that will not apply, or an unreadable snapshot) means the node's on-disk state is corrupt or
// incompatible and it cannot recover on its own, so we fail liveness to have Kubernetes recycle the pod;
// per-node recovery is to clear this node's WAL volume so it re-syncs a fresh snapshot from the leader.
app.MapGet("/health", () => storage.GetHealth().Faulted
    ? Results.StatusCode(StatusCodes.Status503ServiceUnavailable)
    : Results.Ok("ok"));

// Readiness: the node can serve writes (the cluster has a leader it can reach or forward to) and its
// state machine is healthy. A leaderless or faulted node is pulled from the Service endpoints.
app.MapGet("/ready", () =>
{
    var health = storage.GetHealth();
    return health.HasLeader && !health.Faulted
        ? Results.Ok("ready")
        : Results.StatusCode(StatusCodes.Status503ServiceUnavailable);
});

app.MapPost("/enqueue/{n:int}", (int n) =>
{
    for (var i = 0; i < n; i++) BackgroundJob.Enqueue(() => Jobs.Process(i));
    return Results.Ok($"enqueued {n}");
});

app.MapPost("/delayed/{secs:int}", (int secs) =>
{
    var id = BackgroundJob.Schedule(() => Jobs.Process(-1), TimeSpan.FromSeconds(secs));
    return Results.Ok($"scheduled {id} in {secs}s");
});

app.MapPost("/flaky", () => Results.Ok(BackgroundJob.Enqueue(() => Jobs.Flaky(null!))));
app.MapPost("/boom", () => Results.Ok(BackgroundJob.Enqueue(() => Jobs.Boom())));

app.MapPost("/exclusive/{n:int}", (int n) =>
{
    for (var i = 0; i < n; i++) BackgroundJob.Enqueue(() => Jobs.Exclusive(1)); // same arg -> one lock -> serialized
    return Results.Ok($"enqueued {n} exclusive");
});

app.MapGet("/stats", () =>
{
    using var connection = (JobStorageConnection)storage.GetConnection();
    var monitor = storage.GetMonitoringApi();
    var stats = monitor.GetStatistics();
    return Results.Json(new
    {
        node = nodeName,
        leader = storage.GetHealth().IsLeader,
        executions = connection.GetCounter("demo:executions"),
        processed = connection.GetSetCount("demo:processed"),
        stats.Enqueued,
        stats.Processing,
        stats.Scheduled,
        stats.Succeeded,
        stats.Failed,
        stats.Servers,
    });
});

app.MapGet("/", () => Results.Text("Hangfire.Raft Kubernetes sample. Dashboard at /dashboard."));

// DEMO ONLY: the dashboard is exposed without authentication so it is reachable through a Service
// or `kubectl port-forward`. Replace AllowAllDashboardAuthorization with a real authorization filter
// before exposing this anywhere untrusted.
app.UseHangfireDashboard("/dashboard", new DashboardOptions
{
    Authorization = [new AllowAllDashboardAuthorization()],
});

RecurringJob.AddOrUpdate("heartbeat", () => Jobs.Heartbeat(), Cron.Minutely);

// Leave the cluster gracefully on shutdown so the write-ahead log is closed cleanly.
app.Lifetime.ApplicationStopping.Register(() => storage.DisposeAsync().AsTask().GetAwaiter().GetResult());

app.Run("http://0.0.0.0:8080");

static RaftStorageOptions BuildRaftOptions()
{
    const int raftPort = 5000;
    var pod = Environment.GetEnvironmentVariable("POD_NAME");

    // Outside Kubernetes (plain `dotnet run`): a durable single-node cluster on loopback.
    if (string.IsNullOrEmpty(pod))
    {
        var local = new RaftStorageOptions
        {
            SelfEndpoint = $"127.0.0.1:{raftPort}",
            WalPath = Path.Combine(AppContext.BaseDirectory, "wal"),
        };
        local.Members.Add($"127.0.0.1:{raftPort}");
        return local;
    }

    // In Kubernetes: derive stable identities from the StatefulSet pod name and headless service.
    // For pod "hangfire-0" in namespace "jobs" with service "hangfire", SelfEndpoint becomes
    // "hangfire-0.hangfire.jobs.svc.cluster.local:5000" and Members lists all replicas the same way.
    var ns = Environment.GetEnvironmentVariable("POD_NAMESPACE") ?? "default";
    var svc = Environment.GetEnvironmentVariable("RAFT_SERVICE") ?? "hangfire"; // headless service = the pod-name subdomain
    var replicas = int.Parse(Environment.GetEnvironmentVariable("RAFT_REPLICAS") ?? "3");
    string Fqdn(string host) => $"{host}.{svc}.{ns}.svc.cluster.local:{raftPort}";

    // Members share this pod's StatefulSet name (the part of POD_NAME before the ordinal), so Self is
    // always one of them even if RAFT_SERVICE differs from the StatefulSet name.
    var statefulSet = pod[..pod.LastIndexOf('-')];

    var options = new RaftStorageOptions
    {
        SelfEndpoint = Fqdn(pod),
        WalPath = Environment.GetEnvironmentVariable("RAFT_WAL_PATH") ?? "/data/wal",
    };
    for (var i = 0; i < replicas; i++) options.Members.Add(Fqdn($"{statefulSet}-{i}"));
    return options;
}

/// <summary>Demo jobs covering the common Hangfire job shapes, used by the trigger endpoints.</summary>
public static class Jobs
{
    public static void Heartbeat() => Console.WriteLine($"[heartbeat] {DateTime.UtcNow:O}");

    /// <summary>Records its execution in replicated state so a test can verify exactly-once across the cluster.</summary>
    public static void Process(int i)
    {
        using var connection = JobStorage.Current.GetConnection();
        using var transaction = connection.CreateWriteTransaction();
        transaction.IncrementCounter("demo:executions"); // total executions; > enqueued count would mean double-processing
        transaction.AddToSet("demo:processed", i.ToString());
        transaction.Commit();
        Console.WriteLine($"[process] {i}");
    }

    /// <summary>Fails the first two attempts and succeeds on the third, exercising automatic retry.</summary>
    public static void Flaky(PerformContext context)
    {
        var attempt = context.GetJobParameter<int>("RetryCount"); // 0 on the first run, then 1, 2, ...
        Console.WriteLine($"[flaky] attempt {attempt + 1}");
        if (attempt < 2) throw new InvalidOperationException("transient failure");
        Console.WriteLine("[flaky] succeeded");
    }

    /// <summary>Always throws; with no retries it lands directly in the Failed state.</summary>
    [AutomaticRetry(Attempts = 0)]
    public static void Boom() => throw new InvalidOperationException("permanent failure");

    /// <summary>Holds a cluster-wide lock while running, so concurrent invocations must serialize.</summary>
    [DisableConcurrentExecution(timeoutInSeconds: 60)]
    public static void Exclusive(int key)
    {
        Console.WriteLine($"[exclusive {key}] enter {DateTime.UtcNow:O}");
        Thread.Sleep(800);
        Console.WriteLine($"[exclusive {key}] exit  {DateTime.UtcNow:O}");
    }
}

/// <summary>DEMO ONLY authorization filter that allows every dashboard request. Do not use as-is.</summary>
public sealed class AllowAllDashboardAuthorization : IDashboardAuthorizationFilter
{
    public bool Authorize(DashboardContext context) => true;
}
