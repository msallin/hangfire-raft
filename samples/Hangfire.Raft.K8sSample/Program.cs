// Kubernetes-ready ASP.NET host for Hangfire.Raft: a Hangfire server + dashboard on top of a Raft
// cluster, no database. Each pod derives its stable identity from the StatefulSet pod name and the
// headless service (see deploy/kubernetes/hangfire-raft.yaml). Run outside Kubernetes with a plain
// `dotnet run` and it falls back to a durable single-node cluster on loopback.
using Hangfire;
using Hangfire.Dashboard;
using Hangfire.Raft;

var builder = WebApplication.CreateBuilder(args);

var raftOptions = BuildRaftOptions();
Console.WriteLine($"Starting Raft node {raftOptions.SelfEndpoint} (WAL: {raftOptions.WalPath})");
var storage = await RaftJobStorage.StartAsync(raftOptions);

builder.Services.AddHangfire(cfg => cfg.UseStorage(storage));
builder.Services.AddHangfireServer(o => o.WorkerCount = 4);

var app = builder.Build();

// Probe target for the Kubernetes readiness check. This only reports that the host is up; it does
// not yet reflect cluster health (no leader-aware health surface exists in the library). See the
// "limitations" section of docs/kubernetes.md.
app.MapGet("/health", () => Results.Ok("ok"));
app.MapGet("/", () => Results.Text("Hangfire.Raft Kubernetes sample. Dashboard at /dashboard."));

// DEMO ONLY: the dashboard is exposed without authentication so it is reachable through a Service
// or `kubectl port-forward`. Replace AllowAllDashboardAuthorization with a real authorization
// filter before exposing this anywhere untrusted.
app.UseHangfireDashboard("/dashboard", new DashboardOptions
{
    Authorization = [new AllowAllDashboardAuthorization()],
});

// A little demo work so the dashboard is not empty.
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
    var svc = Environment.GetEnvironmentVariable("RAFT_SERVICE") ?? "hangfire";
    var replicas = int.Parse(Environment.GetEnvironmentVariable("RAFT_REPLICAS") ?? "3");
    string Fqdn(string host) => $"{host}.{svc}.{ns}.svc.cluster.local:{raftPort}";

    var options = new RaftStorageOptions
    {
        SelfEndpoint = Fqdn(pod),
        WalPath = Environment.GetEnvironmentVariable("RAFT_WAL_PATH") ?? "/data/wal",
    };
    for (var i = 0; i < replicas; i++) options.Members.Add(Fqdn($"{svc}-{i}"));
    return options;
}

/// <summary>Demo jobs invoked by the recurring schedule.</summary>
public static class Jobs
{
    public static void Heartbeat() => Console.WriteLine($"[heartbeat] {DateTime.UtcNow:O}");
}

/// <summary>DEMO ONLY authorization filter that allows every dashboard request. Do not use as-is.</summary>
public sealed class AllowAllDashboardAuthorization : IDashboardAuthorizationFilter
{
    public bool Authorize(DashboardContext context) => true;
}
