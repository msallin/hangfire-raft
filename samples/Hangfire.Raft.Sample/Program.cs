// Demo for Hangfire.Raft: runs a Hangfire server on top of a Raft cluster, no database.
//
//   dotnet run                  single-node cluster (still durable via the write-ahead log)
//   dotnet run -- 0             node 0 of a three-node cluster on localhost
//   dotnet run -- 1             node 1 (run each node in its own terminal)
//   dotnet run -- 2             node 2
//   dotnet run -- single 20     single node that exits after 20 seconds (scripted demos)
using Hangfire;
using Hangfire.Raft;
using Hangfire.Raft.Sample;

const int basePort = 4100;

var nodeArg = args.Length > 0 ? args[0] : "single";

int? runForSeconds = null;
if (args.Length > 1)
{
    if (!int.TryParse(args[1], out var parsedSeconds) || parsedSeconds <= 0)
        throw new ArgumentException("The second argument must be a positive number of seconds, e.g. `dotnet run -- single 20`.");
    runForSeconds = parsedSeconds;
}

int nodeIndex;
if (nodeArg == "single")
{
    nodeIndex = -1;
}
else if (!int.TryParse(nodeArg, out nodeIndex) || nodeIndex is < 0 or > 2)
{
    // This demo wires up a fixed three-node cluster, so a node index outside 0..2 would build a
    // SelfEndpoint that is not in Members and fail to start with a confusing error.
    throw new ArgumentException("The first argument must be 'single' or a node index 0, 1, or 2, e.g. `dotnet run -- 0`.");
}

var options = new RaftStorageOptions
{
    SelfEndpoint = nodeIndex < 0 ? $"127.0.0.1:{basePort}" : $"127.0.0.1:{basePort + nodeIndex * 2}",
    WalPath = Path.Combine(AppContext.BaseDirectory, "wal", nodeIndex < 0 ? "single" : $"node{nodeIndex}"),
};
if (nodeIndex < 0)
{
    options.Members.Add($"127.0.0.1:{basePort}");
}
else
{
    for (var i = 0; i < 3; i++) options.Members.Add($"127.0.0.1:{basePort + i * 2}");
}

Console.WriteLine($"Starting Raft storage node {options.SelfEndpoint} (WAL: {options.WalPath})");
await using var storage = await RaftJobStorage.StartAsync(options);
GlobalConfiguration.Configuration.UseStorage(storage);

using var server = new BackgroundJobServer(new BackgroundJobServerOptions
{
    ServerName = $"sample-{options.SelfEndpoint.Replace(':', '-')}",
    WorkerCount = 4,
}, storage);
Console.WriteLine("Hangfire server started.");

BackgroundJob.Enqueue(() => Jobs.Say($"startup job from {Environment.MachineName}"));
BackgroundJob.Schedule(() => Jobs.Say("delayed by 10 seconds"), TimeSpan.FromSeconds(10));
RecurringJob.AddOrUpdate("ticker", () => Jobs.Say("recurring tick"), Cron.Minutely);

using var lifetime = new CancellationTokenSource();
if (runForSeconds is { } seconds) lifetime.CancelAfter(TimeSpan.FromSeconds(seconds));
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    lifetime.Cancel();
};

var counter = 0;
while (!lifetime.IsCancellationRequested)
{
    try
    {
        await Task.Delay(TimeSpan.FromSeconds(5), lifetime.Token);
    }
    catch (OperationCanceledException)
    {
        break;
    }

    var jobNumber = ++counter;
    BackgroundJob.Enqueue(() => Jobs.Say($"periodic job #{jobNumber}"));

    var stats = storage.GetMonitoringApi().GetStatistics();
    Console.WriteLine($"[stats] servers={stats.Servers} enqueued={stats.Enqueued} processing={stats.Processing} succeeded={stats.Succeeded} recurring={stats.Recurring}");
}

Console.WriteLine("Shutting down.");

namespace Hangfire.Raft.Sample
{
    public static class Jobs
    {
        public static void Say(string message) => Console.WriteLine($"[job] {DateTime.Now:HH:mm:ss} {message}");
    }
}
