using System.Net;
using System.Net.Sockets;
using Hangfire.Common;
using Hangfire.States;
using Hangfire.Storage;

namespace Hangfire.Raft.Tests;

public static class TestJobs
{
    public static void Run(string argument)
    {
    }
}

/// <summary>
/// End-to-end tests against real Raft clusters on loopback: write-ahead log in a temp directory,
/// real elections, real TCP transport and command forwarding.
/// </summary>
public class RaftJobStorageTests : IDisposable
{
    private readonly string _walRoot = Path.Combine(Path.GetTempPath(), "hangfire-raft-tests", Guid.NewGuid().ToString("n"));
    private readonly List<RaftJobStorage> _storages = [];

    public void Dispose()
    {
        foreach (var storage in _storages)
        {
            try
            {
                storage.DisposeAsync().AsTask().GetAwaiter().GetResult();
            }
            catch (Exception)
            {
                // best-effort teardown
            }
        }

        try
        {
            if (Directory.Exists(_walRoot)) Directory.Delete(_walRoot, recursive: true);
        }
        catch (IOException)
        {
            // a WAL file may still be closing; temp cleanup is best-effort
        }
    }

    private async Task<RaftJobStorage> StartNode(int selfPort, int[] memberPorts, string? walPath = null, int recordsPerPartition = 4096, Action<RaftStorageOptions>? configure = null)
    {
        var options = new RaftStorageOptions
        {
            SelfEndpoint = $"127.0.0.1:{selfPort}",
            WalPath = walPath ?? Path.Combine(_walRoot, selfPort.ToString()),
            WalRecordsPerPartition = recordsPerPartition,
            LowerElectionTimeoutMs = 150,
            UpperElectionTimeoutMs = 300,
            SubmitTimeout = TimeSpan.FromSeconds(20),
        };
        foreach (var port in memberPorts) options.Members.Add($"127.0.0.1:{port}");
        configure?.Invoke(options);

        var storage = await RaftJobStorage.StartAsync(options);
        _storages.Add(storage);
        return storage;
    }

    private async Task<RaftJobStorage> StartSingleNode()
    {
        var port = AllocatePortPairs(1)[0];
        return await StartNode(port, [port]);
    }

    /// <summary>
    /// Reserves base ports where base (Raft) and base+1 (forwarding RPC) are both free, leaving a
    /// gap of 2 between members.
    /// </summary>
    private static int[] AllocatePortPairs(int count)
    {
        var random = Random.Shared;
        var result = new int[count];
        for (var i = 0; i < count; i++)
        {
            while (true)
            {
                var basePort = random.Next(21000, 59000);
                try
                {
                    var raft = new TcpListener(IPAddress.Loopback, basePort);
                    var rpc = new TcpListener(IPAddress.Loopback, basePort + 1);
                    raft.Start();
                    rpc.Start();
                    raft.Stop();
                    rpc.Stop();
                    result[i] = basePort;
                    break;
                }
                catch (SocketException)
                {
                    // port taken, try another
                }
            }
        }

        return result;
    }

    private static async Task PollUntil(Func<bool> condition, string description, int timeoutSeconds = 15)
    {
        var deadline = DateTime.UtcNow.AddSeconds(timeoutSeconds);
        while (!condition())
        {
            if (DateTime.UtcNow > deadline) Assert.Fail($"Timed out waiting for: {description}");
            await Task.Delay(100);
        }
    }

    [Fact]
    public async Task CreateExpiredJob_RoundtripsThroughGetJobData()
    {
        var storage = await StartSingleNode();
        using var connection = (JobStorageConnection)storage.GetConnection();

        var job = Job.FromExpression(() => TestJobs.Run("hello"));
        var createdAt = DateTime.UtcNow;
        var jobId = connection.CreateExpiredJob(job, new Dictionary<string, string> { ["Culture"] = "de-CH" }, createdAt, TimeSpan.FromDays(1));

        var data = connection.GetJobData(jobId);
        Assert.NotNull(data);
        Assert.Null(data.State);
        Assert.Equal(typeof(TestJobs), data.Job.Type);
        Assert.Equal("hello", data.Job.Args[0]);
        Assert.Equal("de-CH", connection.GetJobParameter(jobId, "Culture"));
    }

    [Fact]
    public async Task Transaction_EnqueueAndStateChange_AreAtomicAndVisible()
    {
        var storage = await StartSingleNode();
        using var connection = (JobStorageConnection)storage.GetConnection();

        var jobId = connection.CreateExpiredJob(Job.FromExpression(() => TestJobs.Run("x")), new Dictionary<string, string>(), DateTime.UtcNow, TimeSpan.FromDays(1));

        using (var transaction = connection.CreateWriteTransaction())
        {
            transaction.SetJobState(jobId, new EnqueuedState("default"));
            transaction.AddToQueue("default", jobId);
            transaction.Commit();
        }

        var state = connection.GetStateData(jobId);
        Assert.NotNull(state);
        Assert.Equal(EnqueuedState.StateName, state.Name);
        Assert.Equal("default", state.Data["Queue"]);

        var monitor = storage.GetMonitoringApi();
        Assert.Equal(1, ((Hangfire.Storage.JobStorageMonitor)monitor).EnqueuedCount("default"));
    }

    [Fact]
    public async Task FetchNextJob_ReturnsEnqueuedJob_AndAckRemovesIt()
    {
        var storage = await StartSingleNode();
        using var connection = (JobStorageConnection)storage.GetConnection();

        var jobId = connection.CreateExpiredJob(Job.FromExpression(() => TestJobs.Run("x")), new Dictionary<string, string>(), DateTime.UtcNow, TimeSpan.FromDays(1));
        using (var transaction = connection.CreateWriteTransaction())
        {
            transaction.AddToQueue("default", jobId);
            transaction.Commit();
        }

        using var fetched = connection.FetchNextJob(["default"], CancellationToken.None);
        Assert.Equal(jobId, fetched.JobId);
        fetched.RemoveFromQueue();

        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        Assert.ThrowsAny<OperationCanceledException>(() => connection.FetchNextJob(["default"], timeout.Token));
    }

    [Fact]
    public async Task FetchNextJob_WakesUp_WhenAJobIsEnqueuedLater()
    {
        var storage = await StartSingleNode();
        using var connection = (JobStorageConnection)storage.GetConnection();

        var enqueue = Task.Run(async () =>
        {
            await Task.Delay(300);
            using var other = (JobStorageConnection)storage.GetConnection();
            var jobId = other.CreateExpiredJob(Job.FromExpression(() => TestJobs.Run("late")), new Dictionary<string, string>(), DateTime.UtcNow, TimeSpan.FromDays(1));
            using var transaction = other.CreateWriteTransaction();
            transaction.AddToQueue("default", jobId);
            transaction.Commit();
        });

        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        using var fetched = connection.FetchNextJob(["default"], timeout.Token);
        Assert.NotNull(fetched);
        fetched.RemoveFromQueue();
        await enqueue;
    }

    [Fact]
    public async Task DistributedLock_IsMutuallyExclusive_AcrossConnections()
    {
        var storage = await StartSingleNode();
        using var connection1 = (JobStorageConnection)storage.GetConnection();
        using var connection2 = (JobStorageConnection)storage.GetConnection();

        var handle = connection1.AcquireDistributedLock("locks:test", TimeSpan.FromSeconds(5));
        Assert.Throws<DistributedLockTimeoutException>(() => connection2.AcquireDistributedLock("locks:test", TimeSpan.FromSeconds(1)));

        handle.Dispose();
        using var reacquired = connection2.AcquireDistributedLock("locks:test", TimeSpan.FromSeconds(10));
        Assert.NotNull(reacquired);
    }

    [Fact]
    public async Task DistributedLock_AfterRenewalsThenDispose_IsImmediatelyReacquirable()
    {
        // Short lease so the background renewal fires several times during the hold; this exercises
        // the dispose path that must wait for an in-flight async renewal to commit before releasing.
        // If a late renewal resurrected the lock (TryAcquireLockOp is an upsert), the re-acquire below
        // would block until the lease expired and time out.
        var port = AllocatePortPairs(1)[0];
        var storage = await StartNode(port, [port], configure: o => o.LockLeaseTimeout = TimeSpan.FromMilliseconds(600));
        using var connection1 = (JobStorageConnection)storage.GetConnection();
        using var connection2 = (JobStorageConnection)storage.GetConnection();

        var handle = connection1.AcquireDistributedLock("locks:lease", TimeSpan.FromSeconds(5));
        await Task.Delay(TimeSpan.FromMilliseconds(700)); // at least three renewal periods elapse
        handle.Dispose();

        // A clean release makes this succeed on the first attempt; a resurrected lock would not free
        // for ~600 ms, so a 400 ms acquisition window discriminates the bug.
        using var reacquired = connection2.AcquireDistributedLock("locks:lease", TimeSpan.FromMilliseconds(400));
        Assert.NotNull(reacquired);
    }

    [Fact]
    public async Task Heartbeat_ThrowsServerGone_ForUnknownServer()
    {
        var storage = await StartSingleNode();
        using var connection = (JobStorageConnection)storage.GetConnection();

        connection.AnnounceServer("srv-1", new Hangfire.Server.ServerContext { WorkerCount = 4, Queues = ["default"] });
        connection.Heartbeat("srv-1"); // known: no throw

        Assert.Throws<BackgroundServerGoneException>(() => connection.Heartbeat("unknown"));
    }

    [Fact]
    public async Task State_SurvivesARestart_ViaWalReplay()
    {
        var ports = AllocatePortPairs(2);
        string jobId;

        var first = await StartNode(ports[0], [ports[0]]);
        using (var connection = (JobStorageConnection)first.GetConnection())
        {
            jobId = connection.CreateExpiredJob(Job.FromExpression(() => TestJobs.Run("durable")), new Dictionary<string, string>(), DateTime.UtcNow, TimeSpan.FromDays(1));
        }

        await first.DisposeAsync();
        _storages.Remove(first);

        // Same WAL directory, fresh ports (the old listener may still be in TIME_WAIT).
        var walPath = Path.Combine(_walRoot, ports[0].ToString());
        var options = new RaftStorageOptions
        {
            SelfEndpoint = $"127.0.0.1:{ports[1]}",
            WalPath = walPath,
            LowerElectionTimeoutMs = 150,
            UpperElectionTimeoutMs = 300,
            SubmitTimeout = TimeSpan.FromSeconds(20),
        };
        options.Members.Add($"127.0.0.1:{ports[1]}");
        var second = await RaftJobStorage.StartAsync(options);
        _storages.Add(second);

        using var restartedConnection = (JobStorageConnection)second.GetConnection();
        var data = restartedConnection.GetJobData(jobId);
        Assert.NotNull(data);
        Assert.Equal("durable", data.Job.Args[0]);
    }

    [Fact]
    public async Task ThreeNodeCluster_WritesFromEveryNode_AreVisibleEverywhere()
    {
        var ports = AllocatePortPairs(3);
        var nodes = await Task.WhenAll(
            Task.Run(() => StartNode(ports[0], ports)),
            Task.Run(() => StartNode(ports[1], ports)),
            Task.Run(() => StartNode(ports[2], ports)));

        // One write through each node; followers forward to the leader transparently.
        var jobIds = new List<string>();
        foreach (var node in nodes)
        {
            using var connection = (JobStorageConnection)node.GetConnection();
            var jobId = connection.CreateExpiredJob(Job.FromExpression(() => TestJobs.Run("clustered")), new Dictionary<string, string>(), DateTime.UtcNow, TimeSpan.FromDays(1));
            using var transaction = connection.CreateWriteTransaction();
            transaction.AddToQueue("default", jobId);
            transaction.Commit();
            jobIds.Add(jobId);
        }

        foreach (var node in nodes)
        {
            using var connection = (JobStorageConnection)node.GetConnection();
            await PollUntil(
                () => jobIds.All(id => connection.GetJobData(id) is not null),
                $"all jobs visible on node {node}");
        }

        // Fetch and ack everything through a single node; the queue must drain cluster-wide.
        using (var connection = (JobStorageConnection)nodes[2].GetConnection())
        {
            for (var i = 0; i < jobIds.Count; i++)
            {
                using var fetched = connection.FetchNextJob(["default"], CancellationToken.None);
                fetched.RemoveFromQueue();
            }
        }

        foreach (var node in nodes)
        {
            using var connection = (JobStorageConnection)node.GetConnection();
            await PollUntil(() => NodeQueueLength(node) == 0, "queue drained on every node");
        }

        static long NodeQueueLength(RaftJobStorage node)
            => ((Hangfire.Storage.JobStorageMonitor)node.GetMonitoringApi()).EnqueuedCount("default");
    }

    [Fact]
    public async Task LeaderFailover_WritesResumeOnASurvivingNode_AndPriorStateIsPreserved()
    {
        var ports = AllocatePortPairs(3);
        var nodes = (await Task.WhenAll(
            Task.Run(() => StartNode(ports[0], ports)),
            Task.Run(() => StartNode(ports[1], ports)),
            Task.Run(() => StartNode(ports[2], ports)))).ToList();

        // Write a job through the cluster before the failover; it must survive the leader change.
        string preFailoverJobId;
        using (var connection = (JobStorageConnection)nodes[0].GetConnection())
        {
            preFailoverJobId = connection.CreateExpiredJob(
                Job.FromExpression(() => TestJobs.Run("before-failover")), new Dictionary<string, string>(), DateTime.UtcNow, TimeSpan.FromDays(1));
        }
        foreach (var node in nodes)
        {
            using var connection = (JobStorageConnection)node.GetConnection();
            await PollUntil(() => connection.GetJobData(preFailoverJobId) is not null, "pre-failover job replicated");
        }

        // Find and kill the current leader.
        RaftJobStorage leader = null!;
        await PollUntil(() => (leader = nodes.FirstOrDefault(n => n.Cluster.IsLeader)!) is not null, "a leader is elected");
        var survivors = nodes.Where(n => n != leader).ToList();
        await leader.DisposeAsync();
        _storages.Remove(leader);

        // The two survivors still form a quorum, so a write must succeed once a new leader is elected.
        // Writes may throw transiently during the election window, so retry until the deadline.
        var deadline = DateTime.UtcNow.AddSeconds(30);
        string postFailoverJobId = null!;
        while (true)
        {
            try
            {
                using var connection = (JobStorageConnection)survivors[0].GetConnection();
                postFailoverJobId = connection.CreateExpiredJob(
                    Job.FromExpression(() => TestJobs.Run("after-failover")), new Dictionary<string, string>(), DateTime.UtcNow, TimeSpan.FromDays(1));
                break;
            }
            catch (RaftStorageException) when (DateTime.UtcNow < deadline)
            {
                await Task.Delay(200);
            }
        }

        // Both the pre-failover and post-failover jobs are visible on every surviving node.
        foreach (var node in survivors)
        {
            using var connection = (JobStorageConnection)node.GetConnection();
            await PollUntil(
                () => connection.GetJobData(preFailoverJobId) is not null && connection.GetJobData(postFailoverJobId) is not null,
                $"both jobs visible on a survivor after failover");
        }
    }

    [Fact]
    public async Task Compaction_SnapshotsTheLog_AndStateSurvivesRestart()
    {
        var ports = AllocatePortPairs(2);
        var walPath = Path.Combine(_walRoot, "compaction");

        // Small partitions force sequential compaction (snapshot building) during these writes.
        var first = await StartNode(ports[0], [ports[0]], walPath, recordsPerPartition: 64);
        string jobId;
        using (var connection = (JobStorageConnection)first.GetConnection())
        {
            jobId = connection.CreateExpiredJob(Job.FromExpression(() => TestJobs.Run("compacted")), new Dictionary<string, string>(), DateTime.UtcNow, TimeSpan.FromDays(1));
            for (var i = 0; i < 200; i++)
            {
                connection.SetRangeInHash($"hash-{i % 10}", [new KeyValuePair<string, string>("i", i.ToString())]);
            }
        }

        await first.DisposeAsync();
        _storages.Remove(first);

        var second = await StartNode(ports[1], [ports[1]], walPath, recordsPerPartition: 64);
        using var restarted = (JobStorageConnection)second.GetConnection();
        Assert.NotNull(restarted.GetJobData(jobId));
        Assert.Equal("199", restarted.GetValueFromHash("hash-9", "i"));
    }

    [Fact]
    public async Task Transaction_SecondCommit_Throws()
    {
        var storage = await StartSingleNode();
        using var connection = (JobStorageConnection)storage.GetConnection();

        using var transaction = connection.CreateWriteTransaction();
        transaction.IncrementCounter("c");
        transaction.Commit();
        Assert.Throws<InvalidOperationException>(transaction.Commit);
    }

    [Fact]
    public async Task Statistics_ReflectOperations()
    {
        var storage = await StartSingleNode();
        using var connection = (JobStorageConnection)storage.GetConnection();

        var jobId = connection.CreateExpiredJob(Job.FromExpression(() => TestJobs.Run("x")), new Dictionary<string, string>(), DateTime.UtcNow, TimeSpan.FromDays(1));
        using (var transaction = connection.CreateWriteTransaction())
        {
            transaction.SetJobState(jobId, new ScheduledState(TimeSpan.FromHours(1)));
            transaction.AddToSet("recurring-jobs", "rj-1");
            transaction.IncrementCounter("stats:succeeded");
            transaction.Commit();
        }

        var stats = ((Hangfire.Storage.JobStorageMonitor)storage.GetMonitoringApi()).GetStatistics();
        Assert.Equal(1, stats.Scheduled);
        Assert.Equal(1, stats.Recurring);
        Assert.Equal(1, stats.Succeeded);
    }
}
