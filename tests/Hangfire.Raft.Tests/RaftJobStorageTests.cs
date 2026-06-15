using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using DotNext.Net.Cluster.Consensus.Raft.Membership;
using Hangfire.Common;
using Hangfire.Raft.Cluster;
using Hangfire.States;
using Hangfire.Storage;
using TUnit.Assertions.Enums;

namespace Hangfire.Raft.Tests;

/// <summary>
/// End-to-end tests against real Raft clusters on loopback: write-ahead log in a temp directory,
/// real elections, real TCP transport and command forwarding.
/// </summary>
public class RaftJobStorageTests
{
    // The write-ahead log fsyncs on every commit (FlushInterval.Zero, for single-node durability). On a
    // slow shared CI disk that contention can push co-located cluster tests past their election/submit
    // deadlines, so CI points this at a tmpfs (HANGFIRE_RAFT_TEST_WAL_ROOT=/dev/shm) where fsync is
    // near-free. Locally it falls back to the temp directory. This changes only where the bytes live, not
    // any test semantics: a dispose+reopen restart still resumes from the same path.
    private readonly string _walRoot = Path.Combine(
        Environment.GetEnvironmentVariable("HANGFIRE_RAFT_TEST_WAL_ROOT") is { Length: > 0 } root ? root : Path.GetTempPath(),
        "hangfire-raft-tests",
        Guid.NewGuid().ToString("n"));
    private readonly List<RaftJobStorage> _storages = [];

    [After(Test)]
    public async Task Cleanup()
    {
        foreach (var storage in _storages)
        {
            try
            {
                await storage.DisposeAsync();
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

    private async Task<RaftJobStorage> StartNode(int selfPort, int[] memberPorts, string? walPath = null, int snapshotInterval = 4096, Action<RaftStorageOptions>? configure = null)
    {
        var options = new RaftStorageOptions
        {
            SelfEndpoint = $"127.0.0.1:{selfPort}",
            WalPath = walPath ?? Path.Combine(_walRoot, selfPort.ToString()),
            SnapshotInterval = snapshotInterval,
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
            if (DateTime.UtcNow > deadline) throw new TimeoutException($"Timed out waiting for: {description}");
            await Task.Delay(100);
        }
    }

    [Test]
    [RetryWithDelay(3, 500)]
    public async Task CreateExpiredJob_RoundtripsThroughGetJobData()
    {
        var storage = await StartSingleNode();
        using var connection = (JobStorageConnection)storage.GetConnection();

        var job = Job.FromExpression(() => TestJobs.Run("hello"));
        var createdAt = DateTime.UtcNow;
        var jobId = connection.CreateExpiredJob(job, new Dictionary<string, string> { ["Culture"] = "de-CH" }, createdAt, TimeSpan.FromDays(1));

        var data = connection.GetJobData(jobId);
        await Assert.That(data).IsNotNull();
        await Assert.That(data.State).IsNull();
        await Assert.That(data.Job.Type).IsEqualTo(typeof(TestJobs));
        await Assert.That(data.Job.Args[0]).IsEqualTo("hello");
        await Assert.That(connection.GetJobParameter(jobId, "Culture")).IsEqualTo("de-CH");
    }

    [Test]
    [RetryWithDelay(3, 500)]
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
        await Assert.That(state).IsNotNull();
        await Assert.That(state.Name).IsEqualTo(EnqueuedState.StateName);
        await Assert.That(state.Data["Queue"]).IsEqualTo("default");

        var monitor = storage.GetMonitoringApi();
        await Assert.That(((Hangfire.Storage.JobStorageMonitor)monitor).EnqueuedCount("default")).IsEqualTo(1);
    }

    [Test]
    [RetryWithDelay(3, 500)]
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
        await Assert.That(fetched.JobId).IsEqualTo(jobId);
        fetched.RemoveFromQueue();

        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        await Assert.That(() => connection.FetchNextJob(["default"], timeout.Token)).Throws<OperationCanceledException>();
    }

    [Test]
    [RetryWithDelay(3, 500)]
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
        await Assert.That(fetched).IsNotNull();
        fetched.RemoveFromQueue();
        await enqueue;
    }

    [Test]
    [RetryWithDelay(3, 500)]
    public async Task DistributedLock_IsMutuallyExclusive_AcrossConnections()
    {
        var storage = await StartSingleNode();
        using var connection1 = (JobStorageConnection)storage.GetConnection();
        using var connection2 = (JobStorageConnection)storage.GetConnection();

        var handle = connection1.AcquireDistributedLock("locks:test", TimeSpan.FromSeconds(5));
        await Assert.That(() => connection2.AcquireDistributedLock("locks:test", TimeSpan.FromSeconds(1))).ThrowsExactly<DistributedLockTimeoutException>();

        handle.Dispose();
        using var reacquired = connection2.AcquireDistributedLock("locks:test", TimeSpan.FromSeconds(10));
        await Assert.That(reacquired).IsNotNull();
    }

    [Test]
    [RetryWithDelay(3, 500)]
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
        await Assert.That(reacquired).IsNotNull();
    }

    [Test]
    [RetryWithDelay(3, 500)]
    public async Task Heartbeat_ThrowsServerGone_ForUnknownServer()
    {
        var storage = await StartSingleNode();
        using var connection = (JobStorageConnection)storage.GetConnection();

        connection.AnnounceServer("srv-1", new Hangfire.Server.ServerContext { WorkerCount = 4, Queues = ["default"] });
        connection.Heartbeat("srv-1"); // known: no throw

        await Assert.That(() => connection.Heartbeat("unknown")).ThrowsExactly<BackgroundServerGoneException>();
    }

    [Test]
    [RetryWithDelay(3, 500)]
    public async Task State_SurvivesARestart_ViaWalReplay()
    {
        var port = AllocatePortPairs(1)[0];
        string jobId;

        var first = await StartNode(port, [port]);
        using (var connection = (JobStorageConnection)first.GetConnection())
        {
            jobId = connection.CreateExpiredJob(Job.FromExpression(() => TestJobs.Run("durable")), new Dictionary<string, string>(), DateTime.UtcNow, TimeSpan.FromDays(1));
        }

        await first.DisposeAsync();
        _storages.Remove(first);

        // Restart on the SAME endpoint and WAL directory, exactly as a real node (e.g. a rescheduled
        // Kubernetes pod that keeps its identity) does: it loads its committed single-member configuration
        // from disk (ColdStart=false), re-elects itself, and replays the WAL tail onto the restored
        // snapshot. Restarting under a different endpoint would leave the node outside its own committed
        // membership, so it could never re-elect and would surface no committed state -- which is not how a
        // production restart behaves, and the very durability path this test exercises depends on it.
        var second = await StartNode(port, [port]);

        using var restartedConnection = (JobStorageConnection)second.GetConnection();
        await PollUntil(() => restartedConnection.GetJobData(jobId) is not null, "the job is replayed after restart");
        var data = restartedConnection.GetJobData(jobId);
        await Assert.That(data).IsNotNull();
        await Assert.That(data.Job.Args[0]).IsEqualTo("durable");
    }

    [Test]
    [RetryWithDelay(3, 500)]
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

    [Test]
    [RetryWithDelay(3, 500)]
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

    [Test]
    [RetryWithDelay(3, 500)]
    public async Task GetHealth_ReportsLeadership_OnASingleNode()
    {
        var storage = await StartSingleNode();
        await PollUntil(() => storage.GetHealth().HasLeader, "the node elects itself leader");

        var health = storage.GetHealth();
        await Assert.That(health.HasLeader).IsTrue();
        await Assert.That(health.IsLeader).IsTrue();
        await Assert.That(health.LeaderEndpoint).IsNotNull();
        await Assert.That(health.Faulted).IsFalse();
        // A cold-started single member establishes leadership at the genesis term without a competitive
        // election, so its term is exactly 0 under DotNext 6.x (multi-node elections advance the term).
        await Assert.That(health.Term).IsEqualTo(0L);
        await Assert.That(health.MemberCount).IsEqualTo(1);
    }

    [Test]
    [RetryWithDelay(3, 500)]
    public async Task StartAsync_DoesNotThrow_WhenAPeerHostnameIsUnresolvable()
    {
        // Members are DnsEndPoints resolved lazily by the transport, so an unresolvable peer is
        // tolerated: the node starts, cannot reach the bogus member, and simply has no quorum.
        var port = AllocatePortPairs(1)[0];
        var options = new RaftStorageOptions
        {
            SelfEndpoint = $"127.0.0.1:{port}",
            WalPath = Path.Combine(_walRoot, $"unresolvable-{port}"),
            LowerElectionTimeoutMs = 150,
            UpperElectionTimeoutMs = 300,
            SubmitTimeout = TimeSpan.FromSeconds(2),
        };
        options.Members.Add($"127.0.0.1:{port}");
        options.Members.Add("does-not-exist.invalid:6000");

        var storage = await RaftJobStorage.StartAsync(options); // must not throw
        _storages.Add(storage);

        await Assert.That(storage.GetHealth().HasLeader).IsFalse(); // 1 of 2 reachable -> no quorum, but no crash
    }

    [Test]
    [RetryWithDelay(3, 500)]
    public async Task Compaction_SnapshotsTheLog_AndStateSurvivesRestart()
    {
        var port = AllocatePortPairs(1)[0];
        var walPath = Path.Combine(_walRoot, "compaction");

        // A small snapshot interval forces snapshots (log compaction) during these writes.
        var first = await StartNode(port, [port], walPath, snapshotInterval: 64);
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

        // Restart on the same endpoint/WAL so the node resumes its committed membership and re-elects; it
        // loads the compacted snapshot and replays any post-snapshot tail on top.
        var second = await StartNode(port, [port], walPath, snapshotInterval: 64);
        using var restarted = (JobStorageConnection)second.GetConnection();
        await PollUntil(() => restarted.GetJobData(jobId) is not null, "the compacted state is restored after restart");
        await Assert.That(restarted.GetJobData(jobId)).IsNotNull();
        await Assert.That(restarted.GetValueFromHash("hash-9", "i")).IsEqualTo("199");
    }

    [Test]
    [RetryWithDelay(3, 500)]
    public async Task Transaction_SecondCommit_Throws()
    {
        var storage = await StartSingleNode();
        using var connection = (JobStorageConnection)storage.GetConnection();

        using var transaction = connection.CreateWriteTransaction();
        transaction.IncrementCounter("c");
        transaction.Commit();
        await Assert.That(transaction.Commit).ThrowsExactly<InvalidOperationException>();
    }

    [Test]
    [RetryWithDelay(3, 500)]
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
        await Assert.That(stats.Scheduled).IsEqualTo(1);
        await Assert.That(stats.Recurring).IsEqualTo(1);
        await Assert.That(stats.Succeeded).IsEqualTo(1);
    }

    [Test]
    [RetryWithDelay(3, 500)]
    public async Task Connection_ValidatesArguments()
    {
        var storage = await StartSingleNode();
        using var c = (JobStorageConnection)storage.GetConnection();

        await Assert.That(() => c.FetchNextJob([], CancellationToken.None)).ThrowsExactly<ArgumentException>();
        await Assert.That(() => c.FetchNextJob(null!, CancellationToken.None)).ThrowsExactly<ArgumentException>();
        await Assert.That(() => c.GetFirstByLowestScoreFromSet("s", 0, 10, 0)).ThrowsExactly<ArgumentException>(); // count <= 0
        await Assert.That(() => c.GetFirstByLowestScoreFromSet("s", 10, 0)).ThrowsExactly<ArgumentException>();     // fromScore > toScore
        await Assert.That(() => c.GetSetCount(["s"], -1)).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(() => c.RemoveTimedOutServers(TimeSpan.FromSeconds(-1))).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(() => c.GetJobData("")).ThrowsExactly<ArgumentException>();
        await Assert.That(() => c.GetAllItemsFromSet("")).ThrowsExactly<ArgumentException>();
        await Assert.That(() => c.GetValueFromHash("", "f")).ThrowsExactly<ArgumentException>();
    }

    [Test]
    [RetryWithDelay(3, 500)]
    public async Task Storage_AdvertisesFeatures_AndDescribesItself()
    {
        var storage = await StartSingleNode();

        await Assert.That(storage.HasFeature(JobStorageFeatures.ExtendedApi)).IsTrue();
        await Assert.That(storage.HasFeature(JobStorageFeatures.Transaction.CreateJob)).IsTrue();
        await Assert.That(storage.HasFeature("some.unknown.feature")).IsFalse();
        await Assert.That(storage.ToString()).Contains("Raft");
    }

    [Test]
    [RetryWithDelay(3, 500)]
    public async Task Connection_ReadsBackEverythingWritten()
    {
        var storage = await StartSingleNode();
        using var c = (JobStorageConnection)storage.GetConnection();

        using (var tx = (JobStorageTransaction)c.CreateWriteTransaction())
        {
            tx.AddToSet("s", "a", 1);
            tx.AddToSet("s", "b", 2);
            tx.ExpireSet("s", TimeSpan.FromMinutes(5));
            tx.InsertToList("l", "x");
            tx.InsertToList("l", "y");
            tx.ExpireList("l", TimeSpan.FromMinutes(5));
            tx.SetRangeInHash("h", [new KeyValuePair<string, string>("f", "v")]);
            tx.ExpireHash("h", TimeSpan.FromMinutes(5));
            tx.IncrementCounter("cnt");
            tx.Commit();
        }

        // sets
        await Assert.That(c.GetAllItemsFromSet("s").OrderBy(x => x)).IsEquivalentTo(["a", "b"], CollectionOrdering.Matching);
        await Assert.That(c.GetSetCount("s")).IsEqualTo(2);
        await Assert.That(c.GetSetContains("s", "a")).IsTrue();
        await Assert.That(c.GetSetContains("s", "z")).IsFalse();
        await Assert.That(c.GetRangeFromSet("s", 0, 10)).Contains("a");
        await Assert.That(c.GetFirstByLowestScoreFromSet("s", 0, 5)).IsEqualTo("a");
        await Assert.That(c.GetSetTtl("s") > TimeSpan.Zero).IsTrue();
        // lists (newest-first)
        await Assert.That(c.GetAllItemsFromList("l")).IsEquivalentTo(["y", "x"], CollectionOrdering.Matching);
        await Assert.That(c.GetListCount("l")).IsEqualTo(2);
        await Assert.That(c.GetRangeFromList("l", 0, 0)).IsEquivalentTo(["y"], CollectionOrdering.Matching);
        await Assert.That(c.GetListTtl("l") > TimeSpan.Zero).IsTrue();
        // hash
        await Assert.That(c.GetValueFromHash("h", "f")).IsEqualTo("v");
        await Assert.That(c.GetHashCount("h")).IsEqualTo(1);
        await Assert.That(c.GetHashTtl("h") > TimeSpan.Zero).IsTrue();
        await Assert.That(c.GetAllEntriesFromHash("h")!["f"]).IsEqualTo("v");
        // counter
        await Assert.That(c.GetCounter("cnt")).IsEqualTo(1);

        // job parameter round-trip
        var jobId = c.CreateExpiredJob(Job.FromExpression(() => TestJobs.Run("x")), new Dictionary<string, string>(), DateTime.UtcNow, TimeSpan.FromDays(1));
        c.SetJobParameter(jobId, "p", "1");
        await Assert.That(c.GetJobParameter(jobId, "p")).IsEqualTo("1");
    }

    [Test]
    [RetryWithDelay(3, 500)]
    public async Task FetchedJob_DisposeWithoutAck_RequeuesTheJob()
    {
        var storage = await StartSingleNode();
        using var connection = (JobStorageConnection)storage.GetConnection();

        var jobId = connection.CreateExpiredJob(Job.FromExpression(() => TestJobs.Run("x")), new Dictionary<string, string>(), DateTime.UtcNow, TimeSpan.FromDays(1));
        using (var transaction = connection.CreateWriteTransaction())
        {
            transaction.AddToQueue("q", jobId);
            transaction.Commit();
        }

        var fetched = connection.FetchNextJob(["q"], CancellationToken.None);
        await Assert.That(fetched.JobId).IsEqualTo(jobId);
        fetched.Dispose(); // neither acked nor explicitly requeued -> Dispose requeues

        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        using var refetched = connection.FetchNextJob(["q"], timeout.Token);
        await Assert.That(refetched.JobId).IsEqualTo(jobId);
        refetched.RemoveFromQueue();
    }

    [Test]
    [RetryWithDelay(3, 500)]
    public async Task FetchedJob_RenewalKeepsTheLeaseAlive_PastTheInvisibilityTimeout()
    {
        var port = AllocatePortPairs(1)[0];
        var storage = await StartNode(port, [port], configure: o =>
        {
            o.FetchInvisibilityTimeout = TimeSpan.FromMilliseconds(600); // renew every ~200ms
            o.MaintenanceInterval = TimeSpan.FromMilliseconds(300);      // frequent reclaim attempts
        });
        using var connection = (JobStorageConnection)storage.GetConnection();

        var jobId = connection.CreateExpiredJob(Job.FromExpression(() => TestJobs.Run("x")), new Dictionary<string, string>(), DateTime.UtcNow, TimeSpan.FromDays(1));
        using (var transaction = connection.CreateWriteTransaction())
        {
            transaction.AddToQueue("q", jobId);
            transaction.Commit();
        }

        using var fetched = connection.FetchNextJob(["q"], CancellationToken.None);
        var monitor = (Hangfire.Storage.JobStorageMonitor)storage.GetMonitoringApi();

        // Hold well past the invisibility timeout: the background renewal must keep maintenance from
        // reclaiming the lease, so the job stays fetched and is not put back on the queue.
        await Task.Delay(TimeSpan.FromSeconds(2));
        await Assert.That(monitor.FetchedCount("q")).IsEqualTo(1);
        await Assert.That(monitor.EnqueuedCount("q")).IsEqualTo(0);
        fetched.RemoveFromQueue();
    }

    [Test]
    [RetryWithDelay(3, 500)]
    public async Task GetHealth_OnAFollower_ReportsTheRemoteLeader()
    {
        var ports = AllocatePortPairs(3);
        var nodes = (await Task.WhenAll(ports.Select(p => StartNode(p, ports)))).ToList();

        RaftJobStorage follower = null!;
        await PollUntil(
            () => (follower = nodes.FirstOrDefault(n => n.GetHealth() is { HasLeader: true, IsLeader: false })!) is not null,
            "a follower that sees a remote leader");

        var health = follower.GetHealth();
        await Assert.That(health.HasLeader).IsTrue();
        await Assert.That(health.IsLeader).IsFalse();
        await Assert.That(health.LeaderEndpoint).IsNotNull();
        await Assert.That(health.Faulted).IsFalse();
        // All three members are seeded into the committed configuration at genesis, so every node knows
        // the full set regardless of which is currently reachable.
        await Assert.That(health.MemberCount).IsEqualTo(3);
    }

    [Test]
    public async Task PersistentConfig_RoundTripsIpAndDnsEndpoints()
    {
        // The endpoint (de)serialization is custom (it reuses DotNext's EndPointFormatter), so verify both
        // endpoint shapes survive a save and a reopen from disk: an IP literal must come back as an
        // IPEndPoint and a host name as a DnsEndPoint. This is the round-trip a restarted node relies on to
        // resume its committed membership.
        var dir = Path.Combine(_walRoot, "config-roundtrip");
        Directory.CreateDirectory(dir);
        var file = Path.Combine(dir, "config");

        var ip = new IPEndPoint(IPAddress.Loopback, 5000);
        var dns = new DnsEndPoint("node1.local", 6000);

        using (IClusterConfigurationStorage<EndPoint> storage = new EndPointPersistentConfigurationStorage(file))
        {
            var config = await storage.LoadConfigurationAsync();
            config = config.Add(ip).Add(dns);
            await storage.SaveConfigurationAsync(config, configurationVersion: 0L);
        }

        using (IClusterConfigurationStorage<EndPoint> reopened = new EndPointPersistentConfigurationStorage(file))
        {
            var config = await reopened.LoadConfigurationAsync();
            await Assert.That(config.Members.Count).IsEqualTo(2);
            await Assert.That(config.Members).Contains(ip);
            await Assert.That(config.Members).Contains(dns);
            // The DNS member must round-trip as a DnsEndPoint, not collapse to a resolved IP address.
            await Assert.That(config.Members).Contains(m => m is DnsEndPoint { Host: "node1.local", Port: 6000 });
        }
    }

    [Test]
    [RetryWithDelay(3, 500)]
    public async Task AcknowledgedWrite_SurvivesACrash_WithoutAnyBackgroundFlush()
    {
        // With FlushInterval.Infinite the background flusher is off, so the ONLY thing that persists a
        // write is the flush SubmitAsync performs before acknowledging it. Disposing the node performs no
        // flush (and neither would a real crash), so finding every acknowledged job after a restart proves
        // durability comes solely from that flush-before-ack: drop it and the checkpoint never advances, so
        // the restart replays nothing and the jobs are gone.
        var port = AllocatePortPairs(1)[0];
        var walPath = Path.Combine(_walRoot, "crash-durability");

        var jobIds = new List<string>();
        var first = await StartNode(port, [port], walPath, configure: o => o.FlushInterval = Timeout.InfiniteTimeSpan);
        using (var connection = (JobStorageConnection)first.GetConnection())
        {
            // Several writes, so the assertion is about the whole acknowledged tail, not a lucky single entry.
            for (var i = 0; i < 5; i++)
                jobIds.Add(connection.CreateExpiredJob(Job.FromExpression(() => TestJobs.Run("durable")), new Dictionary<string, string>(), DateTime.UtcNow, TimeSpan.FromDays(1)));
        }

        // Terminate without a graceful flush, mimicking a process crash.
        await first.DisposeAsync();
        _storages.Remove(first);

        var second = await StartNode(port, [port], walPath, configure: o => o.FlushInterval = Timeout.InfiniteTimeSpan);
        using var restarted = (JobStorageConnection)second.GetConnection();
        // Infinite-flush makes the restart's tail replay synchronous (slower), and rebinding the same port
        // can hit a brief re-election delay, so allow a wider budget than the default before the restarted
        // node is caught up.
        await PollUntil(() => jobIds.All(id => restarted.GetJobData(id) is not null), "every acknowledged write is durable after a crash", timeoutSeconds: 30);
        foreach (var id in jobIds)
            await Assert.That(restarted.GetJobData(id)).IsNotNull();
    }

    [Test]
    [RetryWithDelay(3, 500)]
    public async Task Minority_CannotCommitWrites_AfterQuorumIsLost()
    {
        var ports = AllocatePortPairs(3);
        var nodes = (await Task.WhenAll(ports.Select(p => StartNode(p, ports, configure: o => o.SubmitTimeout = TimeSpan.FromSeconds(3))))).ToList();

        // Baseline: a write commits while the full cluster has quorum, and replicates everywhere.
        string baselineId;
        using (var connection = (JobStorageConnection)nodes[0].GetConnection())
            baselineId = connection.CreateExpiredJob(Job.FromExpression(() => TestJobs.Run("quorum")), new Dictionary<string, string>(), DateTime.UtcNow, TimeSpan.FromDays(1));
        foreach (var node in nodes)
        {
            using var c = (JobStorageConnection)node.GetConnection();
            await PollUntil(() => c.GetJobData(baselineId) is not null, "baseline write replicated");
        }

        // Drop two of three members. The lone survivor has no majority and must refuse to commit a write
        // (a minority committing would be a split brain): the attempt fails instead of silently succeeding.
        await nodes[1].DisposeAsync();
        _storages.Remove(nodes[1]);
        await nodes[2].DisposeAsync();
        _storages.Remove(nodes[2]);

        using var survivor = (JobStorageConnection)nodes[0].GetConnection();
        await Assert.That(() => survivor.CreateExpiredJob(Job.FromExpression(() => TestJobs.Run("no-quorum")), new Dictionary<string, string>(), DateTime.UtcNow, TimeSpan.FromDays(1)))
            .Throws<RaftStorageException>();

        // Reads do not need quorum, so the prior committed state is still visible on the survivor.
        await Assert.That(survivor.GetJobData(baselineId)).IsNotNull();
    }

    [Test]
    [RetryWithDelay(3, 500)]
    public async Task ConcurrentFetches_AcrossNodes_DeliverEachJobExactlyOnce()
    {
        var ports = AllocatePortPairs(3);
        var nodes = (await Task.WhenAll(ports.Select(p => StartNode(p, ports)))).ToList();

        // Enqueue a batch through one node and wait until every job is visible on every node. All jobs go
        // in a single transaction (one consensus round) so the test exercises concurrent fetch contention,
        // not a long sequence of co-located fsyncing writes.
        const int jobCount = 20;
        var enqueued = new List<string>();
        using (var connection = (JobStorageConnection)nodes[0].GetConnection())
        {
            using var tx = (JobStorageTransaction)connection.CreateWriteTransaction();
            for (var i = 0; i < jobCount; i++)
            {
                var jobId = tx.CreateJob(Job.FromExpression(() => TestJobs.Run("contended")), new Dictionary<string, string>(), DateTime.UtcNow, TimeSpan.FromDays(1));
                tx.AddToQueue("default", jobId);
                enqueued.Add(jobId);
            }
            tx.Commit();
        }
        foreach (var node in nodes)
        {
            using var c = (JobStorageConnection)node.GetConnection();
            await PollUntil(() => enqueued.All(id => c.GetJobData(id) is not null), "all jobs replicated");
        }

        // Race four fetchers on each of the three nodes. FetchOp is a consensus operation, so every job
        // must go to exactly one fetcher across the whole cluster: no duplicates, no losses.
        var fetched = new ConcurrentBag<string>();
        using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var workers = nodes.SelectMany(node => Enumerable.Range(0, 4).Select(_ => Task.Run(async () =>
        {
            using var connection = (JobStorageConnection)node.GetConnection();
            while (fetched.Count < jobCount && !stop.IsCancellationRequested)
            {
                try
                {
                    using var timeout = CancellationTokenSource.CreateLinkedTokenSource(stop.Token);
                    timeout.CancelAfter(TimeSpan.FromSeconds(2));
                    using var job = connection.FetchNextJob(["default"], timeout.Token);
                    fetched.Add(job.JobId);
                    job.RemoveFromQueue();
                }
                catch (OperationCanceledException)
                {
                    // The queue is momentarily empty (every job already taken); re-check the loop condition.
                }
            }
        }))).ToList();

        await Task.WhenAll(workers);

        // Exactly the enqueued set, each delivered once: no duplicate execution, no lost job.
        await Assert.That(fetched.Count).IsEqualTo(jobCount);
        await Assert.That(fetched.OrderBy(x => x)).IsEquivalentTo(enqueued.OrderBy(x => x), CollectionOrdering.Matching);
    }

    [Test]
    [RetryWithDelay(3, 500)]
    public async Task GetHealth_ExposesAppliedAndCommitIndex()
    {
        var storage = await StartSingleNode();
        await PollUntil(() => storage.GetHealth().HasLeader, "the node elects itself leader");

        using var connection = (JobStorageConnection)storage.GetConnection();
        connection.CreateExpiredJob(Job.FromExpression(() => TestJobs.Run("x")), new Dictionary<string, string>(), DateTime.UtcNow, TimeSpan.FromDays(1));

        var health = storage.GetHealth();
        // A committed, applied and flushed write advances both indices past genesis, and applied can never
        // run ahead of committed: CommitIndex - AppliedIndex is the local apply lag a probe can bound.
        await Assert.That(health.CommitIndex > 0L).IsTrue();
        await Assert.That(health.AppliedIndex > 0L).IsTrue();
        await Assert.That(health.AppliedIndex <= health.CommitIndex).IsTrue();
    }
}
