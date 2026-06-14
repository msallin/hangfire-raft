using System.Text;
using Hangfire.Raft.Commands;
using Hangfire.Raft.State;

namespace Hangfire.Raft.Tests;

/// <summary>
/// Unit tests of the deterministic store. Time always comes from the command envelope, so every
/// scenario controls the clock explicitly.
/// </summary>
public class RaftStoreTests
{
    private static readonly DateTime T0 = new(2026, 6, 11, 12, 0, 0, DateTimeKind.Utc);

    private readonly RaftStore _store = new();

    private object? Apply(DateTime now, params StoreOp[] ops)
        => _store.Apply(new Command { Id = Guid.NewGuid(), NowUtc = now, Ops = ops }).Result;

    private ApplyEffects ApplyWithEffects(DateTime now, params StoreOp[] ops)
        => _store.Apply(new Command { Id = Guid.NewGuid(), NowUtc = now, Ops = ops });

    private static CreateJobOp NewJob(string id) => new(id, $"payload-{id}", [new("Culture", "de-CH")], T0, T0.AddDays(1));

    private static StateRecord State(string name, DateTime createdAt, string? reason = null)
        => new(name, reason, [new("Key", "Value")], createdAt);

    // ----- jobs -----

    [Fact]
    public void CreateJob_StoresInvocationAndParameters()
    {
        Apply(T0, NewJob("a"));

        var job = _store.GetJob("a");
        Assert.NotNull(job);
        Assert.Equal("payload-a", job.InvocationData);
        Assert.Equal(T0, job.CreatedAt);
        Assert.Equal(T0.AddDays(1), job.ExpireAt);
        Assert.Equal("de-CH", job.Parameters["Culture"]);
        Assert.Null(job.CurrentState);
        Assert.Empty(job.History);
    }

    [Fact]
    public void GetJob_ReturnsNull_WhenMissing() => Assert.Null(_store.GetJob("missing"));

    [Fact]
    public void SetJobParameter_AddsOverwritesAndAllowsNull()
    {
        Apply(T0, NewJob("a"));
        Apply(T0, new SetJobParameterOp("a", "X", "1"));
        Apply(T0, new SetJobParameterOp("a", "X", "2"));
        Apply(T0, new SetJobParameterOp("a", "N", null));

        Assert.Equal("2", _store.GetJobParameter("a", "X"));
        Assert.Null(_store.GetJobParameter("a", "N"));
        Assert.Null(_store.GetJobParameter("a", "missing"));
        Assert.Null(_store.GetJobParameter("missing", "X"));
    }

    [Fact]
    public void SetJobParameter_IsNoOp_ForMissingJob()
    {
        Apply(T0, new SetJobParameterOp("missing", "X", "1"));
        Assert.Null(_store.GetJob("missing"));
    }

    [Fact]
    public void SetJobState_UpdatesCurrentStateHistoryAndIndex()
    {
        Apply(T0, NewJob("a"));
        Apply(T0, new SetJobStateOp("a", State("Enqueued", T0)));
        Apply(T0, new SetJobStateOp("a", State("Processing", T0.AddSeconds(1))));

        var job = _store.GetJob("a")!;
        Assert.Equal("Processing", job.CurrentState!.Name);
        Assert.Equal(2, job.History.Count);
        Assert.Equal(0, _store.GetStateCount("Enqueued"));
        Assert.Equal(1, _store.GetStateCount("Processing"));
    }

    [Fact]
    public void AddJobState_AppendsHistoryWithoutChangingCurrentState()
    {
        Apply(T0, NewJob("a"));
        Apply(T0, new SetJobStateOp("a", State("Enqueued", T0)));
        Apply(T0, new AddJobStateOp("a", State("Custom", T0.AddSeconds(1))));

        var job = _store.GetJob("a")!;
        Assert.Equal("Enqueued", job.CurrentState!.Name);
        Assert.Equal(2, job.History.Count);
        Assert.Equal(0, _store.GetStateCount("Custom"));
    }

    [Fact]
    public void GetJobsByState_OrdersAndPages()
    {
        for (var i = 0; i < 5; i++)
        {
            var id = $"job-{i}";
            Apply(T0, NewJob(id));
            Apply(T0, new SetJobStateOp(id, State("Succeeded", T0.AddSeconds(i))));
        }

        var ascending = _store.GetJobsByState("Succeeded", 0, 10, ascending: true).Select(j => j.Id).ToList();
        Assert.Equal(["job-0", "job-1", "job-2", "job-3", "job-4"], ascending);

        var newestFirst = _store.GetJobsByState("Succeeded", 0, 2, ascending: false).Select(j => j.Id).ToList();
        Assert.Equal(["job-4", "job-3"], newestFirst);

        var page = _store.GetJobsByState("Succeeded", 2, 2, ascending: true).Select(j => j.Id).ToList();
        Assert.Equal(["job-2", "job-3"], page);

        Assert.Empty(_store.GetJobsByState("Succeeded", 10, 5, ascending: true));
        Assert.Empty(_store.GetJobsByState("Unknown", 0, 5, ascending: true));
    }

    [Fact]
    public void GetJobsByState_BreaksTimestampTiesById()
    {
        foreach (var id in new[] { "b", "a", "c" })
        {
            Apply(T0, NewJob(id));
            Apply(T0, new SetJobStateOp(id, State("Scheduled", T0)));
        }

        var ids = _store.GetJobsByState("Scheduled", 0, 10, ascending: true).Select(j => j.Id).ToList();
        Assert.Equal(["a", "b", "c"], ids);
    }

    [Fact]
    public void PersistJob_ClearsExpiry_AndExpireJobSetsIt()
    {
        Apply(T0, NewJob("a"));
        Apply(T0, new PersistJobOp("a"));
        Assert.Null(_store.GetJob("a")!.ExpireAt);

        Apply(T0, new ExpireJobOp("a", T0.AddMinutes(30)));
        Assert.Equal(T0.AddMinutes(30), _store.GetJob("a")!.ExpireAt);
    }

    [Fact]
    public void Maintenance_EvictsExpiredJobs_AndTheirIndexEntries()
    {
        Apply(T0, NewJob("a"));
        Apply(T0, new SetJobStateOp("a", State("Succeeded", T0)));
        Apply(T0, new ExpireJobOp("a", T0.AddMinutes(1)));

        Apply(T0.AddSeconds(59), new MaintenanceOp(TimeSpan.FromMinutes(5)));
        Assert.NotNull(_store.GetJob("a"));

        Apply(T0.AddMinutes(1), new MaintenanceOp(TimeSpan.FromMinutes(5)));
        Assert.Null(_store.GetJob("a"));
        Assert.Equal(0, _store.GetStateCount("Succeeded"));
    }

    // ----- queues and fetching -----

    [Fact]
    public void Fetch_ReturnsJobsInFifoOrder()
    {
        Apply(T0, NewJob("a"), NewJob("b"), new EnqueueOp("default", "a"), new EnqueueOp("default", "b"));

        var first = (FetchResult?)Apply(T0, new FetchOp(["default"], Guid.NewGuid()));
        var second = (FetchResult?)Apply(T0, new FetchOp(["default"], Guid.NewGuid()));
        var third = (FetchResult?)Apply(T0, new FetchOp(["default"], Guid.NewGuid()));

        Assert.Equal("a", first!.Value.JobId);
        Assert.Equal("b", second!.Value.JobId);
        Assert.Null(third);
    }

    [Fact]
    public void Fetch_RespectsQueuePriorityOrder()
    {
        Apply(T0, NewJob("low"), NewJob("crit"),
            new EnqueueOp("default", "low"), new EnqueueOp("critical", "crit"));

        var fetched = (FetchResult?)Apply(T0, new FetchOp(["critical", "default"], Guid.NewGuid()));

        Assert.Equal("crit", fetched!.Value.JobId);
        Assert.Equal("critical", fetched.Value.Queue);
    }

    [Fact]
    public void Fetch_SkipsJobsThatNoLongerExist()
    {
        Apply(T0, NewJob("alive"), new EnqueueOp("default", "ghost"), new EnqueueOp("default", "alive"));

        var fetched = (FetchResult?)Apply(T0, new FetchOp(["default"], Guid.NewGuid()));

        Assert.Equal("alive", fetched!.Value.JobId);
    }

    [Fact]
    public void RequeueFetched_PutsJobBackAtTheHead()
    {
        Apply(T0, NewJob("a"), NewJob("b"), new EnqueueOp("q", "a"), new EnqueueOp("q", "b"));
        var token = Guid.NewGuid();
        Apply(T0, new FetchOp(["q"], token));

        var effects = ApplyWithEffects(T0, new RequeueFetchedOp(token));

        Assert.Contains("q", effects.SignaledQueues!);
        var next = (FetchResult?)Apply(T0, new FetchOp(["q"], Guid.NewGuid()));
        Assert.Equal("a", next!.Value.JobId);
    }

    [Fact]
    public void AckFetched_RemovesTheLease()
    {
        Apply(T0, NewJob("a"), new EnqueueOp("q", "a"));
        var token = Guid.NewGuid();
        Apply(T0, new FetchOp(["q"], token));

        Apply(T0, new AckFetchedOp(token));

        Assert.Equal(0, _store.GetFetchedCount("q"));
        Assert.False((bool)Apply(T0, new RenewFetchedOp(token))!);
    }

    [Fact]
    public void Maintenance_ReclaimsStaleFetches_ButNotRenewedOnes()
    {
        Apply(T0, NewJob("stale"), NewJob("active"), new EnqueueOp("q", "stale"), new EnqueueOp("q", "active"));
        var staleToken = Guid.NewGuid();
        var activeToken = Guid.NewGuid();
        Apply(T0, new FetchOp(["q"], staleToken));
        Apply(T0, new FetchOp(["q"], activeToken));

        Assert.True((bool)Apply(T0.AddMinutes(4), new RenewFetchedOp(activeToken))!);

        var effects = ApplyWithEffects(T0.AddMinutes(5), new MaintenanceOp(TimeSpan.FromMinutes(5)));

        Assert.Contains("q", effects.SignaledQueues!);
        Assert.Equal(1, _store.GetFetchedCount("q")); // the renewed lease survives
        var reclaimed = (FetchResult?)Apply(T0.AddMinutes(5), new FetchOp(["q"], Guid.NewGuid()));
        Assert.Equal("stale", reclaimed!.Value.JobId);
    }

    [Fact]
    public void Enqueue_SignalsTheQueue()
    {
        Apply(T0, NewJob("a"));
        var effects = ApplyWithEffects(T0, new EnqueueOp("q", "a"));
        Assert.Contains("q", effects.SignaledQueues!);
    }

    // ----- counters -----

    [Fact]
    public void Counters_AccumulateAndVanishAtZeroWithoutExpiry()
    {
        Apply(T0, new IncrementCounterOp("c", 1, null), new IncrementCounterOp("c", 1, null));
        Assert.Equal(2, _store.GetCounter("c"));

        Apply(T0, new IncrementCounterOp("c", -2, null));
        Assert.Equal(0, _store.GetCounter("c"));
    }

    [Fact]
    public void Counters_ExpiryOnlyExtends()
    {
        Apply(T0, new IncrementCounterOp("c", 1, T0.AddHours(2)));
        Apply(T0, new IncrementCounterOp("c", 1, T0.AddHours(1)));

        Apply(T0.AddMinutes(90), new MaintenanceOp(TimeSpan.FromMinutes(5)));
        Assert.Equal(2, _store.GetCounter("c")); // still alive: the later, shorter expiry did not shrink the TTL

        Apply(T0.AddHours(2), new MaintenanceOp(TimeSpan.FromMinutes(5)));
        Assert.Equal(0, _store.GetCounter("c"));
    }

    // ----- sets -----

    [Fact]
    public void Sets_UpsertScore_AndOrderByScoreThenValue()
    {
        Apply(T0,
            new AddToSetOp("s", "b", 2),
            new AddToSetOp("s", "a", 2),
            new AddToSetOp("s", "c", 1),
            new AddToSetOp("s", "b", 0.5)); // moves b to the front

        Assert.Equal(["b", "c", "a"], _store.GetRangeFromSet("s", 0, 10));
        Assert.Equal(3, _store.GetSetCount("s"));
    }

    [Fact]
    public void GetFirstByLowestScoreFromSet_RespectsInclusiveBoundsAndCount()
    {
        Apply(T0, new AddToSetOp("s", "a", 1), new AddToSetOp("s", "b", 2), new AddToSetOp("s", "c", 3));

        Assert.Equal(["a", "b"], _store.GetFirstByLowestScoreFromSet("s", 1, 2, 10));
        Assert.Equal(["a"], _store.GetFirstByLowestScoreFromSet("s", 1, 3, 1));
        Assert.Empty(_store.GetFirstByLowestScoreFromSet("s", 4, 9, 10));
        Assert.Empty(_store.GetFirstByLowestScoreFromSet("missing", 0, 10, 10));
    }

    [Fact]
    public void Sets_RemoveValue_DropsEmptySet()
    {
        Apply(T0, new AddToSetOp("s", "a", 1));
        Apply(T0, new RemoveFromSetOp("s", "a"));

        Assert.Equal(0, _store.GetSetCount("s"));
        Assert.False(_store.GetSetContains("s", "a"));
    }

    [Fact]
    public void Sets_AddRange_UsesScoreZero()
    {
        Apply(T0, new AddRangeToSetOp("s", ["x", "y"]));
        Assert.Equal(["x", "y"], _store.GetFirstByLowestScoreFromSet("s", 0, 0, 10));
    }

    [Fact]
    public void GetSetCount_WithLimit_Caps()
    {
        Apply(T0, new AddRangeToSetOp("s1", ["a", "b"]), new AddRangeToSetOp("s2", ["c", "d"]));
        Assert.Equal(3, _store.GetSetCount(["s1", "s2"], 3));
        Assert.Equal(4, _store.GetSetCount(["s1", "s2"], 100));
    }

    [Fact]
    public void Sets_ExpireAndPersist_ControlEviction()
    {
        Apply(T0, new AddToSetOp("s", "a", 1), new ExpireSetOp("s", T0.AddMinutes(1)));
        Assert.True(_store.GetSetTtl("s", T0) > TimeSpan.Zero);

        Apply(T0, new PersistSetOp("s"));
        Assert.True(_store.GetSetTtl("s", T0) < TimeSpan.Zero);

        Apply(T0, new ExpireSetOp("s", T0.AddMinutes(1)));
        Apply(T0.AddMinutes(2), new MaintenanceOp(TimeSpan.FromMinutes(5)));
        Assert.Equal(0, _store.GetSetCount("s"));
    }

    // ----- lists -----

    [Fact]
    public void Lists_AreNewestFirst()
    {
        Apply(T0, new InsertToListOp("l", "first"), new InsertToListOp("l", "second"));

        Assert.Equal(["second", "first"], _store.GetAllItemsFromList("l"));
        Assert.Equal(["second"], _store.GetRangeFromList("l", 0, 0));
        Assert.Equal(2, _store.GetListCount("l"));
    }

    [Fact]
    public void Lists_RemoveDeletesAllOccurrences_AndDropsEmptyList()
    {
        Apply(T0, new InsertToListOp("l", "x"), new InsertToListOp("l", "y"), new InsertToListOp("l", "x"));
        Apply(T0, new RemoveFromListOp("l", "x"));
        Assert.Equal(["y"], _store.GetAllItemsFromList("l"));

        Apply(T0, new RemoveFromListOp("l", "y"));
        Assert.Equal(0, _store.GetListCount("l"));
    }

    [Fact]
    public void Lists_ExpireAndPersist_ControlEviction()
    {
        Apply(T0, new InsertToListOp("l", "x"), new ExpireListOp("l", T0.AddMinutes(1)));
        Assert.True(_store.GetListTtl("l", T0) > TimeSpan.Zero);

        Apply(T0, new PersistListOp("l"));
        Assert.True(_store.GetListTtl("l", T0) < TimeSpan.Zero);
        Apply(T0.AddMinutes(2), new MaintenanceOp(TimeSpan.FromMinutes(5)));
        Assert.Equal(1, _store.GetListCount("l")); // persisted: survives

        Apply(T0, new ExpireListOp("l", T0.AddMinutes(1)));
        Apply(T0.AddMinutes(1), new MaintenanceOp(TimeSpan.FromMinutes(5)));
        Assert.Equal(0, _store.GetListCount("l"));
    }

    [Fact]
    public void TrimList_KeepsInclusiveRangeOfNewestFirstView()
    {
        Apply(T0,
            new InsertToListOp("l", "1"), new InsertToListOp("l", "2"),
            new InsertToListOp("l", "3"), new InsertToListOp("l", "4")); // list: 4,3,2,1

        Apply(T0, new TrimListOp("l", 1, 2));
        Assert.Equal(["3", "2"], _store.GetAllItemsFromList("l"));

        Apply(T0, new TrimListOp("l", 5, 9));
        Assert.Equal(0, _store.GetListCount("l"));
    }

    // ----- hashes -----

    [Fact]
    public void Hashes_MergeFields_AndSupportNullValues()
    {
        Apply(T0, new SetRangeInHashOp("h", [new("a", "1"), new("b", null)]));
        Apply(T0, new SetRangeInHashOp("h", [new("a", "2"), new("c", "3")]));

        var fields = _store.GetAllEntriesFromHash("h")!;
        Assert.Equal("2", fields["a"]);
        Assert.Null(fields["b"]);
        Assert.Equal("3", fields["c"]);
        Assert.Equal(3, _store.GetHashCount("h"));
        Assert.Equal("3", _store.GetValueFromHash("h", "c"));
        Assert.Null(_store.GetAllEntriesFromHash("missing"));
    }

    [Fact]
    public void Hashes_RemoveAndExpire()
    {
        Apply(T0, new SetRangeInHashOp("h", [new("a", "1")]), new ExpireHashOp("h", T0.AddMinutes(1)));
        Apply(T0.AddMinutes(1), new MaintenanceOp(TimeSpan.FromMinutes(5)));
        Assert.Equal(0, _store.GetHashCount("h"));

        Apply(T0, new SetRangeInHashOp("h2", [new("a", "1")]));
        Apply(T0, new RemoveHashOp("h2"));
        Assert.Null(_store.GetAllEntriesFromHash("h2"));
    }

    // ----- servers -----

    [Fact]
    public void Servers_AnnounceHeartbeatAndTimeout()
    {
        Apply(T0, new AnnounceServerOp("srv-1", 8, ["default"]));
        Assert.True((bool)Apply(T0.AddMinutes(1), new HeartbeatOp("srv-1"))!);
        Assert.False((bool)Apply(T0, new HeartbeatOp("unknown"))!);

        // heartbeat at T0+1min; timeout 5min; at T0+6min the server is exactly at the cutoff and survives
        Assert.Equal(0, (int)Apply(T0.AddMinutes(6), new RemoveTimedOutServersOp(TimeSpan.FromMinutes(5)))!);
        Assert.Equal(1, (int)Apply(T0.AddMinutes(7), new RemoveTimedOutServersOp(TimeSpan.FromMinutes(5)))!);
        Assert.Empty(_store.GetServers());
    }

    [Fact]
    public void Servers_ReannounceReplaces()
    {
        Apply(T0, new AnnounceServerOp("srv-1", 8, ["default"]));
        Apply(T0.AddMinutes(1), new AnnounceServerOp("srv-1", 16, ["critical"]));

        var server = Assert.Single(_store.GetServers());
        Assert.Equal(16, server.WorkerCount);
        Assert.Equal(["critical"], server.Queues);
        Assert.Equal(T0.AddMinutes(1), server.StartedAt);
    }

    // ----- locks -----

    [Fact]
    public void Locks_MutualExclusion_SameOwnerRenews_ExpiryFrees()
    {
        var owner1 = Guid.NewGuid();
        var owner2 = Guid.NewGuid();
        var lease = TimeSpan.FromMinutes(2);

        Assert.True((bool)Apply(T0, new TryAcquireLockOp("r", owner1, lease))!);
        Assert.False((bool)Apply(T0, new TryAcquireLockOp("r", owner2, lease))!);
        Assert.True((bool)Apply(T0.AddMinutes(1), new TryAcquireLockOp("r", owner1, lease))!); // renewal extends to T0+3min

        Assert.False((bool)Apply(T0.AddMinutes(2.5), new TryAcquireLockOp("r", owner2, lease))!);
        Assert.True((bool)Apply(T0.AddMinutes(3), new TryAcquireLockOp("r", owner2, lease))!); // lease expired
    }

    [Fact]
    public void Locks_ReleaseByOwnerOnly_AndSignals()
    {
        var owner = Guid.NewGuid();
        var other = Guid.NewGuid();
        Apply(T0, new TryAcquireLockOp("r", owner, TimeSpan.FromMinutes(2)));

        var foreignRelease = ApplyWithEffects(T0, new ReleaseLockOp("r", other));
        Assert.False(foreignRelease.LocksReleased);
        Assert.False((bool)Apply(T0, new TryAcquireLockOp("r", other, TimeSpan.FromMinutes(2)))!);

        var ownerRelease = ApplyWithEffects(T0, new ReleaseLockOp("r", owner));
        Assert.True(ownerRelease.LocksReleased);
        Assert.True((bool)Apply(T0, new TryAcquireLockOp("r", other, TimeSpan.FromMinutes(2)))!);
    }

    [Fact]
    public void Maintenance_DropsExpiredLocks()
    {
        Apply(T0, new TryAcquireLockOp("r", Guid.NewGuid(), TimeSpan.FromMinutes(2)));
        var effects = ApplyWithEffects(T0.AddMinutes(2), new MaintenanceOp(TimeSpan.FromMinutes(5)));

        Assert.True(effects.LocksReleased);
        Assert.True((bool)Apply(T0.AddMinutes(2), new TryAcquireLockOp("r", Guid.NewGuid(), TimeSpan.FromMinutes(2)))!);
    }

    // ----- statistics -----

    [Fact]
    public void Statistics_AggregateAcrossTables()
    {
        Apply(T0,
            NewJob("e"), new EnqueueOp("default", "e"),
            NewJob("s"), new SetJobStateOp("s", State("Scheduled", T0)),
            NewJob("p"), new SetJobStateOp("p", State("Processing", T0)),
            NewJob("f"), new SetJobStateOp("f", State("Failed", T0)),
            new IncrementCounterOp("stats:succeeded", 7, null),
            new IncrementCounterOp("stats:deleted", 2, null),
            new AddToSetOp("recurring-jobs", "rj-1", 0),
            new AddToSetOp("retries", "r-1", 0),
            new AnnounceServerOp("srv", 4, ["default"]));

        var stats = _store.GetStatistics();
        Assert.Equal(1, stats.Servers);
        Assert.Equal(1, stats.Queues);
        Assert.Equal(1, stats.Enqueued);
        Assert.Equal(1, stats.Scheduled);
        Assert.Equal(1, stats.Processing);
        Assert.Equal(1, stats.Failed);
        Assert.Equal(7, stats.Succeeded);
        Assert.Equal(2, stats.Deleted);
        Assert.Equal(1, stats.Recurring);
        Assert.Equal(1, stats.Retries);
    }

    // ----- exhaustiveness -----

    /// <summary>
    /// Applies one instance of every op type. A missing apply handler throws NotSupportedException
    /// from a committed log entry, which would brick every node of a real cluster, so this is the
    /// most important regression test in the suite.
    /// </summary>
    [Fact]
    public void EveryOpType_HasAnApplyHandler()
    {
        var token = Guid.NewGuid();
        var owner = Guid.NewGuid();
        StoreOp[] ops =
        [
            new CreateJobOp("j", "payload", [new("k", "v")], T0, T0.AddDays(1)),
            new SetJobParameterOp("j", "p", "v"),
            new SetJobStateOp("j", State("Enqueued", T0)),
            new AddJobStateOp("j", State("Custom", T0)),
            new ExpireJobOp("j", T0.AddDays(1)),
            new PersistJobOp("j"),
            new EnqueueOp("q", "j"),
            new FetchOp(["q"], token),
            new RenewFetchedOp(token),
            new RequeueFetchedOp(token),
            new AckFetchedOp(token),
            new IncrementCounterOp("c", 1, T0.AddDays(1)),
            new AddToSetOp("s", "v", 1),
            new AddRangeToSetOp("s", ["w"]),
            new RemoveFromSetOp("s", "v"),
            new ExpireSetOp("s", T0.AddDays(1)),
            new PersistSetOp("s"),
            new RemoveSetOp("s"),
            new InsertToListOp("l", "v"),
            new TrimListOp("l", 0, 9),
            new ExpireListOp("l", T0.AddDays(1)),
            new PersistListOp("l"),
            new RemoveFromListOp("l", "v"),
            new SetRangeInHashOp("h", [new("f", "v")]),
            new ExpireHashOp("h", T0.AddDays(1)),
            new PersistHashOp("h"),
            new RemoveHashOp("h"),
            new AnnounceServerOp("srv", 1, ["q"]),
            new HeartbeatOp("srv"),
            new RemoveTimedOutServersOp(TimeSpan.FromMinutes(5)),
            new RemoveServerOp("srv"),
            new TryAcquireLockOp("r", owner, TimeSpan.FromMinutes(1)),
            new ReleaseLockOp("r", owner),
            new MaintenanceOp(TimeSpan.FromMinutes(5)),
        ];

        var allOpTypes = typeof(StoreOp).Assembly.GetTypes()
            .Where(t => !t.IsAbstract && t.IsAssignableTo(typeof(StoreOp)))
            .ToHashSet();
        var covered = ops.Select(o => o.GetType()).ToHashSet();
        var missing = allOpTypes.Except(covered).Select(t => t.Name).ToList();
        Assert.True(missing.Count == 0, $"Ops missing from the apply-handler test: {string.Join(", ", missing)}");

        foreach (var op in ops) Apply(T0, op);
    }

    // ----- determinism across snapshot restore -----

    /// <summary>
    /// A node restored from a snapshot rebuilds its dictionaries with a different internal slot
    /// layout than a node that built them incrementally. Maintenance must still requeue stale
    /// fetches in the same order on both, otherwise the replicated queues diverge.
    /// </summary>
    [Fact]
    public void Maintenance_RequeuesStaleFetches_IdenticallyAfterSnapshotRestore()
    {
        Apply(T0, NewJob("a"), NewJob("b"), NewJob("c"), NewJob("d"),
            new EnqueueOp("q", "a"), new EnqueueOp("q", "b"), new EnqueueOp("q", "c"), new EnqueueOp("q", "d"));

        // Fetch four leases, then ack one and fetch another: the removal creates a free slot in the
        // dictionary that the next insert reuses, scrambling enumeration order relative to a store
        // that loads the same content fresh from a snapshot.
        var tokens = Enumerable.Range(0, 4).Select(_ => Guid.NewGuid()).ToArray();
        Apply(T0, new FetchOp(["q"], tokens[0]));
        Apply(T0.AddSeconds(1), new FetchOp(["q"], tokens[1]));
        Apply(T0.AddSeconds(2), new FetchOp(["q"], tokens[2]));
        Apply(T0, new AckFetchedOp(tokens[0]));
        Apply(T0.AddSeconds(3), new FetchOp(["q"], tokens[3]));

        var restored = new RaftStore();
        using (var reader = new BinaryReader(new MemoryStream(Serialize(_store)), Encoding.UTF8))
        {
            restored.LoadSnapshot(reader);
        }

        var maintenance = new Command
        {
            Id = Guid.NewGuid(),
            NowUtc = T0.AddMinutes(10),
            Ops = [new MaintenanceOp(TimeSpan.FromMinutes(5))],
        };
        _store.Apply(maintenance);
        restored.Apply(maintenance);

        Assert.Equal(3, _store.GetQueueLength("q"));
        Assert.Equal(_store.GetEnqueuedJobIds("q", 0, 10), restored.GetEnqueuedJobIds("q", 0, 10));
    }

    [Fact]
    public void Maintenance_RemovesEmptyQueues()
    {
        Apply(T0, NewJob("a"), new EnqueueOp("q", "a"));
        Apply(T0, new FetchOp(["q"], Guid.NewGuid()));

        Apply(T0, new MaintenanceOp(TimeSpan.FromMinutes(5)));

        Assert.Empty(_store.GetQueues(5));
    }

    // ----- snapshot -----

    [Fact]
    public void Snapshot_RoundtripsTheEntireState()
    {
        Apply(T0,
            NewJob("a"), new SetJobStateOp("a", State("Succeeded", T0, "done")),
            NewJob("b"), new EnqueueOp("default", "b"),
            new AddToSetOp("schedule", "a", 123.5),
            new InsertToListOp("console", "line"),
            new SetRangeInHashOp("recurring-job:x", [new("Cron", "* * * * *"), new("Null", null)]),
            new IncrementCounterOp("stats:succeeded", 5, T0.AddDays(30)),
            new AnnounceServerOp("srv", 4, ["default"]),
            new TryAcquireLockOp("lock", Guid.NewGuid(), TimeSpan.FromMinutes(2)));
        Apply(T0, new FetchOp(["default"], Guid.NewGuid()));

        var snapshot = Serialize(_store);
        var restored = new RaftStore();
        using (var reader = new BinaryReader(new MemoryStream(snapshot), Encoding.UTF8))
        {
            restored.LoadSnapshot(reader);
        }

        // A second serialization of the restored store must be byte-identical.
        Assert.Equal(snapshot, Serialize(restored));

        var job = restored.GetJob("a")!;
        Assert.Equal("Succeeded", job.CurrentState!.Name);
        Assert.Equal("done", job.CurrentState.Reason);
        Assert.Equal(1, restored.GetStateCount("Succeeded")); // index rebuilt on load
        Assert.Equal(1, restored.GetFetchedCount("default"));
        Assert.Equal(["a"], restored.GetFirstByLowestScoreFromSet("schedule", 0, 200, 10));
        Assert.Equal(["line"], restored.GetAllItemsFromList("console"));
        Assert.Null(restored.GetAllEntriesFromHash("recurring-job:x")!["Null"]);
        Assert.Equal(5, restored.GetCounter("stats:succeeded"));
        Assert.Single(restored.GetServers());
        Assert.Equal(0, restored.GetQueueLength("default")); // b was fetched
    }

    // ----- read boundaries and op edge cases -----

    [Fact]
    public void GetRangeFromList_ClampsNegativeStart_AndHandlesEmptyRange()
    {
        Apply(T0, new InsertToListOp("l", "a"), new InsertToListOp("l", "b")); // newest-first: b, a

        Assert.Equal(["b", "a"], _store.GetRangeFromList("l", -5, 10)); // negative start clamps to 0
        Assert.Empty(_store.GetRangeFromList("l", 5, 1));               // from > to
        Assert.Equal(["b", "a"], _store.GetRangeFromList("l", 0, 99)); // past-end upper bound
    }

    [Fact]
    public void RequeueFetched_UnknownToken_IsNoOp()
    {
        var effects = ApplyWithEffects(T0, new RequeueFetchedOp(Guid.NewGuid()));
        Assert.Null(effects.SignaledQueues);
    }

    [Fact]
    public void RequeueFetched_DoesNotReEnqueue_AnEvictedJob()
    {
        Apply(T0, NewJob("a"), new EnqueueOp("q", "a"), new ExpireJobOp("a", T0.AddMinutes(1)));
        var token = Guid.NewGuid();
        Apply(T0, new FetchOp(["q"], token));
        Apply(T0.AddMinutes(2), new MaintenanceOp(TimeSpan.FromMinutes(5))); // evicts "a" (expired), lease not yet stale

        Apply(T0.AddMinutes(2), new RequeueFetchedOp(token)); // drops the lease; "a" is gone so it is not re-enqueued
        Assert.Equal(0, _store.GetQueueLength("q"));
    }

    [Fact]
    public void Counter_IsRecreated_AfterReachingZero()
    {
        Apply(T0, new IncrementCounterOp("c", 1, null));
        Apply(T0, new IncrementCounterOp("c", -1, null)); // back to zero -> entry removed
        Assert.Equal(0, _store.GetCounter("c"));

        Apply(T0, new IncrementCounterOp("c", 1, null));   // re-created from nothing
        Assert.Equal(1, _store.GetCounter("c"));
    }

    [Fact]
    public void RemoveServer_RemovesOnlyThatServer()
    {
        Apply(T0, new AnnounceServerOp("a", 1, []), new AnnounceServerOp("b", 1, []));
        Apply(T0, new RemoveServerOp("a"));
        Assert.Equal("b", Assert.Single(_store.GetServers()).Id);
    }

    [Fact]
    public void RemoveTimedOutServers_RemovesOnlyStaleServers()
    {
        Apply(T0, new AnnounceServerOp("stale", 1, []));
        Apply(T0.AddMinutes(10), new AnnounceServerOp("fresh", 1, []));

        var removed = (int)Apply(T0.AddMinutes(11), new RemoveTimedOutServersOp(TimeSpan.FromMinutes(5)))!;
        Assert.Equal(1, removed);
        Assert.Equal("fresh", Assert.Single(_store.GetServers()).Id);
    }

    // ----- snapshot edge cases -----

    [Fact]
    public void LoadSnapshot_Throws_OnUnknownVersion()
    {
        var snapshot = Serialize(_store);
        snapshot[0] = 0xFF; // corrupt the version byte
        var other = new RaftStore();
        using var reader = new BinaryReader(new MemoryStream(snapshot), Encoding.UTF8);
        Assert.Throws<NotSupportedException>(() => other.LoadSnapshot(reader));
    }

    [Fact]
    public void Snapshot_RoundtripsAnEmptyStore()
    {
        var snapshot = Serialize(_store); // brand-new, every table empty
        var other = new RaftStore();
        using (var reader = new BinaryReader(new MemoryStream(snapshot), Encoding.UTF8)) other.LoadSnapshot(reader);
        Assert.Equal(snapshot, Serialize(other)); // every zero-count table prefix round-trips
    }

    private static byte[] Serialize(RaftStore store)
    {
        using var stream = new MemoryStream();
        using (var writer = new BinaryWriter(stream, Encoding.UTF8, leaveOpen: true))
        {
            store.WriteSnapshot(writer);
        }

        return stream.ToArray();
    }
}
