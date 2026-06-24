using System.Text;
using Hangfire.Raft.Commands;
using Hangfire.Raft.State;
using TUnit.Assertions.Enums;

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

    [Test]
    public async Task CreateJob_StoresInvocationAndParameters()
    {
        Apply(T0, NewJob("a"));

        var job = _store.GetJob("a");
        await Assert.That(job).IsNotNull();
        await Assert.That(job.InvocationData).IsEqualTo("payload-a");
        await Assert.That(job.CreatedAt).IsEqualTo(T0);
        await Assert.That(job.ExpireAt).IsEqualTo(T0.AddDays(1));
        await Assert.That(job.Parameters["Culture"]).IsEqualTo("de-CH");
        await Assert.That(job.CurrentState).IsNull();
        await Assert.That(job.History).IsEmpty();
    }

    [Test]
    public async Task GetJob_ReturnsNull_WhenMissing() => await Assert.That(_store.GetJob("missing")).IsNull();

    [Test]
    public async Task SetJobParameter_AddsOverwritesAndAllowsNull()
    {
        Apply(T0, NewJob("a"));
        Apply(T0, new SetJobParameterOp("a", "X", "1"));
        Apply(T0, new SetJobParameterOp("a", "X", "2"));
        Apply(T0, new SetJobParameterOp("a", "N", null));

        await Assert.That(_store.GetJobParameter("a", "X")).IsEqualTo("2");
        await Assert.That(_store.GetJobParameter("a", "N")).IsNull();
        await Assert.That(_store.GetJobParameter("a", "missing")).IsNull();
        await Assert.That(_store.GetJobParameter("missing", "X")).IsNull();
    }

    [Test]
    public async Task SetJobParameter_IsNoOp_ForMissingJob()
    {
        Apply(T0, new SetJobParameterOp("missing", "X", "1"));
        await Assert.That(_store.GetJob("missing")).IsNull();
    }

    [Test]
    public async Task SetJobState_UpdatesCurrentStateHistoryAndIndex()
    {
        Apply(T0, NewJob("a"));
        Apply(T0, new SetJobStateOp("a", State("Enqueued", T0)));
        Apply(T0, new SetJobStateOp("a", State("Processing", T0.AddSeconds(1))));

        var job = _store.GetJob("a")!;
        await Assert.That(job.CurrentState!.Name).IsEqualTo("Processing");
        await Assert.That(job.History.Count).IsEqualTo(2);
        await Assert.That(_store.GetStateCount("Enqueued")).IsEqualTo(0);
        await Assert.That(_store.GetStateCount("Processing")).IsEqualTo(1);
    }

    [Test]
    public async Task AddJobState_AppendsHistoryWithoutChangingCurrentState()
    {
        Apply(T0, NewJob("a"));
        Apply(T0, new SetJobStateOp("a", State("Enqueued", T0)));
        Apply(T0, new AddJobStateOp("a", State("Custom", T0.AddSeconds(1))));

        var job = _store.GetJob("a")!;
        await Assert.That(job.CurrentState!.Name).IsEqualTo("Enqueued");
        await Assert.That(job.History.Count).IsEqualTo(2);
        await Assert.That(_store.GetStateCount("Custom")).IsEqualTo(0);
    }

    [Test]
    public async Task GetJobsByState_OrdersAndPages()
    {
        for (var i = 0; i < 5; i++)
        {
            var id = $"job-{i}";
            Apply(T0, NewJob(id));
            Apply(T0, new SetJobStateOp(id, State("Succeeded", T0.AddSeconds(i))));
        }

        var ascending = _store.GetJobsByState("Succeeded", 0, 10, ascending: true).Select(j => j.Id).ToList();
        await Assert.That(ascending).IsEquivalentTo(["job-0", "job-1", "job-2", "job-3", "job-4"], CollectionOrdering.Matching);

        var newestFirst = _store.GetJobsByState("Succeeded", 0, 2, ascending: false).Select(j => j.Id).ToList();
        await Assert.That(newestFirst).IsEquivalentTo(["job-4", "job-3"], CollectionOrdering.Matching);

        var page = _store.GetJobsByState("Succeeded", 2, 2, ascending: true).Select(j => j.Id).ToList();
        await Assert.That(page).IsEquivalentTo(["job-2", "job-3"], CollectionOrdering.Matching);

        await Assert.That(_store.GetJobsByState("Succeeded", 10, 5, ascending: true)).IsEmpty();
        await Assert.That(_store.GetJobsByState("Unknown", 0, 5, ascending: true)).IsEmpty();
    }

    [Test]
    public async Task GetJobsByState_BreaksTimestampTiesById()
    {
        foreach (var id in new[] { "b", "a", "c" })
        {
            Apply(T0, NewJob(id));
            Apply(T0, new SetJobStateOp(id, State("Scheduled", T0)));
        }

        var ids = _store.GetJobsByState("Scheduled", 0, 10, ascending: true).Select(j => j.Id).ToList();
        await Assert.That(ids).IsEquivalentTo(["a", "b", "c"], CollectionOrdering.Matching);
    }

    [Test]
    public async Task PersistJob_ClearsExpiry_AndExpireJobSetsIt()
    {
        Apply(T0, NewJob("a"));
        Apply(T0, new PersistJobOp("a"));
        await Assert.That(_store.GetJob("a")!.ExpireAt).IsNull();

        Apply(T0, new ExpireJobOp("a", T0.AddMinutes(30)));
        await Assert.That(_store.GetJob("a")!.ExpireAt).IsEqualTo(T0.AddMinutes(30));
    }

    [Test]
    public async Task Maintenance_EvictsExpiredJobs_AndTheirIndexEntries()
    {
        Apply(T0, NewJob("a"));
        Apply(T0, new SetJobStateOp("a", State("Succeeded", T0)));
        Apply(T0, new ExpireJobOp("a", T0.AddMinutes(1)));

        Apply(T0.AddSeconds(59), new MaintenanceOp(TimeSpan.FromMinutes(5)));
        await Assert.That(_store.GetJob("a")).IsNotNull();

        Apply(T0.AddMinutes(1), new MaintenanceOp(TimeSpan.FromMinutes(5)));
        await Assert.That(_store.GetJob("a")).IsNull();
        await Assert.That(_store.GetStateCount("Succeeded")).IsEqualTo(0);
    }

    // ----- queues and fetching -----

    [Test]
    public async Task Fetch_ReturnsJobsInFifoOrder()
    {
        Apply(T0, NewJob("a"), NewJob("b"), new EnqueueOp("default", "a"), new EnqueueOp("default", "b"));

        var first = (FetchResult?)Apply(T0, new FetchOp(["default"], Guid.NewGuid()));
        var second = (FetchResult?)Apply(T0, new FetchOp(["default"], Guid.NewGuid()));
        var third = (FetchResult?)Apply(T0, new FetchOp(["default"], Guid.NewGuid()));

        await Assert.That(first!.Value.JobId).IsEqualTo("a");
        await Assert.That(second!.Value.JobId).IsEqualTo("b");
        await Assert.That(third).IsNull();
    }

    [Test]
    public async Task Fetch_RespectsQueuePriorityOrder()
    {
        Apply(T0, NewJob("low"), NewJob("crit"),
            new EnqueueOp("default", "low"), new EnqueueOp("critical", "crit"));

        var fetched = (FetchResult?)Apply(T0, new FetchOp(["critical", "default"], Guid.NewGuid()));

        await Assert.That(fetched!.Value.JobId).IsEqualTo("crit");
        await Assert.That(fetched.Value.Queue).IsEqualTo("critical");
    }

    [Test]
    public async Task Fetch_SkipsJobsThatNoLongerExist()
    {
        Apply(T0, NewJob("alive"), new EnqueueOp("default", "ghost"), new EnqueueOp("default", "alive"));

        var fetched = (FetchResult?)Apply(T0, new FetchOp(["default"], Guid.NewGuid()));

        await Assert.That(fetched!.Value.JobId).IsEqualTo("alive");
    }

    [Test]
    public async Task RequeueFetched_PutsJobBackAtTheHead()
    {
        Apply(T0, NewJob("a"), NewJob("b"), new EnqueueOp("q", "a"), new EnqueueOp("q", "b"));
        var token = Guid.NewGuid();
        Apply(T0, new FetchOp(["q"], token));

        var effects = ApplyWithEffects(T0, new RequeueFetchedOp(token));

        await Assert.That(effects.SignaledQueues!).Contains("q");
        var next = (FetchResult?)Apply(T0, new FetchOp(["q"], Guid.NewGuid()));
        await Assert.That(next!.Value.JobId).IsEqualTo("a");
    }

    [Test]
    public async Task AckFetched_RemovesTheLease()
    {
        Apply(T0, NewJob("a"), new EnqueueOp("q", "a"));
        var token = Guid.NewGuid();
        Apply(T0, new FetchOp(["q"], token));

        Apply(T0, new AckFetchedOp(token));

        await Assert.That(_store.GetFetchedCount("q")).IsEqualTo(0);
        await Assert.That((bool)Apply(T0, new RenewFetchedOp(token))!).IsFalse();
    }

    [Test]
    public async Task Maintenance_ReclaimsStaleFetches_ButNotRenewedOnes()
    {
        Apply(T0, NewJob("stale"), NewJob("active"), new EnqueueOp("q", "stale"), new EnqueueOp("q", "active"));
        var staleToken = Guid.NewGuid();
        var activeToken = Guid.NewGuid();
        Apply(T0, new FetchOp(["q"], staleToken));
        Apply(T0, new FetchOp(["q"], activeToken));

        await Assert.That((bool)Apply(T0.AddMinutes(4), new RenewFetchedOp(activeToken))!).IsTrue();

        var effects = ApplyWithEffects(T0.AddMinutes(5), new MaintenanceOp(TimeSpan.FromMinutes(5)));

        await Assert.That(effects.SignaledQueues!).Contains("q");
        await Assert.That(_store.GetFetchedCount("q")).IsEqualTo(1); // the renewed lease survives
        var reclaimed = (FetchResult?)Apply(T0.AddMinutes(5), new FetchOp(["q"], Guid.NewGuid()));
        await Assert.That(reclaimed!.Value.JobId).IsEqualTo("stale");
    }

    [Test]
    public async Task Enqueue_SignalsTheQueue()
    {
        Apply(T0, NewJob("a"));
        var effects = ApplyWithEffects(T0, new EnqueueOp("q", "a"));
        await Assert.That(effects.SignaledQueues!).Contains("q");
    }

    // ----- counters -----

    [Test]
    public async Task Counters_AccumulateAndVanishAtZeroWithoutExpiry()
    {
        Apply(T0, new IncrementCounterOp("c", 1, null), new IncrementCounterOp("c", 1, null));
        await Assert.That(_store.GetCounter("c")).IsEqualTo(2);

        Apply(T0, new IncrementCounterOp("c", -2, null));
        await Assert.That(_store.GetCounter("c")).IsEqualTo(0);
    }

    [Test]
    public async Task Counters_ExpiryOnlyExtends()
    {
        Apply(T0, new IncrementCounterOp("c", 1, T0.AddHours(2)));
        Apply(T0, new IncrementCounterOp("c", 1, T0.AddHours(1)));

        Apply(T0.AddMinutes(90), new MaintenanceOp(TimeSpan.FromMinutes(5)));
        await Assert.That(_store.GetCounter("c")).IsEqualTo(2); // still alive: the later, shorter expiry did not shrink the TTL

        Apply(T0.AddHours(2), new MaintenanceOp(TimeSpan.FromMinutes(5)));
        await Assert.That(_store.GetCounter("c")).IsEqualTo(0);
    }

    // ----- sets -----

    [Test]
    public async Task Sets_UpsertScore_AndOrderByScoreThenValue()
    {
        Apply(T0,
            new AddToSetOp("s", "b", 2),
            new AddToSetOp("s", "a", 2),
            new AddToSetOp("s", "c", 1),
            new AddToSetOp("s", "b", 0.5)); // moves b to the front

        await Assert.That(_store.GetRangeFromSet("s", 0, 10)).IsEquivalentTo(["b", "c", "a"], CollectionOrdering.Matching);
        await Assert.That(_store.GetSetCount("s")).IsEqualTo(3);
    }

    [Test]
    public async Task GetFirstByLowestScoreFromSet_RespectsInclusiveBoundsAndCount()
    {
        Apply(T0, new AddToSetOp("s", "a", 1), new AddToSetOp("s", "b", 2), new AddToSetOp("s", "c", 3));

        await Assert.That(_store.GetFirstByLowestScoreFromSet("s", 1, 2, 10)).IsEquivalentTo(["a", "b"], CollectionOrdering.Matching);
        await Assert.That(_store.GetFirstByLowestScoreFromSet("s", 1, 3, 1)).IsEquivalentTo(["a"], CollectionOrdering.Matching);
        await Assert.That(_store.GetFirstByLowestScoreFromSet("s", 4, 9, 10)).IsEmpty();
        await Assert.That(_store.GetFirstByLowestScoreFromSet("missing", 0, 10, 10)).IsEmpty();
    }

    [Test]
    public async Task Sets_RemoveValue_DropsEmptySet()
    {
        Apply(T0, new AddToSetOp("s", "a", 1));
        Apply(T0, new RemoveFromSetOp("s", "a"));

        await Assert.That(_store.GetSetCount("s")).IsEqualTo(0);
        await Assert.That(_store.GetSetContains("s", "a")).IsFalse();
    }

    [Test]
    public async Task Sets_AddRange_UsesScoreZero()
    {
        Apply(T0, new AddRangeToSetOp("s", ["x", "y"]));
        await Assert.That(_store.GetFirstByLowestScoreFromSet("s", 0, 0, 10)).IsEquivalentTo(["x", "y"], CollectionOrdering.Matching);
    }

    [Test]
    public async Task GetSetCount_WithLimit_Caps()
    {
        Apply(T0, new AddRangeToSetOp("s1", ["a", "b"]), new AddRangeToSetOp("s2", ["c", "d"]));
        await Assert.That(_store.GetSetCount(["s1", "s2"], 3)).IsEqualTo(3);
        await Assert.That(_store.GetSetCount(["s1", "s2"], 100)).IsEqualTo(4);
    }

    [Test]
    public async Task Sets_ExpireAndPersist_ControlEviction()
    {
        Apply(T0, new AddToSetOp("s", "a", 1), new ExpireSetOp("s", T0.AddMinutes(1)));
        await Assert.That(_store.GetSetTtl("s", T0) > TimeSpan.Zero).IsTrue();

        Apply(T0, new PersistSetOp("s"));
        await Assert.That(_store.GetSetTtl("s", T0) < TimeSpan.Zero).IsTrue();

        Apply(T0, new ExpireSetOp("s", T0.AddMinutes(1)));
        Apply(T0.AddMinutes(2), new MaintenanceOp(TimeSpan.FromMinutes(5)));
        await Assert.That(_store.GetSetCount("s")).IsEqualTo(0);
    }

    // ----- lists -----

    [Test]
    public async Task Lists_AreNewestFirst()
    {
        Apply(T0, new InsertToListOp("l", "first"), new InsertToListOp("l", "second"));

        await Assert.That(_store.GetAllItemsFromList("l")).IsEquivalentTo(["second", "first"], CollectionOrdering.Matching);
        await Assert.That(_store.GetRangeFromList("l", 0, 0)).IsEquivalentTo(["second"], CollectionOrdering.Matching);
        await Assert.That(_store.GetListCount("l")).IsEqualTo(2);
    }

    [Test]
    public async Task Lists_RemoveDeletesAllOccurrences_AndDropsEmptyList()
    {
        Apply(T0, new InsertToListOp("l", "x"), new InsertToListOp("l", "y"), new InsertToListOp("l", "x"));
        Apply(T0, new RemoveFromListOp("l", "x"));
        await Assert.That(_store.GetAllItemsFromList("l")).IsEquivalentTo(["y"], CollectionOrdering.Matching);

        Apply(T0, new RemoveFromListOp("l", "y"));
        await Assert.That(_store.GetListCount("l")).IsEqualTo(0);
    }

    [Test]
    public async Task Lists_ExpireAndPersist_ControlEviction()
    {
        Apply(T0, new InsertToListOp("l", "x"), new ExpireListOp("l", T0.AddMinutes(1)));
        await Assert.That(_store.GetListTtl("l", T0) > TimeSpan.Zero).IsTrue();

        Apply(T0, new PersistListOp("l"));
        await Assert.That(_store.GetListTtl("l", T0) < TimeSpan.Zero).IsTrue();
        Apply(T0.AddMinutes(2), new MaintenanceOp(TimeSpan.FromMinutes(5)));
        await Assert.That(_store.GetListCount("l")).IsEqualTo(1); // persisted: survives

        Apply(T0, new ExpireListOp("l", T0.AddMinutes(1)));
        Apply(T0.AddMinutes(1), new MaintenanceOp(TimeSpan.FromMinutes(5)));
        await Assert.That(_store.GetListCount("l")).IsEqualTo(0);
    }

    [Test]
    public async Task TrimList_KeepsInclusiveRangeOfNewestFirstView()
    {
        Apply(T0,
            new InsertToListOp("l", "1"), new InsertToListOp("l", "2"),
            new InsertToListOp("l", "3"), new InsertToListOp("l", "4")); // list: 4,3,2,1

        Apply(T0, new TrimListOp("l", 1, 2));
        await Assert.That(_store.GetAllItemsFromList("l")).IsEquivalentTo(["3", "2"], CollectionOrdering.Matching);

        Apply(T0, new TrimListOp("l", 5, 9));
        await Assert.That(_store.GetListCount("l")).IsEqualTo(0);
    }

    // ----- hashes -----

    [Test]
    public async Task Hashes_MergeFields_AndSupportNullValues()
    {
        Apply(T0, new SetRangeInHashOp("h", [new("a", "1"), new("b", null)]));
        Apply(T0, new SetRangeInHashOp("h", [new("a", "2"), new("c", "3")]));

        var fields = _store.GetAllEntriesFromHash("h")!;
        await Assert.That(fields["a"]).IsEqualTo("2");
        await Assert.That(fields["b"]).IsNull();
        await Assert.That(fields["c"]).IsEqualTo("3");
        await Assert.That(_store.GetHashCount("h")).IsEqualTo(3);
        await Assert.That(_store.GetValueFromHash("h", "c")).IsEqualTo("3");
        await Assert.That(_store.GetAllEntriesFromHash("missing")).IsNull();
    }

    [Test]
    public async Task Hashes_RemoveAndExpire()
    {
        Apply(T0, new SetRangeInHashOp("h", [new("a", "1")]), new ExpireHashOp("h", T0.AddMinutes(1)));
        Apply(T0.AddMinutes(1), new MaintenanceOp(TimeSpan.FromMinutes(5)));
        await Assert.That(_store.GetHashCount("h")).IsEqualTo(0);

        Apply(T0, new SetRangeInHashOp("h2", [new("a", "1")]));
        Apply(T0, new RemoveHashOp("h2"));
        await Assert.That(_store.GetAllEntriesFromHash("h2")).IsNull();
    }

    // ----- servers -----

    [Test]
    public async Task Servers_AnnounceHeartbeatAndTimeout()
    {
        Apply(T0, new AnnounceServerOp("srv-1", 8, ["default"]));
        await Assert.That((bool)Apply(T0.AddMinutes(1), new HeartbeatOp("srv-1"))!).IsTrue();
        await Assert.That((bool)Apply(T0, new HeartbeatOp("unknown"))!).IsFalse();

        // heartbeat at T0+1min; timeout 5min; at T0+6min the server is exactly at the cutoff and survives
        await Assert.That((int)Apply(T0.AddMinutes(6), new RemoveTimedOutServersOp(TimeSpan.FromMinutes(5)))!).IsEqualTo(0);
        await Assert.That((int)Apply(T0.AddMinutes(7), new RemoveTimedOutServersOp(TimeSpan.FromMinutes(5)))!).IsEqualTo(1);
        await Assert.That(_store.GetServers()).IsEmpty();
    }

    [Test]
    public async Task Servers_ReannounceReplaces()
    {
        Apply(T0, new AnnounceServerOp("srv-1", 8, ["default"]));
        Apply(T0.AddMinutes(1), new AnnounceServerOp("srv-1", 16, ["critical"]));

        var server = await Assert.That(_store.GetServers()).HasSingleItem();
        await Assert.That(server.WorkerCount).IsEqualTo(16);
        await Assert.That(server.Queues).IsEquivalentTo(["critical"], CollectionOrdering.Matching);
        await Assert.That(server.StartedAt).IsEqualTo(T0.AddMinutes(1));
    }

    // ----- locks -----

    [Test]
    public async Task Locks_MutualExclusion_SameOwnerRenews_ExpiryFrees()
    {
        var owner1 = Guid.NewGuid();
        var owner2 = Guid.NewGuid();
        var lease = TimeSpan.FromMinutes(2);

        await Assert.That((bool)Apply(T0, new TryAcquireLockOp("r", owner1, lease))!).IsTrue();
        await Assert.That((bool)Apply(T0, new TryAcquireLockOp("r", owner2, lease))!).IsFalse();
        await Assert.That((bool)Apply(T0.AddMinutes(1), new TryAcquireLockOp("r", owner1, lease))!).IsTrue(); // renewal extends to T0+3min

        await Assert.That((bool)Apply(T0.AddMinutes(2.5), new TryAcquireLockOp("r", owner2, lease))!).IsFalse();
        await Assert.That((bool)Apply(T0.AddMinutes(3), new TryAcquireLockOp("r", owner2, lease))!).IsTrue(); // lease expired
    }

    [Test]
    public async Task Locks_ReleaseByOwnerOnly_AndSignals()
    {
        var owner = Guid.NewGuid();
        var other = Guid.NewGuid();
        Apply(T0, new TryAcquireLockOp("r", owner, TimeSpan.FromMinutes(2)));

        var foreignRelease = ApplyWithEffects(T0, new ReleaseLockOp("r", other));
        await Assert.That(foreignRelease.LocksReleased).IsFalse();
        await Assert.That((bool)Apply(T0, new TryAcquireLockOp("r", other, TimeSpan.FromMinutes(2)))!).IsFalse();

        var ownerRelease = ApplyWithEffects(T0, new ReleaseLockOp("r", owner));
        await Assert.That(ownerRelease.LocksReleased).IsTrue();
        await Assert.That((bool)Apply(T0, new TryAcquireLockOp("r", other, TimeSpan.FromMinutes(2)))!).IsTrue();
    }

    [Test]
    public async Task Maintenance_DropsExpiredLocks()
    {
        Apply(T0, new TryAcquireLockOp("r", Guid.NewGuid(), TimeSpan.FromMinutes(2)));
        var effects = ApplyWithEffects(T0.AddMinutes(2), new MaintenanceOp(TimeSpan.FromMinutes(5)));

        await Assert.That(effects.LocksReleased).IsTrue();
        await Assert.That((bool)Apply(T0.AddMinutes(2), new TryAcquireLockOp("r", Guid.NewGuid(), TimeSpan.FromMinutes(2)))!).IsTrue();
    }

    // ----- statistics -----

    [Test]
    public async Task Statistics_AggregateAcrossTables()
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
        await Assert.That(stats.Servers).IsEqualTo(1);
        await Assert.That(stats.Queues).IsEqualTo(1);
        await Assert.That(stats.Enqueued).IsEqualTo(1);
        await Assert.That(stats.Scheduled).IsEqualTo(1);
        await Assert.That(stats.Processing).IsEqualTo(1);
        await Assert.That(stats.Failed).IsEqualTo(1);
        await Assert.That(stats.Succeeded).IsEqualTo(7);
        await Assert.That(stats.Deleted).IsEqualTo(2);
        await Assert.That(stats.Recurring).IsEqualTo(1);
        await Assert.That(stats.Retries).IsEqualTo(1);
    }

    // ----- exhaustiveness -----

    /// <summary>
    /// Applies one instance of every op type. A missing apply handler throws NotSupportedException
    /// from a committed log entry, which would brick every node of a real cluster, so this is the
    /// most important regression test in the suite.
    /// </summary>
    [Test]
    public async Task EveryOpType_HasAnApplyHandler()
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
        await Assert.That(missing).IsEmpty(); // failure lists the ops missing from the apply-handler test

        foreach (var op in ops) Apply(T0, op);
    }

    // ----- determinism across snapshot restore -----

    /// <summary>
    /// A node restored from a snapshot rebuilds its dictionaries with a different internal slot
    /// layout than a node that built them incrementally. Maintenance must still requeue stale
    /// fetches in the same order on both, otherwise the replicated queues diverge.
    /// </summary>
    [Test]
    public async Task Maintenance_RequeuesStaleFetches_IdenticallyAfterSnapshotRestore()
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

        await Assert.That(_store.GetQueueLength("q")).IsEqualTo(3);
        await Assert.That(restored.GetEnqueuedJobIds("q", 0, 10)).IsEquivalentTo(_store.GetEnqueuedJobIds("q", 0, 10), CollectionOrdering.Matching);
    }

    [Test]
    public async Task Maintenance_RemovesEmptyQueues()
    {
        Apply(T0, NewJob("a"), new EnqueueOp("q", "a"));
        Apply(T0, new FetchOp(["q"], Guid.NewGuid()));

        Apply(T0, new MaintenanceOp(TimeSpan.FromMinutes(5)));

        await Assert.That(_store.GetQueues(5)).IsEmpty();
    }

    // ----- snapshot -----

    [Test]
    public async Task Snapshot_RoundtripsTheEntireState()
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
        await Assert.That(Serialize(restored)).IsEquivalentTo(snapshot, CollectionOrdering.Matching);

        var job = restored.GetJob("a")!;
        await Assert.That(job.CurrentState!.Name).IsEqualTo("Succeeded");
        await Assert.That(job.CurrentState.Reason).IsEqualTo("done");
        await Assert.That(restored.GetStateCount("Succeeded")).IsEqualTo(1); // index rebuilt on load
        await Assert.That(restored.GetFetchedCount("default")).IsEqualTo(1);
        await Assert.That(restored.GetFirstByLowestScoreFromSet("schedule", 0, 200, 10)).IsEquivalentTo(["a"], CollectionOrdering.Matching);
        await Assert.That(restored.GetAllItemsFromList("console")).IsEquivalentTo(["line"], CollectionOrdering.Matching);
        await Assert.That(restored.GetAllEntriesFromHash("recurring-job:x")!["Null"]).IsNull();
        await Assert.That(restored.GetCounter("stats:succeeded")).IsEqualTo(5);
        await Assert.That(restored.GetServers()).HasSingleItem();
        await Assert.That(restored.GetQueueLength("default")).IsEqualTo(0); // b was fetched
    }

    // ----- read boundaries and op edge cases -----

    [Test]
    public async Task GetRangeFromList_ClampsNegativeStart_AndHandlesEmptyRange()
    {
        Apply(T0, new InsertToListOp("l", "a"), new InsertToListOp("l", "b")); // newest-first: b, a

        await Assert.That(_store.GetRangeFromList("l", -5, 10)).IsEquivalentTo(["b", "a"], CollectionOrdering.Matching); // negative start clamps to 0
        await Assert.That(_store.GetRangeFromList("l", 5, 1)).IsEmpty();               // from > to
        await Assert.That(_store.GetRangeFromList("l", 0, 99)).IsEquivalentTo(["b", "a"], CollectionOrdering.Matching); // past-end upper bound
    }

    [Test]
    public async Task RequeueFetched_UnknownToken_IsNoOp()
    {
        var effects = ApplyWithEffects(T0, new RequeueFetchedOp(Guid.NewGuid()));
        await Assert.That(effects.SignaledQueues).IsNull();
    }

    [Test]
    public async Task Maintenance_DoesNotEvict_AFetchedJob()
    {
        // A job that expires WHILE held under a fetch lease must not be evicted out from under the
        // worker; otherwise an ExpireJob racing an in-flight fetch would drop it with zero runs.
        Apply(T0, NewJob("a"), new EnqueueOp("q", "a"), new ExpireJobOp("a", T0.AddMinutes(1)));
        var token = Guid.NewGuid();
        Apply(T0, new FetchOp(["q"], token));

        // Past the job's expiry, but the lease is still fresh: maintenance keeps the fetched job.
        Apply(T0.AddMinutes(2), new MaintenanceOp(TimeSpan.FromMinutes(5)));
        await Assert.That(_store.GetJob("a")).IsNotNull();

        // Releasing the lease re-enqueues it (the job still exists), so it can still run.
        Apply(T0.AddMinutes(2), new RequeueFetchedOp(token));
        await Assert.That(_store.GetQueueLength("q")).IsEqualTo(1);
    }

    [Test]
    public async Task Maintenance_DoesNotLose_AReclaimedExpiredJob_OnTheNextPass()
    {
        // A job whose expiry elapsed while it was held under a fetch lease (an ExpireJob that raced the
        // fetch) is protected while the lease is live. Once the lease goes stale and is reclaimed the job is
        // re-enqueued to run again; the reclaim must clear its expiry, otherwise the NEXT maintenance pass --
        // where the job is no longer fetch-protected -- would evict it with zero executions and strip it from
        // the queue. This is the cross-pass silent-loss regression.
        Apply(T0, NewJob("a"), new EnqueueOp("q", "a"));
        var token = Guid.NewGuid();
        Apply(T0, new FetchOp(["q"], token));
        Apply(T0, new ExpireJobOp("a", T0.AddMinutes(1)));

        // Pass 1: the lease is stale (10min elapsed > 5min invisibility), so the job is reclaimed, re-enqueued
        // and its expiry cleared. It is reported in the summary so the leader can warn about the possible re-run.
        var summary = (MaintenanceSummary)Apply(T0.AddMinutes(10), new MaintenanceOp(TimeSpan.FromMinutes(5)))!;
        await Assert.That(summary.StaleFetchesReclaimed).IsEqualTo(1);
        await Assert.That(_store.GetJob("a")!.ExpireAt).IsNull();
        await Assert.That(_store.GetQueueLength("q")).IsEqualTo(1);

        // Pass 2: the job is no longer fetch-protected. With expiry cleared on reclaim it survives; without
        // the fix it would be evicted here and dropped from the queue.
        Apply(T0.AddMinutes(20), new MaintenanceOp(TimeSpan.FromMinutes(5)));
        await Assert.That(_store.GetJob("a")).IsNotNull();
        await Assert.That(_store.GetQueueLength("q")).IsEqualTo(1);
    }

    [Test]
    public async Task RequeueFetched_ClearsExpiry_SoARequeuedExpiredJobSurvivesMaintenance()
    {
        Apply(T0, NewJob("a"), new EnqueueOp("q", "a"));
        var token = Guid.NewGuid();
        Apply(T0, new FetchOp(["q"], token));
        Apply(T0, new ExpireJobOp("a", T0.AddMinutes(1)));

        Apply(T0, new RequeueFetchedOp(token)); // re-enqueued to run again -> expiry cleared
        await Assert.That(_store.GetJob("a")!.ExpireAt).IsNull();

        Apply(T0.AddMinutes(10), new MaintenanceOp(TimeSpan.FromMinutes(5)));
        await Assert.That(_store.GetJob("a")).IsNotNull();
        await Assert.That(_store.GetQueueLength("q")).IsEqualTo(1);
    }

    [Test]
    public async Task Maintenance_Summary_ReportsEvictionAndReclaimCounts()
    {
        // A genuinely expired, unfetched job is evicted and reported; no lease is stale, so no reclaim.
        Apply(T0, NewJob("a"), new SetJobStateOp("a", State("Succeeded", T0)), new ExpireJobOp("a", T0.AddMinutes(1)));

        var summary = (MaintenanceSummary)Apply(T0.AddMinutes(2), new MaintenanceOp(TimeSpan.FromMinutes(5)))!;
        await Assert.That(summary.EvictedJobs).IsEqualTo(1);
        await Assert.That(summary.StaleFetchesReclaimed).IsEqualTo(0);
        await Assert.That(_store.GetJob("a")).IsNull();
    }

    [Test]
    public async Task Counter_IsRecreated_AfterReachingZero()
    {
        Apply(T0, new IncrementCounterOp("c", 1, null));
        Apply(T0, new IncrementCounterOp("c", -1, null)); // back to zero -> entry removed
        await Assert.That(_store.GetCounter("c")).IsEqualTo(0);

        Apply(T0, new IncrementCounterOp("c", 1, null));   // re-created from nothing
        await Assert.That(_store.GetCounter("c")).IsEqualTo(1);
    }

    [Test]
    public async Task RemoveServer_RemovesOnlyThatServer()
    {
        Apply(T0, new AnnounceServerOp("a", 1, []), new AnnounceServerOp("b", 1, []));
        Apply(T0, new RemoveServerOp("a"));
        var single = await Assert.That(_store.GetServers()).HasSingleItem();
        await Assert.That(single.Id).IsEqualTo("b");
    }

    [Test]
    public async Task RemoveTimedOutServers_RemovesOnlyStaleServers()
    {
        Apply(T0, new AnnounceServerOp("stale", 1, []));
        Apply(T0.AddMinutes(10), new AnnounceServerOp("fresh", 1, []));

        var removed = (int)Apply(T0.AddMinutes(11), new RemoveTimedOutServersOp(TimeSpan.FromMinutes(5)))!;
        await Assert.That(removed).IsEqualTo(1);
        var single = await Assert.That(_store.GetServers()).HasSingleItem();
        await Assert.That(single.Id).IsEqualTo("fresh");
    }

    // ----- snapshot edge cases -----

    [Test]
    public async Task LoadSnapshot_Throws_OnUnknownVersion()
    {
        var snapshot = Serialize(_store);
        snapshot[0] = 0xFF; // corrupt the version byte
        var other = new RaftStore();
        using var reader = new BinaryReader(new MemoryStream(snapshot), Encoding.UTF8);
        await Assert.That(() => other.LoadSnapshot(reader)).ThrowsExactly<NotSupportedException>();
    }

    [Test]
    public async Task Snapshot_RoundtripsAnEmptyStore()
    {
        var snapshot = Serialize(_store); // brand-new, every table empty
        var other = new RaftStore();
        using (var reader = new BinaryReader(new MemoryStream(snapshot), Encoding.UTF8)) other.LoadSnapshot(reader);
        await Assert.That(Serialize(other)).IsEquivalentTo(snapshot, CollectionOrdering.Matching); // every zero-count table prefix round-trips
    }

    [Test]
    public async Task LoadSnapshot_Throws_OnTruncatedSnapshot()
    {
        Apply(T0, NewJob("a"), new EnqueueOp("q", "a"), new AddToSetOp("s", "v", 1));
        var snapshot = Serialize(_store);
        var truncated = snapshot[..^5]; // drop the tail mid-structure

        var other = new RaftStore();
        using var reader = new BinaryReader(new MemoryStream(truncated), Encoding.UTF8);
        // A corrupt/partial snapshot must fail loudly (EndOfStream/InvalidData), not load partial state.
        await Assert.That(() => other.LoadSnapshot(reader)).Throws<Exception>();
    }

    [Test]
    public async Task LoadSnapshot_LeavesExistingStateIntact_WhenIncomingSnapshotIsCorrupt()
    {
        // A failed load is atomic. Populate the store, then feed it a truncated snapshot taken
        // from a different state. The incoming bytes are parsed into a throwaway store and only swapped
        // in on success, so after the load throws the live store must be byte-identical to before, never
        // half-wiped nor partially overwritten with the incoming data.
        Apply(T0,
            NewJob("keep"), new SetJobStateOp("keep", State("Succeeded", T0)),
            new EnqueueOp("default", "keep"),
            new AddToSetOp("s", "v", 1),
            new IncrementCounterOp("c", 3, null));
        var baseline = Serialize(_store);

        var incoming = new RaftStore();
        incoming.Apply(new Command
        {
            Id = Guid.NewGuid(),
            NowUtc = T0,
            Ops = [NewJob("other"), new EnqueueOp("q2", "other"), new AddToSetOp("s2", "w", 2)],
        });
        var corrupt = Serialize(incoming)[..^5]; // drop the tail mid-structure so the load fails partway

        using (var reader = new BinaryReader(new MemoryStream(corrupt), Encoding.UTF8))
            await Assert.That(() => _store.LoadSnapshot(reader)).Throws<Exception>();

        await Assert.That(Serialize(_store)).IsEquivalentTo(baseline, CollectionOrdering.Matching); // the live store survived the failed load intact
    }

    [Test]
    public async Task LoadSnapshot_Throws_OnOversizedCount()
    {
        using var ms = new MemoryStream();
        using (var w = new BinaryWriter(ms, Encoding.UTF8, leaveOpen: true))
        {
            w.Write((byte)1);                    // valid snapshot version (matches RaftStore.SnapshotVersion)
            w.Write7BitEncodedInt(int.MaxValue); // a jobs count far larger than the buffer
        }
        ms.Position = 0;

        using var reader = new BinaryReader(ms, Encoding.UTF8);
        // ReadCount must reject the hostile count before attempting a huge allocation.
        await Assert.That(() => new RaftStore().LoadSnapshot(reader)).ThrowsExactly<InvalidDataException>();
    }

    [Test]
    public async Task Snapshot_RoundtripsMultiEntryHistory_DistinctFromCurrentState()
    {
        // History holds three entries while CurrentState is the middle one (an AddJobState appends to
        // history without changing the current state), so the snapshot must serialize the two
        // independently rather than conflating "last history entry" with "current state".
        Apply(T0, NewJob("a"));
        Apply(T0, new SetJobStateOp("a", State("Enqueued", T0)));
        Apply(T0, new SetJobStateOp("a", State("Processing", T0.AddSeconds(1))));
        Apply(T0, new AddJobStateOp("a", State("Custom", T0.AddSeconds(2)))); // history-only

        var restored = new RaftStore();
        using (var reader = new BinaryReader(new MemoryStream(Serialize(_store)), Encoding.UTF8))
            restored.LoadSnapshot(reader);

        var job = restored.GetJob("a")!;
        await Assert.That(job.History.Select(s => s.Name)).IsEquivalentTo(["Enqueued", "Processing", "Custom"], CollectionOrdering.Matching);
        await Assert.That(job.CurrentState!.Name).IsEqualTo("Processing");       // current is the middle entry, not the last
        await Assert.That(restored.GetStateCount("Processing")).IsEqualTo(1);    // index rebuilt from CurrentState
        await Assert.That(restored.GetStateCount("Custom")).IsEqualTo(0);        // history-only entry is not indexed
    }

    [Test]
    public async Task Apply_SameCommandSequence_YieldsByteIdenticalState_OnIndependentStores()
    {
        // Determinism: applying the identical committed sequence to two independent stores must produce
        // byte-identical state. A wall-clock read, Guid.NewGuid, or any ambient input sneaking into the
        // apply path would make the two diverge and fail this.
        var token1 = Guid.NewGuid();
        var token2 = Guid.NewGuid();
        var owner = Guid.NewGuid();
        Command Cmd(DateTime now, params StoreOp[] ops) => new() { Id = Guid.NewGuid(), NowUtc = now, Ops = ops };
        Command[] sequence =
        [
            Cmd(T0, new CreateJobOp("j1", "p1", [new("k", "v")], T0, T0.AddDays(1)), new CreateJobOp("j2", "p2", [], T0, T0.AddDays(1))),
            Cmd(T0, new SetJobStateOp("j1", State("Enqueued", T0)), new EnqueueOp("default", "j1"), new EnqueueOp("default", "j2")),
            Cmd(T0.AddSeconds(1), new FetchOp(["default"], token1)),
            Cmd(T0.AddSeconds(2), new FetchOp(["default"], token2)),
            Cmd(T0.AddSeconds(3), new RenewFetchedOp(token1)),
            Cmd(T0, new AddToSetOp("s", "b", 2), new AddToSetOp("s", "a", 1), new AddToSetOp("s", "c", 2)),
            Cmd(T0, new InsertToListOp("l", "1"), new InsertToListOp("l", "2"), new TrimListOp("l", 0, 5)),
            Cmd(T0, new SetRangeInHashOp("h", [new("f1", "v1"), new("f2", null)])),
            Cmd(T0, new IncrementCounterOp("c", 3, T0.AddHours(1)), new IncrementCounterOp("c", -1, null)),
            Cmd(T0, new AnnounceServerOp("srv", 4, ["default"]), new TryAcquireLockOp("r", owner, TimeSpan.FromMinutes(2))),
            Cmd(T0.AddSeconds(4), new RequeueFetchedOp(token2)),
            Cmd(T0.AddMinutes(10), new MaintenanceOp(TimeSpan.FromMinutes(5))),
        ];

        var a = new RaftStore();
        var b = new RaftStore();
        foreach (var cmd in sequence)
        {
            a.Apply(cmd);
            b.Apply(cmd);
        }

        await Assert.That(Serialize(b)).IsEquivalentTo(Serialize(a), CollectionOrdering.Matching);
    }

    [Test]
    public async Task TryAcquireLock_AfterLosingToAnotherOwner_RenewReturnsFalse()
    {
        // The behavior RaftDistributedLock.RenewAsync relies on: once a lease has expired and another
        // owner took it, the original owner's renewal must be denied (it must not steal the lock back).
        var owner1 = Guid.NewGuid();
        var owner2 = Guid.NewGuid();
        var lease = TimeSpan.FromMinutes(2);

        await Assert.That((bool)Apply(T0, new TryAcquireLockOp("r", owner1, lease))!).IsTrue();
        await Assert.That((bool)Apply(T0.AddMinutes(3), new TryAcquireLockOp("r", owner2, lease))!).IsTrue(); // owner1 expired
        await Assert.That((bool)Apply(T0.AddMinutes(3), new TryAcquireLockOp("r", owner1, lease))!).IsFalse(); // owner2 holds a live lease
    }

    [Test]
    public async Task TrimList_BoundaryCases()
    {
        // Each list's newest-first view is 3,2,1.
        Apply(T0, new InsertToListOp("neg", "1"), new InsertToListOp("neg", "2"), new InsertToListOp("neg", "3"));
        Apply(T0, new TrimListOp("neg", -2, 1)); // negative start clamps to 0 -> keep indices 0..1
        await Assert.That(_store.GetAllItemsFromList("neg")).IsEquivalentTo(["3", "2"], CollectionOrdering.Matching);

        Apply(T0, new InsertToListOp("over", "1"), new InsertToListOp("over", "2"), new InsertToListOp("over", "3"));
        Apply(T0, new TrimListOp("over", 1, 99)); // upper bound past the end -> keep 2,1
        await Assert.That(_store.GetAllItemsFromList("over")).IsEquivalentTo(["2", "1"], CollectionOrdering.Matching);

        Apply(T0, new InsertToListOp("one", "1"), new InsertToListOp("one", "2"), new InsertToListOp("one", "3"));
        Apply(T0, new TrimListOp("one", 2, 2)); // keep exactly index 2
        await Assert.That(_store.GetAllItemsFromList("one")).IsEquivalentTo(["1"], CollectionOrdering.Matching);
    }

    [Test]
    public async Task GetSetCount_WithLimit_NeverExceedsLimit_AndHandlesEdges()
    {
        Apply(T0, new AddRangeToSetOp("s1", ["a", "b", "c"]), new AddRangeToSetOp("s2", ["d", "e"])); // 5 total

        await Assert.That(_store.GetSetCount([], 100)).IsEqualTo(0);            // no keys
        await Assert.That(_store.GetSetCount(["s1", "s2"], 0)).IsEqualTo(0);    // limit 0
        await Assert.That(_store.GetSetCount(["s1", "s2"], 4)).IsEqualTo(4);    // capped below the actual 5
        await Assert.That(_store.GetSetCount(["s1", "s2"], 100)).IsEqualTo(5);  // actual, under the limit
        await Assert.That(_store.GetSetCount(["s1", "s2"], 4) <= 4).IsTrue();   // never exceeds the limit
    }

    [Test]
    public async Task GetRangeFromSet_EmptyWhenFromExceedsTo_OrKeyMissing()
    {
        Apply(T0, new AddToSetOp("s", "a", 1), new AddToSetOp("s", "b", 2));
        await Assert.That(_store.GetRangeFromSet("s", 5, 1)).IsEmpty();        // from > to
        await Assert.That(_store.GetRangeFromSet("missing", 0, 10)).IsEmpty(); // unknown key
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
