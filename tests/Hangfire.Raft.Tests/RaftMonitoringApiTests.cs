using Hangfire.Common;
using Hangfire.Raft.Commands;
using Hangfire.Raft.Monitoring;
using Hangfire.Raft.State;
using Hangfire.States;
using Hangfire.Storage;
using TUnit.Assertions.Enums;

namespace Hangfire.Raft.Tests;

/// <summary>
/// Unit tests of the dashboard monitoring API. It is a pure mapper over <see cref="RaftStore"/>, so
/// the tests seed a store by applying commands and read the DTOs back without a live cluster.
/// </summary>
public class RaftMonitoringApiTests
{
    private static readonly DateTime T0 = new(2026, 6, 12, 9, 0, 0, DateTimeKind.Utc);

    private readonly RaftStore _store = new();
    private readonly RaftMonitoringApi _api;

    public RaftMonitoringApiTests() => _api = new RaftMonitoringApi(_store);

    private void Apply(params StoreOp[] ops) => Apply(T0, ops);

    private void Apply(DateTime now, params StoreOp[] ops)
        => _store.Apply(new Command { Id = Guid.NewGuid(), NowUtc = now, Ops = ops });

    private static string Payload(string arg)
        => InvocationData.SerializeJob(Job.FromExpression(() => TestJobs.Run(arg))).SerializePayload();

    private string CreateJob(string id, string arg = "x")
    {
        Apply(new CreateJobOp(id, Payload(arg), [new("Culture", "de-CH")], T0, T0.AddDays(1)));
        return id;
    }

    private static StateRecord State(string name, DateTime at, params (string Key, string? Value)[] data)
        => new(name, null, data.Select(d => new KeyValuePair<string, string?>(d.Key, d.Value)).ToList(), at);

    // ----- statistics -----

    [Test]
    public async Task GetStatistics_AggregatesEveryCount()
    {
        CreateJob("e");
        Apply(new EnqueueOp("default", "e"));
        CreateJob("p");
        Apply(new SetJobStateOp("p", State(ProcessingState.StateName, T0)));
        CreateJob("f");
        Apply(new SetJobStateOp("f", State(FailedState.StateName, T0)));
        Apply(
            new IncrementCounterOp("stats:succeeded", 9, null),
            new IncrementCounterOp("stats:deleted", 2, null),
            new AddToSetOp("recurring-jobs", "rj", 0),
            new AddToSetOp("retries", "r", 0),
            new AnnounceServerOp("srv", 4, ["default"]));

        var stats = _api.GetStatistics();
        await Assert.That(stats.Enqueued).IsEqualTo(1);
        await Assert.That(stats.Processing).IsEqualTo(1);
        await Assert.That(stats.Failed).IsEqualTo(1);
        await Assert.That(stats.Succeeded).IsEqualTo(9);
        await Assert.That(stats.Deleted).IsEqualTo(2);
        await Assert.That(stats.Recurring).IsEqualTo(1);
        await Assert.That(stats.Retries).IsEqualTo(1);
        await Assert.That(stats.Servers).IsEqualTo(1);
        await Assert.That(stats.Queues).IsEqualTo(1);
    }

    // ----- queues / servers -----

    [Test]
    public async Task Queues_ReportsLengthAndFetchedCount()
    {
        CreateJob("a");
        CreateJob("b");
        Apply(new EnqueueOp("default", "a"), new EnqueueOp("default", "b"));
        Apply(new FetchOp(["default"], Guid.NewGuid())); // fetches "a" -> 1 enqueued left, 1 fetched

        var queue = await Assert.That(_api.Queues()).HasSingleItem();
        await Assert.That(queue.Name).IsEqualTo("default");
        await Assert.That(queue.Length).IsEqualTo(1);
        await Assert.That(queue.Fetched).IsEqualTo(1);
        await Assert.That(queue.FirstJobs).HasSingleItem(); // "b" still enqueued
    }

    [Test]
    public async Task Servers_MapsContextAndHeartbeat()
    {
        Apply(new AnnounceServerOp("srv-1", 8, ["default", "critical"]));
        Apply(T0.AddMinutes(1), new HeartbeatOp("srv-1"));

        var server = await Assert.That(_api.Servers()).HasSingleItem();
        await Assert.That(server.Name).IsEqualTo("srv-1");
        await Assert.That(server.WorkersCount).IsEqualTo(8);
        await Assert.That(server.Queues).IsEquivalentTo(["default", "critical"], CollectionOrdering.Matching);
        await Assert.That(server.Heartbeat).IsEqualTo(T0.AddMinutes(1));
    }

    // ----- job details -----

    [Test]
    public async Task JobDetails_MapsPropertiesAndReversesHistory()
    {
        CreateJob("j", "hello");
        Apply(new SetJobStateOp("j", State(EnqueuedState.StateName, T0)));
        Apply(new SetJobStateOp("j", State(ProcessingState.StateName, T0.AddSeconds(1))));

        var details = _api.JobDetails("j");
        await Assert.That(details).IsNotNull();
        await Assert.That(details.Job!.Type).IsEqualTo(typeof(TestJobs));
        await Assert.That(details.Job.Args[0]).IsEqualTo("hello");
        await Assert.That(details.ExpireAt).IsEqualTo(T0.AddDays(1));
        await Assert.That(details.Properties["Culture"]).IsEqualTo("de-CH");
        // History is newest-first in the dashboard.
        await Assert.That(details.History.Select(h => h.StateName)).IsEquivalentTo([ProcessingState.StateName, EnqueuedState.StateName], CollectionOrdering.Matching);
    }

    [Test]
    public async Task JobDetails_ReturnsNull_ForMissingJob() => await Assert.That(_api.JobDetails("nope")).IsNull();

    [Test]
    public async Task JobDetails_ThrowsForEmptyId() => await Assert.That(() => _api.JobDetails("")).ThrowsExactly<ArgumentException>();

    // ----- enqueued page -----

    [Test]
    public async Task EnqueuedJobs_PagesAndSkipsEvictedJobs()
    {
        CreateJob("a");
        CreateJob("b");
        Apply(new SetJobStateOp("a", State(EnqueuedState.StateName, T0, ("EnqueuedAt", JobHelper.SerializeDateTime(T0)))));
        Apply(new SetJobStateOp("b", State(EnqueuedState.StateName, T0)));
        // "ghost" is enqueued but never created -> the mapper must skip it.
        Apply(new EnqueueOp("default", "a"), new EnqueueOp("default", "ghost"), new EnqueueOp("default", "b"));

        var page = _api.EnqueuedJobs("default", 0, 10);
        await Assert.That(page.Select(kv => kv.Key)).IsEquivalentTo(["a", "b"], CollectionOrdering.Matching);
        await Assert.That(page.First(kv => kv.Key == "a").Value.EnqueuedAt).IsEqualTo(T0);
        await Assert.That(page.First(kv => kv.Key == "a").Value.InEnqueuedState).IsTrue();
    }

    // ----- scheduled page (ordered by schedule-set score, not the state index) -----

    [Test]
    public async Task ScheduledJobs_OrdersByScheduleSetScore()
    {
        foreach (var (id, score) in new[] { ("late", 300d), ("soon", 100d), ("mid", 200d) })
        {
            CreateJob(id);
            Apply(new SetJobStateOp(id, State(ScheduledState.StateName, T0, ("EnqueueAt", JobHelper.SerializeDateTime(T0.AddSeconds(score))))));
            Apply(new AddToSetOp("schedule", id, score));
        }

        var page = _api.ScheduledJobs(0, 10);
        await Assert.That(page.Select(kv => kv.Key)).IsEquivalentTo(["soon", "mid", "late"], CollectionOrdering.Matching); // by score, not insertion/state time
        await Assert.That(_api.ScheduledCount()).IsEqualTo(3);
    }

    // ----- succeeded page (duration math + timestamp fallback) -----

    [Test]
    public async Task SucceededJobs_ComputesTotalDuration_AndFallsBackForTimestamp()
    {
        CreateJob("s");
        Apply(new SetJobStateOp("s", State(SucceededState.StateName, T0,
            ("Result", "42"),
            ("PerformanceDuration", "10"),
            ("Latency", "5"))));
        // no "SucceededAt" key -> SucceededAt falls back to the current state's CreatedAt (T0).

        var dto = (await Assert.That(_api.SucceededJobs(0, 10)).HasSingleItem()).Value;
        await Assert.That(dto.Result).IsEqualTo("42");
        await Assert.That(dto.TotalDuration).IsEqualTo(15); // PerformanceDuration + Latency
        await Assert.That(dto.SucceededAt).IsEqualTo(T0);
        await Assert.That(_api.SucceededListCount()).IsEqualTo(1);
    }

    // ----- fetched page -----

    [Test]
    public async Task FetchedJobs_MapsFetchTime()
    {
        CreateJob("a");
        Apply(new EnqueueOp("q", "a"));
        Apply(T0.AddSeconds(5), new FetchOp(["q"], Guid.NewGuid()));

        var dto = await Assert.That(_api.FetchedJobs("q", 0, 10)).HasSingleItem();
        await Assert.That(dto.Key).IsEqualTo("a");
        await Assert.That(dto.Value.FetchedAt).IsEqualTo(T0.AddSeconds(5));
        await Assert.That(_api.FetchedCount("q")).IsEqualTo(1);
    }

    // ----- awaiting page (continuation parent-state scraping) -----

    [Test]
    public async Task AwaitingJobs_ExtractsParentStateName()
    {
        CreateJob("w");
        Apply(new SetJobStateOp("w", State(AwaitingState.StateName, T0,
            ("NextState", "{\"$type\":\"...\",\"Name\":\"Enqueued\"}"))));

        var dto = (await Assert.That(_api.AwaitingJobs(0, 10)).HasSingleItem()).Value;
        await Assert.That(dto.ParentStateName).IsEqualTo("Enqueued");
        await Assert.That(_api.AwaitingCount()).IsEqualTo(1);
    }

    [Test]
    [Arguments("{\"$type\":\"x\",\"Name\":\"Enqueued\"}", "Enqueued")]
    [Arguments("{\"name\":\"Scheduled\"}", "Scheduled")] // marker match is case-insensitive
    [Arguments("no marker", null)]
    [Arguments("{\"Name\":\"unterminated", null)]
    [Arguments("", null)]
    public async Task ParseStateName_Cases(string json, string? expected)
        => await Assert.That(RaftMonitoringApi.ParseStateName(json)).IsEqualTo(expected);

    // ----- date-bucket graphs -----

    [Test]
    public async Task DailyCounts_ReadsSevenDailyBuckets()
    {
        // A fixed instant so the seeded key and the read bucket share one timestamp; reading via the
        // clock-parameterized overload removes the midnight-boundary race the public method would have.
        var now = new DateTime(2026, 6, 12, 9, 0, 0, DateTimeKind.Utc);
        var today = now.Date;
        Apply(new IncrementCounterOp($"stats:succeeded:{today:yyyy-MM-dd}", 7, null));

        var byDate = _api.DailyCounts("succeeded", now);
        await Assert.That(byDate.Count).IsEqualTo(7); // one bucket per day for a week
        await Assert.That(byDate[today]).IsEqualTo(7);
        await Assert.That(byDate[today.AddDays(-3)]).IsEqualTo(0);
    }

    [Test]
    public async Task HourlyCounts_ReadsTwentyFourHourlyBuckets()
    {
        var now = new DateTime(2026, 6, 12, 9, 30, 0, DateTimeKind.Utc);
        var hour = new DateTime(now.Year, now.Month, now.Day, now.Hour, 0, 0, DateTimeKind.Utc);
        Apply(new IncrementCounterOp($"stats:failed:{hour:yyyy-MM-dd-HH}", 3, null));

        var byHour = _api.HourlyCounts("failed", now);
        await Assert.That(byHour.Count).IsEqualTo(24);
        await Assert.That(byHour[hour]).IsEqualTo(3);
    }

    // ----- counts -----

    [Test]
    public async Task StateCounts_ReflectTheStateIndex()
    {
        CreateJob("p1");
        Apply(new SetJobStateOp("p1", State(ProcessingState.StateName, T0)));
        CreateJob("d1");
        Apply(new SetJobStateOp("d1", State(DeletedState.StateName, T0)));

        await Assert.That(_api.ProcessingCount()).IsEqualTo(1);
        await Assert.That(_api.DeletedListCount()).IsEqualTo(1);
        await Assert.That(_api.FailedCount()).IsEqualTo(0);
    }
}
