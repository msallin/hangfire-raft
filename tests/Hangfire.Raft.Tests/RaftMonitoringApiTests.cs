using Hangfire.Common;
using Hangfire.Raft.Commands;
using Hangfire.Raft.Monitoring;
using Hangfire.Raft.State;
using Hangfire.States;
using Hangfire.Storage;

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

    [Fact]
    public void GetStatistics_AggregatesEveryCount()
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
        Assert.Equal(1, stats.Enqueued);
        Assert.Equal(1, stats.Processing);
        Assert.Equal(1, stats.Failed);
        Assert.Equal(9, stats.Succeeded);
        Assert.Equal(2, stats.Deleted);
        Assert.Equal(1, stats.Recurring);
        Assert.Equal(1, stats.Retries);
        Assert.Equal(1, stats.Servers);
        Assert.Equal(1, stats.Queues);
    }

    // ----- queues / servers -----

    [Fact]
    public void Queues_ReportsLengthAndFetchedCount()
    {
        CreateJob("a");
        CreateJob("b");
        Apply(new EnqueueOp("default", "a"), new EnqueueOp("default", "b"));
        Apply(new FetchOp(["default"], Guid.NewGuid())); // fetches "a" -> 1 enqueued left, 1 fetched

        var queue = Assert.Single(_api.Queues());
        Assert.Equal("default", queue.Name);
        Assert.Equal(1, queue.Length);
        Assert.Equal(1, queue.Fetched);
        Assert.Single(queue.FirstJobs); // "b" still enqueued
    }

    [Fact]
    public void Servers_MapsContextAndHeartbeat()
    {
        Apply(new AnnounceServerOp("srv-1", 8, ["default", "critical"]));
        Apply(T0.AddMinutes(1), new HeartbeatOp("srv-1"));

        var server = Assert.Single(_api.Servers());
        Assert.Equal("srv-1", server.Name);
        Assert.Equal(8, server.WorkersCount);
        Assert.Equal(["default", "critical"], server.Queues);
        Assert.Equal(T0.AddMinutes(1), server.Heartbeat);
    }

    // ----- job details -----

    [Fact]
    public void JobDetails_MapsPropertiesAndReversesHistory()
    {
        CreateJob("j", "hello");
        Apply(new SetJobStateOp("j", State(EnqueuedState.StateName, T0)));
        Apply(new SetJobStateOp("j", State(ProcessingState.StateName, T0.AddSeconds(1))));

        var details = _api.JobDetails("j");
        Assert.NotNull(details);
        Assert.Equal(typeof(TestJobs), details.Job!.Type);
        Assert.Equal("hello", details.Job.Args[0]);
        Assert.Equal(T0.AddDays(1), details.ExpireAt);
        Assert.Equal("de-CH", details.Properties["Culture"]);
        // History is newest-first in the dashboard.
        Assert.Equal([ProcessingState.StateName, EnqueuedState.StateName], details.History.Select(h => h.StateName));
    }

    [Fact]
    public void JobDetails_ReturnsNull_ForMissingJob() => Assert.Null(_api.JobDetails("nope"));

    [Fact]
    public void JobDetails_ThrowsForEmptyId() => Assert.Throws<ArgumentException>(() => _api.JobDetails(""));

    // ----- enqueued page -----

    [Fact]
    public void EnqueuedJobs_PagesAndSkipsEvictedJobs()
    {
        CreateJob("a");
        CreateJob("b");
        Apply(new SetJobStateOp("a", State(EnqueuedState.StateName, T0, ("EnqueuedAt", JobHelper.SerializeDateTime(T0)))));
        Apply(new SetJobStateOp("b", State(EnqueuedState.StateName, T0)));
        // "ghost" is enqueued but never created -> the mapper must skip it.
        Apply(new EnqueueOp("default", "a"), new EnqueueOp("default", "ghost"), new EnqueueOp("default", "b"));

        var page = _api.EnqueuedJobs("default", 0, 10);
        Assert.Equal(["a", "b"], page.Select(kv => kv.Key));
        Assert.Equal(T0, page.First(kv => kv.Key == "a").Value.EnqueuedAt);
        Assert.True(page.First(kv => kv.Key == "a").Value.InEnqueuedState);
    }

    // ----- scheduled page (ordered by schedule-set score, not the state index) -----

    [Fact]
    public void ScheduledJobs_OrdersByScheduleSetScore()
    {
        foreach (var (id, score) in new[] { ("late", 300d), ("soon", 100d), ("mid", 200d) })
        {
            CreateJob(id);
            Apply(new SetJobStateOp(id, State(ScheduledState.StateName, T0, ("EnqueueAt", JobHelper.SerializeDateTime(T0.AddSeconds(score))))));
            Apply(new AddToSetOp("schedule", id, score));
        }

        var page = _api.ScheduledJobs(0, 10);
        Assert.Equal(["soon", "mid", "late"], page.Select(kv => kv.Key)); // by score, not insertion/state time
        Assert.Equal(3, _api.ScheduledCount());
    }

    // ----- succeeded page (duration math + timestamp fallback) -----

    [Fact]
    public void SucceededJobs_ComputesTotalDuration_AndFallsBackForTimestamp()
    {
        CreateJob("s");
        Apply(new SetJobStateOp("s", State(SucceededState.StateName, T0,
            ("Result", "42"),
            ("PerformanceDuration", "10"),
            ("Latency", "5"))));
        // no "SucceededAt" key -> SucceededAt falls back to the current state's CreatedAt (T0).

        var dto = Assert.Single(_api.SucceededJobs(0, 10)).Value;
        Assert.Equal("42", dto.Result);
        Assert.Equal(15, dto.TotalDuration); // PerformanceDuration + Latency
        Assert.Equal(T0, dto.SucceededAt);
        Assert.Equal(1, _api.SucceededListCount());
    }

    // ----- fetched page -----

    [Fact]
    public void FetchedJobs_MapsFetchTime()
    {
        CreateJob("a");
        Apply(new EnqueueOp("q", "a"));
        Apply(T0.AddSeconds(5), new FetchOp(["q"], Guid.NewGuid()));

        var dto = Assert.Single(_api.FetchedJobs("q", 0, 10));
        Assert.Equal("a", dto.Key);
        Assert.Equal(T0.AddSeconds(5), dto.Value.FetchedAt);
        Assert.Equal(1, _api.FetchedCount("q"));
    }

    // ----- awaiting page (continuation parent-state scraping) -----

    [Fact]
    public void AwaitingJobs_ExtractsParentStateName()
    {
        CreateJob("w");
        Apply(new SetJobStateOp("w", State(AwaitingState.StateName, T0,
            ("NextState", "{\"$type\":\"...\",\"Name\":\"Enqueued\"}"))));

        var dto = Assert.Single(_api.AwaitingJobs(0, 10)).Value;
        Assert.Equal("Enqueued", dto.ParentStateName);
        Assert.Equal(1, _api.AwaitingCount());
    }

    [Theory]
    [InlineData("{\"$type\":\"x\",\"Name\":\"Enqueued\"}", "Enqueued")]
    [InlineData("{\"name\":\"Scheduled\"}", "Scheduled")] // marker match is case-insensitive
    [InlineData("no marker", null)]
    [InlineData("{\"Name\":\"unterminated", null)]
    [InlineData("", null)]
    public void ParseStateName_Cases(string json, string? expected)
        => Assert.Equal(expected, RaftMonitoringApi.ParseStateName(json));

    // ----- date-bucket graphs -----

    [Fact]
    public void SucceededByDatesCount_ReadsSevenDailyBuckets()
    {
        var today = DateTime.UtcNow.Date;
        Apply(new IncrementCounterOp($"stats:succeeded:{today:yyyy-MM-dd}", 7, null));

        var byDate = _api.SucceededByDatesCount();
        Assert.Equal(7, byDate.Count); // one bucket per day for a week
        Assert.Equal(7, byDate[today]);
        Assert.Equal(0, byDate[today.AddDays(-3)]);
    }

    [Fact]
    public void HourlyFailedJobs_ReadsTwentyFourHourlyBuckets()
    {
        var now = DateTime.UtcNow;
        var hour = new DateTime(now.Year, now.Month, now.Day, now.Hour, 0, 0, DateTimeKind.Utc);
        Apply(new IncrementCounterOp($"stats:failed:{hour:yyyy-MM-dd-HH}", 3, null));

        var byHour = _api.HourlyFailedJobs();
        Assert.Equal(24, byHour.Count);
        Assert.Equal(3, byHour[hour]);
    }

    // ----- counts -----

    [Fact]
    public void StateCounts_ReflectTheStateIndex()
    {
        CreateJob("p1");
        Apply(new SetJobStateOp("p1", State(ProcessingState.StateName, T0)));
        CreateJob("d1");
        Apply(new SetJobStateOp("d1", State(DeletedState.StateName, T0)));

        Assert.Equal(1, _api.ProcessingCount());
        Assert.Equal(1, _api.DeletedListCount());
        Assert.Equal(0, _api.FailedCount());
    }
}
