using Hangfire.Raft.Commands;

namespace Hangfire.Raft.Tests;

/// <summary>
/// Roundtrip coverage for the wire format. The exhaustiveness test fails when a new op type is
/// added without extending the roundtrip command, keeping serializer and op set in sync.
/// </summary>
public class CommandSerializerTests
{
    private static readonly DateTime Now = new(2026, 6, 11, 12, 0, 0, DateTimeKind.Utc);

    private static Command BuildCommandWithEveryOp() => new()
    {
        Id = Guid.Parse("11111111-2222-3333-4444-555555555555"),
        NowUtc = Now,
        Ops =
        [
            new CreateJobOp("job-1", "{\"t\":\"x\"}", [new("Key", "Value"), new("NullKey", null)], Now, Now.AddDays(1)),
            new SetJobParameterOp("job-1", "RetryCount", "3"),
            new SetJobParameterOp("job-1", "Nulled", null),
            new SetJobStateOp("job-1", new StateRecord("Enqueued", "reason", [new("Queue", "default")], Now)),
            new AddJobStateOp("job-1", new StateRecord("Custom", null, [], Now)),
            new ExpireJobOp("job-1", Now.AddHours(2)),
            new PersistJobOp("job-1"),
            new EnqueueOp("default", "job-1"),
            new FetchOp(["critical", "default"], Guid.NewGuid()),
            new AckFetchedOp(Guid.NewGuid()),
            new RequeueFetchedOp(Guid.NewGuid()),
            new RenewFetchedOp(Guid.NewGuid()),
            new IncrementCounterOp("stats:succeeded", 1, Now.AddDays(30)),
            new IncrementCounterOp("stats:raw", -5, null),
            new AddToSetOp("schedule", "job-1", 1718100000.5),
            new AddRangeToSetOp("batch", ["a", "b", "c"]),
            new RemoveFromSetOp("schedule", "job-1"),
            new RemoveSetOp("schedule"),
            new ExpireSetOp("schedule", Now.AddMinutes(5)),
            new PersistSetOp("schedule"),
            new InsertToListOp("console", "line"),
            new RemoveFromListOp("console", "line"),
            new TrimListOp("console", 0, 99),
            new ExpireListOp("console", Now.AddMinutes(5)),
            new PersistListOp("console"),
            new SetRangeInHashOp("recurring-job:x", [new("Cron", "* * * * *"), new("Empty", null)]),
            new RemoveHashOp("recurring-job:x"),
            new ExpireHashOp("recurring-job:x", Now.AddMinutes(5)),
            new PersistHashOp("recurring-job:x"),
            new AnnounceServerOp("server-1", 20, ["default", "critical"]),
            new RemoveServerOp("server-1"),
            new HeartbeatOp("server-1"),
            new RemoveTimedOutServersOp(TimeSpan.FromMinutes(5)),
            new TryAcquireLockOp("locks:recurring", Guid.NewGuid(), TimeSpan.FromMinutes(2)),
            new ReleaseLockOp("locks:recurring", Guid.NewGuid()),
            new MaintenanceOp(TimeSpan.FromMinutes(5)),
        ],
    };

    [Fact]
    public void Roundtrip_PreservesEveryOp()
    {
        var command = BuildCommandWithEveryOp();

        var bytes = CommandSerializer.Serialize(command);
        var restored = CommandSerializer.TryDeserialize(bytes);

        Assert.NotNull(restored);
        Assert.Equal(command.Id, restored.Id);
        Assert.Equal(command.NowUtc, restored.NowUtc);
        Assert.Equal(command.Ops.Count, restored.Ops.Count);
        Assert.Equal(command.Ops.Select(o => o.GetType()), restored.Ops.Select(o => o.GetType()));

        // Re-serializing the restored command must produce identical bytes; this catches any field
        // that is written but read differently (or not at all).
        var bytes2 = CommandSerializer.Serialize(restored);
        Assert.Equal(bytes, bytes2);
    }

    [Fact]
    public void Roundtrip_PreservesValues()
    {
        var command = BuildCommandWithEveryOp();
        var restored = CommandSerializer.TryDeserialize(CommandSerializer.Serialize(command))!;

        var create = Assert.IsType<CreateJobOp>(restored.Ops[0]);
        Assert.Equal("job-1", create.JobId);
        Assert.Equal(Now.AddDays(1), create.ExpireAt);
        Assert.Equal([new("Key", "Value"), new("NullKey", null)], create.Parameters);

        var setState = Assert.IsType<SetJobStateOp>(restored.Ops[3]);
        Assert.Equal("Enqueued", setState.State.Name);
        Assert.Equal("reason", setState.State.Reason);
        Assert.Equal([new("Queue", "default")], setState.State.Data);

        var fetch = Assert.IsType<FetchOp>(restored.Ops[8]);
        Assert.Equal(["critical", "default"], fetch.Queues);

        var counter = Assert.IsType<IncrementCounterOp>(restored.Ops[13]);
        Assert.Equal(-5, counter.Delta);
        Assert.Null(counter.ExpireAt);

        var addToSet = Assert.IsType<AddToSetOp>(restored.Ops[14]);
        Assert.Equal(1718100000.5, addToSet.Score);
    }

    [Fact]
    public void EveryOpType_IsCoveredByRoundtripCommand()
    {
        var allOpTypes = typeof(StoreOp).Assembly.GetTypes()
            .Where(t => !t.IsAbstract && t.IsAssignableTo(typeof(StoreOp)))
            .ToHashSet();
        var covered = BuildCommandWithEveryOp().Ops.Select(o => o.GetType()).ToHashSet();

        var missing = allOpTypes.Except(covered).Select(t => t.Name).ToList();
        Assert.True(missing.Count == 0, $"Ops missing from the roundtrip test: {string.Join(", ", missing)}");
    }

    [Fact]
    public void TryDeserialize_ReturnsNull_ForForeignPayloads()
    {
        Assert.Null(CommandSerializer.TryDeserialize(ReadOnlyMemory<byte>.Empty));
        Assert.Null(CommandSerializer.TryDeserialize(new byte[] { 0x00, 0x01, 0x02 }));
    }
}
