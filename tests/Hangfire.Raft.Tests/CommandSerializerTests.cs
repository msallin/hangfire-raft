using Hangfire.Raft.Commands;
using TUnit.Assertions.Enums;

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

    [Test]
    public async Task Roundtrip_PreservesEveryOp()
    {
        var command = BuildCommandWithEveryOp();

        var bytes = CommandSerializer.Serialize(command);
        var restored = CommandSerializer.TryDeserialize(bytes);

        await Assert.That(restored).IsNotNull();
        await Assert.That(restored.Id).IsEqualTo(command.Id);
        await Assert.That(restored.NowUtc).IsEqualTo(command.NowUtc);
        await Assert.That(restored.Ops.Count).IsEqualTo(command.Ops.Count);
        await Assert.That(restored.Ops.Select(o => o.GetType())).IsEquivalentTo(command.Ops.Select(o => o.GetType()), CollectionOrdering.Matching);

        // Re-serializing the restored command must produce identical bytes; this catches any field
        // that is written but read differently (or not at all).
        var bytes2 = CommandSerializer.Serialize(restored);
        await Assert.That(bytes2).IsEquivalentTo(bytes, CollectionOrdering.Matching);
    }

    [Test]
    public async Task Roundtrip_PreservesValues()
    {
        var command = BuildCommandWithEveryOp();
        var restored = CommandSerializer.TryDeserialize(CommandSerializer.Serialize(command))!;

        await Assert.That(restored.Ops[0]).IsTypeOf<CreateJobOp>();
        var create = (CreateJobOp)restored.Ops[0];
        await Assert.That(create.JobId).IsEqualTo("job-1");
        await Assert.That(create.ExpireAt).IsEqualTo(Now.AddDays(1));
        await Assert.That(create.Parameters).IsEquivalentTo(new KeyValuePair<string, string?>[] { new("Key", "Value"), new("NullKey", null) }, CollectionOrdering.Matching);

        await Assert.That(restored.Ops[3]).IsTypeOf<SetJobStateOp>();
        var setState = (SetJobStateOp)restored.Ops[3];
        await Assert.That(setState.State.Name).IsEqualTo("Enqueued");
        await Assert.That(setState.State.Reason).IsEqualTo("reason");
        await Assert.That(setState.State.Data).IsEquivalentTo(new KeyValuePair<string, string?>[] { new("Queue", "default") }, CollectionOrdering.Matching);

        await Assert.That(restored.Ops[8]).IsTypeOf<FetchOp>();
        var fetch = (FetchOp)restored.Ops[8];
        await Assert.That(fetch.Queues).IsEquivalentTo(["critical", "default"], CollectionOrdering.Matching);

        await Assert.That(restored.Ops[13]).IsTypeOf<IncrementCounterOp>();
        var counter = (IncrementCounterOp)restored.Ops[13];
        await Assert.That(counter.Delta).IsEqualTo(-5);
        await Assert.That(counter.ExpireAt).IsNull();

        await Assert.That(restored.Ops[14]).IsTypeOf<AddToSetOp>();
        var addToSet = (AddToSetOp)restored.Ops[14];
        await Assert.That(addToSet.Score).IsEqualTo(1718100000.5);
    }

    [Test]
    public async Task EveryOpType_IsCoveredByRoundtripCommand()
    {
        var allOpTypes = typeof(StoreOp).Assembly.GetTypes()
            .Where(t => !t.IsAbstract && t.IsAssignableTo(typeof(StoreOp)))
            .ToHashSet();
        var covered = BuildCommandWithEveryOp().Ops.Select(o => o.GetType()).ToHashSet();

        var missing = allOpTypes.Except(covered).Select(t => t.Name).ToList();
        await Assert.That(missing).IsEmpty(); // failure lists the ops missing from the roundtrip command
    }

    [Test]
    public async Task TryDeserialize_ReturnsNull_ForForeignPayloads()
    {
        await Assert.That(CommandSerializer.TryDeserialize(ReadOnlyMemory<byte>.Empty)).IsNull();
        await Assert.That(CommandSerializer.TryDeserialize(new byte[] { 0x00, 0x01, 0x02 })).IsNull();
    }

    [Test]
    public async Task Batch_SnapshotsOps_SoLaterMutationOfTheSourceListCannotChangeTheCommand()
    {
        var source = new List<StoreOp> { new PersistJobOp("a"), new PersistJobOp("b") };
        var command = Command.Batch(source);

        source.Add(new PersistJobOp("c")); // mutate the caller's list after creating the command
        source[0] = new PersistJobOp("mutated");

        await Assert.That(command.Ops.Count).IsEqualTo(2); // the envelope kept its own copy
        await Assert.That(command.Ops[0]).IsTypeOf<PersistJobOp>();
        await Assert.That(((PersistJobOp)command.Ops[0]).JobId).IsEqualTo("a");
    }
}
