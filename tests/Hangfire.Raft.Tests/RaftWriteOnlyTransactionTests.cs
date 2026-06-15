using Hangfire.Common;
using Hangfire.Raft.Commands;
using Hangfire.States;

namespace Hangfire.Raft.Tests;

/// <summary>
/// Unit tests of the Hangfire-call to <see cref="StoreOp"/> mapping. The transaction only touches the
/// cluster in Commit, so these build ops and inspect them via the PendingOps seam without a node.
/// </summary>
public class RaftWriteOnlyTransactionTests
{
    // storage is referenced only by Commit, which these tests never call.
    private readonly RaftWriteOnlyTransaction _tx = new(storage: null!);

    [Test]
    public async Task DecrementCounter_EmitsIncrementByMinusOne()
    {
        _tx.DecrementCounter("c");
        var single = await Assert.That(_tx.PendingOps).HasSingleItem();
        await Assert.That(single).IsTypeOf<IncrementCounterOp>();
        var op = (IncrementCounterOp)single;
        await Assert.That(op.Key).IsEqualTo("c");
        await Assert.That(op.Delta).IsEqualTo(-1);
        await Assert.That(op.ExpireAt).IsNull();
    }

    [Test]
    public async Task IncrementCounter_WithExpiry_SetsExpireAt()
    {
        _tx.IncrementCounter("c", TimeSpan.FromHours(1));
        var single = await Assert.That(_tx.PendingOps).HasSingleItem();
        await Assert.That(single).IsTypeOf<IncrementCounterOp>();
        var op = (IncrementCounterOp)single;
        await Assert.That(op.Delta).IsEqualTo(1);
        await Assert.That(op.ExpireAt).IsNotNull();
    }

    [Test]
    public async Task AddToSet_WithoutScore_DefaultsToZero()
    {
        _tx.AddToSet("s", "v");
        var single = await Assert.That(_tx.PendingOps).HasSingleItem();
        await Assert.That(single).IsTypeOf<AddToSetOp>();
        var op = (AddToSetOp)single;
        await Assert.That(op.Score).IsEqualTo(0.0d);
    }

    [Test]
    public async Task SetJobState_MapsNameReasonAndData()
    {
        _tx.SetJobState("j", new ScheduledState(TimeSpan.FromHours(2)));
        var single = await Assert.That(_tx.PendingOps).HasSingleItem();
        await Assert.That(single).IsTypeOf<SetJobStateOp>();
        var op = (SetJobStateOp)single;
        await Assert.That(op.JobId).IsEqualTo("j");
        await Assert.That(op.State.Name).IsEqualTo(ScheduledState.StateName);
        await Assert.That(op.State.Data).Contains(p => p.Key == "EnqueueAt"); // ScheduledState serializes EnqueueAt
    }

    [Test]
    public async Task CreateJob_ProducesAnIdAndACreateOp()
    {
        var id = _tx.CreateJob(Job.FromExpression(() => TestJobs.Run("x")), new Dictionary<string, string> { ["k"] = "v" }, DateTime.UtcNow, TimeSpan.FromDays(1));
        await Assert.That(string.IsNullOrEmpty(id)).IsFalse();
        var single = await Assert.That(_tx.PendingOps).HasSingleItem();
        await Assert.That(single).IsTypeOf<CreateJobOp>();
        var op = (CreateJobOp)single;
        await Assert.That(op.JobId).IsEqualTo(id);
        await Assert.That(op.Parameters).Contains(p => p.Key == "k" && p.Value == "v");
    }

    [Test]
    public async Task AllOpsAreBatched_InOrder()
    {
        _tx.IncrementCounter("c");
        _tx.AddToQueue("q", "j");
        _tx.ExpireJob("j", TimeSpan.FromMinutes(5));

        await Assert.That(_tx.PendingOps.Count).IsEqualTo(3);
        await Assert.That(_tx.PendingOps[0]).IsTypeOf<IncrementCounterOp>();
        await Assert.That(_tx.PendingOps[1]).IsTypeOf<EnqueueOp>();
        await Assert.That(_tx.PendingOps[2]).IsTypeOf<ExpireJobOp>();
    }

    [Test]
    [Arguments("")]
    [Arguments((string?)null)]
    public async Task Builders_RejectNullOrEmptyKeys(string? bad)
    {
        // The key/id guards are uniform across the builders, so spot-check a representative set
        // (counter key, set key, queue name, job id) for both empty and null. Throws<ArgumentException>
        // accepts the ArgumentNullException thrown for null as well as the ArgumentException thrown for empty.
        await Assert.That(() => _tx.IncrementCounter(bad!)).Throws<ArgumentException>();
        await Assert.That(() => _tx.AddToSet(bad!, "v")).Throws<ArgumentException>();
        await Assert.That(() => _tx.AddToQueue(bad!, "j")).Throws<ArgumentException>();
        await Assert.That(() => _tx.SetJobParameter(bad!, "n", "v")).Throws<ArgumentException>();
    }

    [Test]
    public async Task EveryBuilder_ProducesTheExpectedOpInOrder()
    {
        _tx.SetJobParameter("j", "n", "v");
        _tx.AddJobState("j", new ScheduledState(TimeSpan.Zero));
        _tx.ExpireJob("j", TimeSpan.FromMinutes(1));
        _tx.PersistJob("j");
        _tx.DecrementCounter("c", TimeSpan.FromMinutes(1));
        _tx.AddToSet("s", "v", 1.5);
        _tx.AddRangeToSet("s", ["a", "b"]);
        _tx.RemoveFromSet("s", "v");
        _tx.RemoveSet("s");
        _tx.ExpireSet("s", TimeSpan.FromMinutes(1));
        _tx.PersistSet("s");
        _tx.InsertToList("l", "v");
        _tx.RemoveFromList("l", "v");
        _tx.TrimList("l", 0, 9);
        _tx.ExpireList("l", TimeSpan.FromMinutes(1));
        _tx.PersistList("l");
        _tx.SetRangeInHash("h", [new("f", "v")]);
        _tx.RemoveHash("h");
        _tx.ExpireHash("h", TimeSpan.FromMinutes(1));
        _tx.PersistHash("h");

        await Assert.That(_tx.PendingOps.Count).IsEqualTo(20);
        await Assert.That(_tx.PendingOps[0]).IsTypeOf<SetJobParameterOp>();
        await Assert.That(_tx.PendingOps[1]).IsTypeOf<AddJobStateOp>();
        await Assert.That(_tx.PendingOps[2]).IsTypeOf<ExpireJobOp>();
        await Assert.That(_tx.PendingOps[3]).IsTypeOf<PersistJobOp>();
        await Assert.That(_tx.PendingOps[4]).IsTypeOf<IncrementCounterOp>(); // DecrementCounter maps to IncrementCounterOp(-1)
        await Assert.That(_tx.PendingOps[5]).IsTypeOf<AddToSetOp>();
        await Assert.That(_tx.PendingOps[6]).IsTypeOf<AddRangeToSetOp>();
        await Assert.That(_tx.PendingOps[7]).IsTypeOf<RemoveFromSetOp>();
        await Assert.That(_tx.PendingOps[8]).IsTypeOf<RemoveSetOp>();
        await Assert.That(_tx.PendingOps[9]).IsTypeOf<ExpireSetOp>();
        await Assert.That(_tx.PendingOps[10]).IsTypeOf<PersistSetOp>();
        await Assert.That(_tx.PendingOps[11]).IsTypeOf<InsertToListOp>();
        await Assert.That(_tx.PendingOps[12]).IsTypeOf<RemoveFromListOp>();
        await Assert.That(_tx.PendingOps[13]).IsTypeOf<TrimListOp>();
        await Assert.That(_tx.PendingOps[14]).IsTypeOf<ExpireListOp>();
        await Assert.That(_tx.PendingOps[15]).IsTypeOf<PersistListOp>();
        await Assert.That(_tx.PendingOps[16]).IsTypeOf<SetRangeInHashOp>();
        await Assert.That(_tx.PendingOps[17]).IsTypeOf<RemoveHashOp>();
        await Assert.That(_tx.PendingOps[18]).IsTypeOf<ExpireHashOp>();
        await Assert.That(_tx.PendingOps[19]).IsTypeOf<PersistHashOp>();
    }
}
