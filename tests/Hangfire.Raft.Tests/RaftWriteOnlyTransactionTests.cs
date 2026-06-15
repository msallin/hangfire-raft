using Hangfire.Common;
using Hangfire.Raft.Commands;
using Hangfire.States;
using Hangfire.Storage;

namespace Hangfire.Raft.Tests;

/// <summary>
/// Unit tests of the Hangfire-call to <see cref="StoreOp"/> mapping. The transaction only touches the
/// cluster in Commit, so these build ops and inspect them via the PendingOps seam without a node.
/// </summary>
public class RaftWriteOnlyTransactionTests
{
    // storage is referenced only by Commit, which these tests never call.
    private readonly RaftWriteOnlyTransaction _tx = new(storage: null!);

    [Fact]
    public void DecrementCounter_EmitsIncrementByMinusOne()
    {
        _tx.DecrementCounter("c");
        var op = Assert.IsType<IncrementCounterOp>(Assert.Single(_tx.PendingOps));
        Assert.Equal("c", op.Key);
        Assert.Equal(-1, op.Delta);
        Assert.Null(op.ExpireAt);
    }

    [Fact]
    public void IncrementCounter_WithExpiry_SetsExpireAt()
    {
        _tx.IncrementCounter("c", TimeSpan.FromHours(1));
        var op = Assert.IsType<IncrementCounterOp>(Assert.Single(_tx.PendingOps));
        Assert.Equal(1, op.Delta);
        Assert.NotNull(op.ExpireAt);
    }

    [Fact]
    public void AddToSet_WithoutScore_DefaultsToZero()
    {
        _tx.AddToSet("s", "v");
        var op = Assert.IsType<AddToSetOp>(Assert.Single(_tx.PendingOps));
        Assert.Equal(0.0d, op.Score);
    }

    [Fact]
    public void SetJobState_MapsNameReasonAndData()
    {
        _tx.SetJobState("j", new ScheduledState(TimeSpan.FromHours(2)));
        var op = Assert.IsType<SetJobStateOp>(Assert.Single(_tx.PendingOps));
        Assert.Equal("j", op.JobId);
        Assert.Equal(ScheduledState.StateName, op.State.Name);
        Assert.Contains(op.State.Data, p => p.Key == "EnqueueAt"); // ScheduledState serializes EnqueueAt
    }

    [Fact]
    public void CreateJob_ProducesAnIdAndACreateOp()
    {
        var id = _tx.CreateJob(Job.FromExpression(() => TestJobs.Run("x")), new Dictionary<string, string> { ["k"] = "v" }, DateTime.UtcNow, TimeSpan.FromDays(1));
        Assert.False(string.IsNullOrEmpty(id));
        var op = Assert.IsType<CreateJobOp>(Assert.Single(_tx.PendingOps));
        Assert.Equal(id, op.JobId);
        Assert.Contains(op.Parameters, p => p.Key == "k" && p.Value == "v");
    }

    [Fact]
    public void AllOpsAreBatched_InOrder()
    {
        _tx.IncrementCounter("c");
        _tx.AddToQueue("q", "j");
        _tx.ExpireJob("j", TimeSpan.FromMinutes(5));

        Assert.Collection(_tx.PendingOps,
            o => Assert.IsType<IncrementCounterOp>(o),
            o => Assert.IsType<EnqueueOp>(o),
            o => Assert.IsType<ExpireJobOp>(o));
    }

    [Theory]
    [InlineData("")]
    [InlineData(null)]
    public void Builders_RejectNullOrEmptyKeys(string? bad)
    {
        // The key/id guards are uniform across the builders, so spot-check a representative set
        // (counter key, set key, queue name, job id) for both empty and null. ThrowsAny accepts the
        // ArgumentNullException thrown for null as well as the ArgumentException thrown for empty.
        Assert.ThrowsAny<ArgumentException>(() => _tx.IncrementCounter(bad!));
        Assert.ThrowsAny<ArgumentException>(() => _tx.AddToSet(bad!, "v"));
        Assert.ThrowsAny<ArgumentException>(() => _tx.AddToQueue(bad!, "j"));
        Assert.ThrowsAny<ArgumentException>(() => _tx.SetJobParameter(bad!, "n", "v"));
    }

    [Fact]
    public void EveryBuilder_ProducesTheExpectedOpInOrder()
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

        Assert.Collection(_tx.PendingOps,
            o => Assert.IsType<SetJobParameterOp>(o),
            o => Assert.IsType<AddJobStateOp>(o),
            o => Assert.IsType<ExpireJobOp>(o),
            o => Assert.IsType<PersistJobOp>(o),
            o => Assert.IsType<IncrementCounterOp>(o), // DecrementCounter maps to IncrementCounterOp(-1)
            o => Assert.IsType<AddToSetOp>(o),
            o => Assert.IsType<AddRangeToSetOp>(o),
            o => Assert.IsType<RemoveFromSetOp>(o),
            o => Assert.IsType<RemoveSetOp>(o),
            o => Assert.IsType<ExpireSetOp>(o),
            o => Assert.IsType<PersistSetOp>(o),
            o => Assert.IsType<InsertToListOp>(o),
            o => Assert.IsType<RemoveFromListOp>(o),
            o => Assert.IsType<TrimListOp>(o),
            o => Assert.IsType<ExpireListOp>(o),
            o => Assert.IsType<PersistListOp>(o),
            o => Assert.IsType<SetRangeInHashOp>(o),
            o => Assert.IsType<RemoveHashOp>(o),
            o => Assert.IsType<ExpireHashOp>(o),
            o => Assert.IsType<PersistHashOp>(o));
    }
}
