namespace Hangfire.Raft.Commands;

/// <summary>
/// Envelope for one replicated mutation. <see cref="Id"/> correlates the submitting node's waiter
/// with the local apply of the entry; <see cref="NowUtc"/> is the submitter's clock and the only
/// wall-clock input the state machine may use while applying the contained ops.
/// </summary>
internal sealed class Command
{
    public required Guid Id { get; init; }
    public required DateTime NowUtc { get; init; }
    public required IReadOnlyList<StoreOp> Ops { get; init; }

    public static Command Single(StoreOp op) => new()
    {
        Id = Guid.NewGuid(),
        NowUtc = DateTime.UtcNow,
        Ops = [op],
    };

    public static Command Batch(IReadOnlyList<StoreOp> ops) => new()
    {
        Id = Guid.NewGuid(),
        NowUtc = DateTime.UtcNow,
        Ops = ops,
    };
}
