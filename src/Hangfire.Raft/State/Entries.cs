using Hangfire.Raft.Commands;

namespace Hangfire.Raft.State;

/// <summary>
/// Mutable in-memory entities of the replicated store. Instances are only touched while holding
/// the store lock; everything handed out to callers is a copy (see JobSnapshot and the read API).
/// </summary>
internal sealed class JobEntry
{
    public required string Id { get; init; }
    public required string InvocationData { get; init; }
    public required DateTime CreatedAt { get; init; }
    public Dictionary<string, string?> Parameters { get; } = [];
    public List<StateRecord> History { get; } = [];
    public StateRecord? CurrentState { get; set; }

    /// <summary>Copy of CurrentState.CreatedAt used as the state-index sort key. Must only change while the job is not in the index.</summary>
    public DateTime StateChangedAt { get; set; }

    public DateTime? ExpireAt { get; set; }
}

internal sealed class FetchedEntry
{
    public required string JobId { get; init; }
    public required string Queue { get; init; }
    public DateTime FetchedAt { get; set; }
}

internal sealed class SetEntry
{
    /// <summary>value -> score; kept in sync with <see cref="Sorted"/>.</summary>
    public Dictionary<string, double> Scores { get; } = [];

    public SortedSet<SetItem> Sorted { get; } = new(SetItemComparer.Instance);
    public DateTime? ExpireAt { get; set; }
}

internal readonly record struct SetItem(double Score, string Value);

/// <summary>Orders set items by score, then ordinally by value, which makes range reads deterministic across nodes.</summary>
internal sealed class SetItemComparer : IComparer<SetItem>
{
    public static readonly SetItemComparer Instance = new();

    public int Compare(SetItem x, SetItem y)
    {
        var byScore = x.Score.CompareTo(y.Score);
        return byScore != 0 ? byScore : string.CompareOrdinal(x.Value, y.Value);
    }
}

internal sealed class ListEntry
{
    /// <summary>Index 0 is the newest item, matching Hangfire's newest-first list reads.</summary>
    public List<string> Items { get; } = [];

    public DateTime? ExpireAt { get; set; }
}

internal sealed class HashEntry
{
    public Dictionary<string, string?> Fields { get; } = [];
    public DateTime? ExpireAt { get; set; }
}

internal sealed class CounterEntry
{
    public long Value { get; set; }
    public DateTime? ExpireAt { get; set; }
}

internal sealed class ServerEntry
{
    public required int WorkerCount { get; init; }
    public required IReadOnlyList<string> Queues { get; init; }
    public required DateTime StartedAt { get; init; }
    public DateTime LastHeartbeat { get; set; }
}

internal sealed class LockEntry
{
    public required Guid Owner { get; set; }
    public required DateTime ExpiresAt { get; set; }
}

/// <summary>Orders the per-state job index by state-transition time, then by id for a deterministic total order.</summary>
internal sealed class JobStateIndexComparer : IComparer<JobEntry>
{
    public static readonly JobStateIndexComparer Instance = new();

    public int Compare(JobEntry? x, JobEntry? y)
    {
        if (ReferenceEquals(x, y)) return 0;
        if (x is null) return -1;
        if (y is null) return 1;
        var byTime = x.StateChangedAt.CompareTo(y.StateChangedAt);
        return byTime != 0 ? byTime : string.CompareOrdinal(x.Id, y.Id);
    }
}

/// <summary>Immutable copy of a job handed out by the read API.</summary>
internal sealed record JobSnapshot(
    string Id,
    string InvocationData,
    DateTime CreatedAt,
    DateTime? ExpireAt,
    IReadOnlyDictionary<string, string?> Parameters,
    StateRecord? CurrentState,
    IReadOnlyList<StateRecord> History);

internal sealed record ServerSnapshot(string Id, int WorkerCount, IReadOnlyList<string> Queues, DateTime StartedAt, DateTime LastHeartbeat);

internal sealed record QueueSnapshot(string Name, int Length, IReadOnlyList<string> TopJobIds);
