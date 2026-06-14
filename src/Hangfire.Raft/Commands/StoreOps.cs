namespace Hangfire.Raft.Commands;

/// <summary>
/// The closed set of mutations that can be applied to the replicated store. Every Hangfire write
/// is expressed as one command containing one or more ops; ops inside a command apply atomically.
/// All timestamps that influence state come from the command envelope or the op payload, never
/// from the local clock, so that every node applies the log deterministically.
///
/// When adding a new op:
/// 1. Add the record here and a value to <see cref="OpCode"/>.
/// 2. Add write/read cases in CommandSerializer (it throws on unknown ops).
/// 3. Add an apply case in RaftStore.ApplyOp.
/// 4. Extend the serializer roundtrip test with the new op.
/// </summary>
internal abstract record StoreOp;

/// <summary>Stable byte tag for each <see cref="StoreOp"/> on the wire. Values must not be renumbered or reused once shipped, or an existing write-ahead log would decode wrongly.</summary>
internal enum OpCode : byte
{
    CreateJob = 1,
    SetJobParameter = 2,
    SetJobState = 3,
    AddJobState = 4,
    ExpireJob = 5,
    PersistJob = 6,
    Enqueue = 7,
    Fetch = 8,
    AckFetched = 9,
    RequeueFetched = 10,
    RenewFetched = 11,
    IncrementCounter = 12,
    AddToSet = 13,
    AddRangeToSet = 14,
    RemoveFromSet = 15,
    RemoveSet = 16,
    ExpireSet = 17,
    PersistSet = 18,
    InsertToList = 19,
    RemoveFromList = 20,
    TrimList = 21,
    ExpireList = 22,
    PersistList = 23,
    SetRangeInHash = 24,
    RemoveHash = 25,
    ExpireHash = 26,
    PersistHash = 27,
    AnnounceServer = 28,
    RemoveServer = 29,
    Heartbeat = 30,
    RemoveTimedOutServers = 31,
    TryAcquireLock = 32,
    ReleaseLock = 33,
    Maintenance = 34,
}

/// <summary>A single entry of a job's state history, as stored and replicated.</summary>
internal sealed record StateRecord(string Name, string? Reason, IReadOnlyList<KeyValuePair<string, string?>> Data, DateTime CreatedAt);

/// <summary>Result of a <see cref="FetchOp"/>: the dequeued job and the queue it came from.</summary>
internal readonly record struct FetchResult(string JobId, string Queue);

internal sealed record CreateJobOp(string JobId, string InvocationData, IReadOnlyList<KeyValuePair<string, string?>> Parameters, DateTime CreatedAt, DateTime ExpireAt) : StoreOp;
internal sealed record SetJobParameterOp(string JobId, string Name, string? Value) : StoreOp;
internal sealed record SetJobStateOp(string JobId, StateRecord State) : StoreOp;
internal sealed record AddJobStateOp(string JobId, StateRecord State) : StoreOp;
internal sealed record ExpireJobOp(string JobId, DateTime ExpireAt) : StoreOp;
internal sealed record PersistJobOp(string JobId) : StoreOp;

internal sealed record EnqueueOp(string Queue, string JobId) : StoreOp;

/// <summary>Dequeues the first available job from the first non-empty queue, in the given order. Result: <see cref="FetchResult"/>? (boxed, null when all queues are empty).</summary>
internal sealed record FetchOp(IReadOnlyList<string> Queues, Guid FetchToken) : StoreOp;
internal sealed record AckFetchedOp(Guid FetchToken) : StoreOp;
internal sealed record RequeueFetchedOp(Guid FetchToken) : StoreOp;

/// <summary>Refreshes the fetch lease so maintenance does not reclaim an actively processed job. Result: bool (false when the lease no longer exists).</summary>
internal sealed record RenewFetchedOp(Guid FetchToken) : StoreOp;

internal sealed record IncrementCounterOp(string Key, long Delta, DateTime? ExpireAt) : StoreOp;

internal sealed record AddToSetOp(string Key, string Value, double Score) : StoreOp;
internal sealed record AddRangeToSetOp(string Key, IReadOnlyList<string> Values) : StoreOp;
internal sealed record RemoveFromSetOp(string Key, string Value) : StoreOp;
internal sealed record RemoveSetOp(string Key) : StoreOp;
internal sealed record ExpireSetOp(string Key, DateTime ExpireAt) : StoreOp;
internal sealed record PersistSetOp(string Key) : StoreOp;

internal sealed record InsertToListOp(string Key, string Value) : StoreOp;
internal sealed record RemoveFromListOp(string Key, string Value) : StoreOp;
internal sealed record TrimListOp(string Key, int KeepStartingFrom, int KeepEndingAt) : StoreOp;
internal sealed record ExpireListOp(string Key, DateTime ExpireAt) : StoreOp;
internal sealed record PersistListOp(string Key) : StoreOp;

internal sealed record SetRangeInHashOp(string Key, IReadOnlyList<KeyValuePair<string, string?>> Fields) : StoreOp;
internal sealed record RemoveHashOp(string Key) : StoreOp;
internal sealed record ExpireHashOp(string Key, DateTime ExpireAt) : StoreOp;
internal sealed record PersistHashOp(string Key) : StoreOp;

internal sealed record AnnounceServerOp(string ServerId, int WorkerCount, IReadOnlyList<string> Queues) : StoreOp;
internal sealed record RemoveServerOp(string ServerId) : StoreOp;

/// <summary>Updates the server heartbeat. Result: bool (false when the server is unknown).</summary>
internal sealed record HeartbeatOp(string ServerId) : StoreOp;

/// <summary>Removes servers whose last heartbeat is older than the timeout. Result: int (number removed).</summary>
internal sealed record RemoveTimedOutServersOp(TimeSpan Timeout) : StoreOp;

/// <summary>
/// Acquires or renews a lock lease. Succeeds when the lock is free, expired, or already held by
/// the same owner (renewal extends the lease). Result: bool.
/// </summary>
internal sealed record TryAcquireLockOp(string Resource, Guid Owner, TimeSpan Lease) : StoreOp;
internal sealed record ReleaseLockOp(string Resource, Guid Owner) : StoreOp;

/// <summary>
/// Periodic leader-submitted cleanup: evicts expired jobs/sets/lists/hashes/counters, drops expired
/// lock leases and requeues fetched jobs whose lease exceeded the invisibility timeout.
/// </summary>
internal sealed record MaintenanceOp(TimeSpan FetchInvisibilityTimeout) : StoreOp;
