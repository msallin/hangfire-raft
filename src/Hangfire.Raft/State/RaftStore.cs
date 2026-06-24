using Hangfire.Raft.Commands;
using static Hangfire.Raft.Commands.BinaryFormat;

namespace Hangfire.Raft.State;

/// <summary>
/// The replicated in-memory data store. Commands are applied deterministically: the only wall-clock
/// input is the submitter timestamp in the command envelope, so every cluster node that applies the
/// same log arrives at the same state. All public members are thread-safe via a single store lock;
/// applies are serialized by the Raft commit pipeline anyway, reads come from Hangfire worker and
/// dashboard threads.
/// </summary>
internal sealed partial class RaftStore
{
    private readonly object _sync = new();

    private readonly Dictionary<string, JobEntry> _jobs = [];
    private readonly Dictionary<string, LinkedList<string>> _queues = [];
    private readonly Dictionary<Guid, FetchedEntry> _fetched = [];
    private readonly Dictionary<string, SetEntry> _sets = [];
    private readonly Dictionary<string, ListEntry> _lists = [];
    private readonly Dictionary<string, HashEntry> _hashes = [];
    private readonly Dictionary<string, CounterEntry> _counters = [];
    private readonly Dictionary<string, ServerEntry> _servers = [];
    private readonly Dictionary<string, LockEntry> _locks = [];

    /// <summary>state name -> jobs currently in that state, ordered by transition time then id.</summary>
    private readonly Dictionary<string, SortedSet<JobEntry>> _jobsByState = [];

    public ApplyEffects Apply(Command command)
    {
        var effects = new ApplyEffects();
        lock (_sync)
        {
            foreach (var op in command.Ops)
                effects.Result = ApplyOp(op, command.NowUtc, effects);
        }
        return effects;
    }

    // When adding a new op: extend this switch; unknown ops throw so a forgotten case cannot silently
    // diverge the replicated state. Op handlers must otherwise NOT throw for a well-formed (decoded)
    // command: Apply mutates in place with no rollback, so a throw mid-batch would leave partial state
    // and fault the apply pipeline. The serializer rejects unknown opcodes at decode and the leader
    // pre-validates decodability before append, so a committed command only ever carries known ops.
    private object? ApplyOp(StoreOp op, DateTime now, ApplyEffects effects)
    {
        switch (op)
        {
            case CreateJobOp o:
                {
                    if (_jobs.TryGetValue(o.JobId, out var existing)) RemoveFromStateIndex(existing);
                    var job = new JobEntry { Id = o.JobId, InvocationData = o.InvocationData, CreatedAt = o.CreatedAt, ExpireAt = o.ExpireAt };
                    foreach (var (key, value) in o.Parameters) job.Parameters[key] = value;
                    _jobs[o.JobId] = job;
                    return null;
                }
            case SetJobParameterOp o:
                {
                    if (_jobs.TryGetValue(o.JobId, out var job)) job.Parameters[o.Name] = o.Value;
                    return null;
                }
            case SetJobStateOp o:
                {
                    if (_jobs.TryGetValue(o.JobId, out var job))
                    {
                        RemoveFromStateIndex(job);
                        job.History.Add(o.State);
                        job.CurrentState = o.State;
                        job.StateChangedAt = o.State.CreatedAt;
                        AddToStateIndex(job);
                    }
                    return null;
                }
            case AddJobStateOp o:
                {
                    if (_jobs.TryGetValue(o.JobId, out var job)) job.History.Add(o.State);
                    return null;
                }
            case ExpireJobOp o:
                {
                    if (_jobs.TryGetValue(o.JobId, out var job)) job.ExpireAt = o.ExpireAt;
                    return null;
                }
            case PersistJobOp o:
                {
                    if (_jobs.TryGetValue(o.JobId, out var job)) job.ExpireAt = null;
                    return null;
                }
            case EnqueueOp o:
                {
                    Queue(o.Queue).AddLast(o.JobId);
                    effects.SignalQueue(o.Queue);
                    return null;
                }
            case FetchOp o:
                {
                    foreach (var queueName in o.Queues)
                    {
                        if (!_queues.TryGetValue(queueName, out var queue)) continue;
                        while (queue.Count > 0)
                        {
                            var jobId = queue.First!.Value;
                            queue.RemoveFirst();
                            // Jobs can expire while still queued; skip dangling ids deterministically.
                            if (_jobs.ContainsKey(jobId))
                            {
                                _fetched[o.FetchToken] = new FetchedEntry { JobId = jobId, Queue = queueName, FetchedAt = now };
                                return new FetchResult(jobId, queueName);
                            }
                        }
                    }
                    return null;
                }
            case AckFetchedOp o:
                {
                    _fetched.Remove(o.FetchToken);
                    return null;
                }
            case RequeueFetchedOp o:
                {
                    // Always drop the lease; re-enqueue only if the job still exists. Maintenance never
                    // evicts a job while it is fetched, so this existence guard is defensive (it matches
                    // the stale-fetch reclaim in RunMaintenance).
                    if (_fetched.Remove(o.FetchToken, out var fetched) && _jobs.TryGetValue(fetched.JobId, out var job))
                    {
                        // A re-enqueued job is runnable again, so clear any pending expiry. Without this, a job
                        // whose ExpireAt already elapsed while it was fetched (an ExpireJob that raced the fetch)
                        // would be put back on the queue only to be evicted by the next maintenance pass with
                        // zero executions. ExpireAt is not part of the state index, so clearing it here is safe.
                        job.ExpireAt = null;
                        Queue(fetched.Queue).AddFirst(fetched.JobId);
                        effects.SignalQueue(fetched.Queue);
                    }
                    return null;
                }
            case RenewFetchedOp o:
                {
                    if (_fetched.TryGetValue(o.FetchToken, out var fetched))
                    {
                        fetched.FetchedAt = now;
                        return true;
                    }
                    return false;
                }
            case IncrementCounterOp o:
                {
                    if (!_counters.TryGetValue(o.Key, out var counter)) _counters[o.Key] = counter = new CounterEntry();
                    counter.Value += o.Delta;
                    if (o.ExpireAt is { } expireAt)
                        counter.ExpireAt = counter.ExpireAt is { } current && current > expireAt ? current : expireAt;
                    if (counter is { Value: 0, ExpireAt: null }) _counters.Remove(o.Key);
                    return null;
                }
            case AddToSetOp o:
                {
                    AddToSet(o.Key, o.Value, o.Score);
                    return null;
                }
            case AddRangeToSetOp o:
                {
                    foreach (var value in o.Values) AddToSet(o.Key, value, 0d);
                    return null;
                }
            case RemoveFromSetOp o:
                {
                    if (_sets.TryGetValue(o.Key, out var set) && set.Scores.Remove(o.Value, out var score))
                    {
                        set.Sorted.Remove(new SetItem(score, o.Value));
                        if (set.Scores.Count == 0) _sets.Remove(o.Key);
                    }
                    return null;
                }
            case RemoveSetOp o:
                {
                    _sets.Remove(o.Key);
                    return null;
                }
            case ExpireSetOp o:
                {
                    if (_sets.TryGetValue(o.Key, out var set)) set.ExpireAt = o.ExpireAt;
                    return null;
                }
            case PersistSetOp o:
                {
                    if (_sets.TryGetValue(o.Key, out var set)) set.ExpireAt = null;
                    return null;
                }
            case InsertToListOp o:
                {
                    if (!_lists.TryGetValue(o.Key, out var list)) _lists[o.Key] = list = new ListEntry();
                    list.Items.Insert(0, o.Value);
                    return null;
                }
            case RemoveFromListOp o:
                {
                    if (_lists.TryGetValue(o.Key, out var list))
                    {
                        list.Items.RemoveAll(v => v == o.Value);
                        if (list.Items.Count == 0) _lists.Remove(o.Key);
                    }
                    return null;
                }
            case TrimListOp o:
                {
                    if (_lists.TryGetValue(o.Key, out var list))
                    {
                        var from = Math.Max(0, o.KeepStartingFrom);
                        var to = Math.Min(list.Items.Count - 1, o.KeepEndingAt);
                        var kept = from <= to ? list.Items.GetRange(from, to - from + 1) : [];
                        list.Items.Clear();
                        list.Items.AddRange(kept);
                        if (list.Items.Count == 0) _lists.Remove(o.Key);
                    }
                    return null;
                }
            case ExpireListOp o:
                {
                    if (_lists.TryGetValue(o.Key, out var list)) list.ExpireAt = o.ExpireAt;
                    return null;
                }
            case PersistListOp o:
                {
                    if (_lists.TryGetValue(o.Key, out var list)) list.ExpireAt = null;
                    return null;
                }
            case SetRangeInHashOp o:
                {
                    if (!_hashes.TryGetValue(o.Key, out var hash)) _hashes[o.Key] = hash = new HashEntry();
                    foreach (var (field, value) in o.Fields) hash.Fields[field] = value;
                    return null;
                }
            case RemoveHashOp o:
                {
                    _hashes.Remove(o.Key);
                    return null;
                }
            case ExpireHashOp o:
                {
                    if (_hashes.TryGetValue(o.Key, out var hash)) hash.ExpireAt = o.ExpireAt;
                    return null;
                }
            case PersistHashOp o:
                {
                    if (_hashes.TryGetValue(o.Key, out var hash)) hash.ExpireAt = null;
                    return null;
                }
            case AnnounceServerOp o:
                {
                    _servers[o.ServerId] = new ServerEntry
                    {
                        WorkerCount = o.WorkerCount,
                        Queues = o.Queues,
                        StartedAt = now,
                        LastHeartbeat = now,
                    };
                    return null;
                }
            case RemoveServerOp o:
                {
                    _servers.Remove(o.ServerId);
                    return null;
                }
            case HeartbeatOp o:
                {
                    if (_servers.TryGetValue(o.ServerId, out var server))
                    {
                        server.LastHeartbeat = now;
                        return true;
                    }
                    return false;
                }
            case RemoveTimedOutServersOp o:
                {
                    var cutoff = now - o.Timeout;
                    var timedOut = _servers.Where(kv => kv.Value.LastHeartbeat < cutoff).Select(kv => kv.Key).ToList();
                    foreach (var id in timedOut) _servers.Remove(id);
                    return timedOut.Count;
                }
            case TryAcquireLockOp o:
                {
                    if (!_locks.TryGetValue(o.Resource, out var existing) || existing.ExpiresAt <= now || existing.Owner == o.Owner)
                    {
                        _locks[o.Resource] = new LockEntry { Owner = o.Owner, ExpiresAt = now + o.Lease };
                        return true;
                    }
                    return false;
                }
            case ReleaseLockOp o:
                {
                    if (_locks.TryGetValue(o.Resource, out var existing) && existing.Owner == o.Owner)
                    {
                        _locks.Remove(o.Resource);
                        effects.LocksReleased = true;
                    }
                    return null;
                }
            case MaintenanceOp o:
                // The summary is returned as the op result so the submitting (leader) node can log and meter
                // the outcome once, instead of every replica logging from its deterministic apply.
                return RunMaintenance(o, now, effects);
            default:
                throw new NotSupportedException($"Op {op.GetType().Name} has no apply handler.");
        }
    }

    private void AddToSet(string key, string value, double score)
    {
        if (!_sets.TryGetValue(key, out var set)) _sets[key] = set = new SetEntry();
        if (set.Scores.TryGetValue(value, out var oldScore)) set.Sorted.Remove(new SetItem(oldScore, value));
        set.Scores[value] = score;
        set.Sorted.Add(new SetItem(score, value));
    }

    private MaintenanceSummary RunMaintenance(MaintenanceOp op, DateTime now, ApplyEffects effects)
    {
        // Never evict a job that is currently held under a fetch lease: the worker (or the stale-fetch
        // reclaim below) still needs it. Otherwise an ExpireJob racing an in-flight fetch could drop the
        // job with zero executions instead of letting it run. The fetched-id set is a deterministic
        // function of _fetched, so every replica makes the same decision.
        var fetchedJobIds = _fetched.Values.Select(f => f.JobId).ToHashSet(StringComparer.Ordinal);
        var evictedJobs = _jobs.Values.Where(j => j.ExpireAt is { } e && e <= now && !fetchedJobIds.Contains(j.Id)).ToList();
        foreach (var job in evictedJobs)
        {
            RemoveFromStateIndex(job);
            _jobs.Remove(job.Id);
        }

        // A queue entry can reference a missing job only as a result of a job eviction (the eviction
        // that removes a job removes its queue references in the same pass; nothing else enqueues a
        // non-existent id), so this O(total queued) walk is only needed when something was evicted.
        // The branch is deterministic across nodes (evictedJobs is a pure function of the envelope time).
        // A nonzero count means an enqueued job was dropped along with the queue entry pointing at it,
        // which the leader surfaces as a warning (see RaftStorageCluster.MaintenanceLoop).
        var orphanedQueueEntries = 0;
        if (evictedJobs.Count > 0)
        {
            foreach (var queue in _queues.Values)
            {
                var node = queue.First;
                while (node is not null)
                {
                    var next = node.Next;
                    if (!_jobs.ContainsKey(node.Value))
                    {
                        queue.Remove(node);
                        orphanedQueueEntries++;
                    }

                    node = next;
                }
            }
        }

        var expiredCollections =
            EvictExpired(_sets, e => e.ExpireAt, now)
            + EvictExpired(_lists, e => e.ExpireAt, now)
            + EvictExpired(_hashes, e => e.ExpireAt, now)
            + EvictExpired(_counters, e => e.ExpireAt, now);

        var expiredLocks = 0;
        foreach (var resource in _locks.Where(kv => kv.Value.ExpiresAt <= now).Select(kv => kv.Key).ToList())
        {
            _locks.Remove(resource);
            effects.LocksReleased = true;
            expiredLocks++;
        }

        // Requeue order must not depend on Dictionary enumeration order: that order differs between
        // a node that built _fetched incrementally and one that restored it from a snapshot, which
        // would diverge the replicated queues. Sort stale leases explicitly; iterating oldest-last
        // with AddFirst puts the oldest fetch back at the very head of its queue.
        var stale = _fetched
            .Where(kv => kv.Value.FetchedAt + op.FetchInvisibilityTimeout <= now)
            .OrderByDescending(kv => kv.Value.FetchedAt)
            .ThenByDescending(kv => kv.Value.JobId, StringComparer.Ordinal)
            .ThenByDescending(kv => kv.Key)
            .ToList();
        var staleReclaimed = 0;
        foreach (var (token, fetched) in stale)
        {
            _fetched.Remove(token);
            if (_jobs.TryGetValue(fetched.JobId, out var job))
            {
                // The lease is being reclaimed because no worker renewed it (a crashed/partitioned worker,
                // or one that stalled past the invisibility timeout); the job is about to run again. Clear
                // any pending expiry so the re-enqueued job is not evicted by the NEXT maintenance pass: it
                // is no longer in fetchedJobIds, so without this an expired-while-fetched job would be lost
                // here with zero executions. Each reclaim means a possible duplicate run, surfaced by the
                // leader as a warning + metric (see RaftStorageCluster.MaintenanceLoop).
                job.ExpireAt = null;
                Queue(fetched.Queue).AddFirst(fetched.JobId);
                effects.SignalQueue(fetched.Queue);
                staleReclaimed++;
            }
        }

        // Queues whose last item was consumed disappear, matching how SQL-backed storages behave.
        foreach (var name in _queues.Where(kv => kv.Value.Count == 0).Select(kv => kv.Key).ToList())
            _queues.Remove(name);

        return new MaintenanceSummary(evictedJobs.Count, orphanedQueueEntries, staleReclaimed, expiredLocks, expiredCollections);
    }

    private static int EvictExpired<TEntry>(Dictionary<string, TEntry> table, Func<TEntry, DateTime?> expireAt, DateTime now)
    {
        var expired = table.Where(kv => expireAt(kv.Value) is { } e && e <= now).Select(kv => kv.Key).ToList();
        foreach (var key in expired) table.Remove(key);
        return expired.Count;
    }

    private LinkedList<string> Queue(string name)
    {
        if (!_queues.TryGetValue(name, out var queue)) _queues[name] = queue = [];
        return queue;
    }

    private void AddToStateIndex(JobEntry job)
    {
        if (job.CurrentState is null) return;
        if (!_jobsByState.TryGetValue(job.CurrentState.Name, out var index))
            _jobsByState[job.CurrentState.Name] = index = new SortedSet<JobEntry>(JobStateIndexComparer.Instance);
        index.Add(job);
    }

    private void RemoveFromStateIndex(JobEntry job)
    {
        if (job.CurrentState is null) return;
        if (_jobsByState.TryGetValue(job.CurrentState.Name, out var index))
        {
            index.Remove(job);
            if (index.Count == 0) _jobsByState.Remove(job.CurrentState.Name);
        }
    }
}

/// <summary>
/// Side effects of applying a command, consumed by the state machine outside the store lock:
/// the op result for the submitting waiter and which local signals to pulse.
/// </summary>
internal sealed class ApplyEffects
{
    public object? Result { get; set; }
    public HashSet<string>? SignaledQueues { get; private set; }
    public bool LocksReleased { get; set; }

    public void SignalQueue(string queue) => (SignaledQueues ??= []).Add(queue);
}

/// <summary>
/// What a single maintenance pass changed. Returned as the <see cref="MaintenanceOp"/> apply result so the
/// submitting (leader) node can log and meter it once, rather than every replica logging from its
/// deterministic apply. The counts are a pure function of the committed command, so they match on every node.
/// </summary>
internal sealed record MaintenanceSummary(
    int EvictedJobs,
    int OrphanedQueueEntriesRemoved,
    int StaleFetchesReclaimed,
    int ExpiredLocksReleased,
    int ExpiredCollections);
