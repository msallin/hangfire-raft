using Hangfire.Raft.Commands;
using Hangfire.States;

namespace Hangfire.Raft.State;

/// <summary>
/// Read API of the store. Everything returned is a copy or an immutable record so callers never
/// observe mutation. Reads reflect the local node's applied log prefix; the submit pipeline gives
/// each writer read-your-writes consistency by waiting for the local apply.
/// </summary>
internal sealed partial class RaftStore
{
    private static readonly TimeSpan NoTtl = TimeSpan.FromSeconds(-1);

    public JobSnapshot? GetJob(string jobId)
    {
        lock (_sync)
        {
            return _jobs.TryGetValue(jobId, out var job) ? Snapshot(job) : null;
        }
    }

    public string? GetJobParameter(string jobId, string name)
    {
        lock (_sync)
        {
            return _jobs.TryGetValue(jobId, out var job) && job.Parameters.TryGetValue(name, out var value) ? value : null;
        }
    }

    public HashSet<string> GetAllItemsFromSet(string key)
    {
        lock (_sync)
        {
            return _sets.TryGetValue(key, out var set) ? [.. set.Scores.Keys] : [];
        }
    }

    public List<string> GetFirstByLowestScoreFromSet(string key, double fromScore, double toScore, int count)
    {
        lock (_sync)
        {
            var result = new List<string>();
            if (!_sets.TryGetValue(key, out var set)) return result;
            foreach (var item in set.Sorted)
            {
                if (item.Score > toScore || result.Count >= count) break;
                if (item.Score >= fromScore) result.Add(item.Value);
            }
            return result;
        }
    }

    public long GetSetCount(string key)
    {
        lock (_sync)
        {
            return _sets.TryGetValue(key, out var set) ? set.Scores.Count : 0;
        }
    }

    public long GetSetCount(IEnumerable<string> keys, int limit)
    {
        lock (_sync)
        {
            long total = 0;
            foreach (var key in keys)
            {
                if (_sets.TryGetValue(key, out var set)) total += set.Scores.Count;
                if (total >= limit) return limit;
            }
            return total;
        }
    }

    public bool GetSetContains(string key, string value)
    {
        lock (_sync)
        {
            return _sets.TryGetValue(key, out var set) && set.Scores.ContainsKey(value);
        }
    }

    public List<string> GetRangeFromSet(string key, int startingFrom, int endingAt)
    {
        lock (_sync)
        {
            var result = new List<string>();
            if (!_sets.TryGetValue(key, out var set) || startingFrom > endingAt) return result;
            var i = 0;
            foreach (var item in set.Sorted)
            {
                if (i > endingAt) break;
                if (i >= startingFrom) result.Add(item.Value);
                i++;
            }
            return result;
        }
    }

    public TimeSpan GetSetTtl(string key, DateTime now)
    {
        lock (_sync)
        {
            return _sets.TryGetValue(key, out var set) && set.ExpireAt is { } e ? e - now : NoTtl;
        }
    }

    public long GetCounter(string key)
    {
        lock (_sync)
        {
            return _counters.TryGetValue(key, out var counter) ? counter.Value : 0;
        }
    }

    public Dictionary<string, string?>? GetAllEntriesFromHash(string key)
    {
        lock (_sync)
        {
            return _hashes.TryGetValue(key, out var hash) ? new Dictionary<string, string?>(hash.Fields) : null;
        }
    }

    public long GetHashCount(string key)
    {
        lock (_sync)
        {
            return _hashes.TryGetValue(key, out var hash) ? hash.Fields.Count : 0;
        }
    }

    public TimeSpan GetHashTtl(string key, DateTime now)
    {
        lock (_sync)
        {
            return _hashes.TryGetValue(key, out var hash) && hash.ExpireAt is { } e ? e - now : NoTtl;
        }
    }

    public string? GetValueFromHash(string key, string name)
    {
        lock (_sync)
        {
            return _hashes.TryGetValue(key, out var hash) && hash.Fields.TryGetValue(name, out var value) ? value : null;
        }
    }

    public List<string> GetAllItemsFromList(string key)
    {
        lock (_sync)
        {
            return _lists.TryGetValue(key, out var list) ? [.. list.Items] : [];
        }
    }

    public long GetListCount(string key)
    {
        lock (_sync)
        {
            return _lists.TryGetValue(key, out var list) ? list.Items.Count : 0;
        }
    }

    public TimeSpan GetListTtl(string key, DateTime now)
    {
        lock (_sync)
        {
            return _lists.TryGetValue(key, out var list) && list.ExpireAt is { } e ? e - now : NoTtl;
        }
    }

    public List<string> GetRangeFromList(string key, int startingFrom, int endingAt)
    {
        lock (_sync)
        {
            var result = new List<string>();
            if (!_lists.TryGetValue(key, out var list)) return result;
            // Clamp the lower bound: a negative startingFrom must not index Items[-1]. An empty range
            // (startingFrom > endingAt) and a past-the-end endingAt fall out of the loop condition.
            for (var i = Math.Max(0, startingFrom); i <= endingAt && i < list.Items.Count; i++) result.Add(list.Items[i]);
            return result;
        }
    }

    /// <summary>Cheap local probe used by the fetch loop to avoid consensus round-trips on idle queues.</summary>
    public bool HasQueuedJob(IReadOnlyList<string> queues)
    {
        lock (_sync)
        {
            foreach (var name in queues)
            {
                if (_queues.TryGetValue(name, out var queue) && queue.Count > 0) return true;
            }
            return false;
        }
    }

    public List<QueueSnapshot> GetQueues(int topJobCount)
    {
        lock (_sync)
        {
            return _queues
                .OrderBy(kv => kv.Key, StringComparer.Ordinal)
                .Select(kv => new QueueSnapshot(kv.Key, kv.Value.Count, kv.Value.Take(topJobCount).ToList()))
                .ToList();
        }
    }

    public long GetQueueLength(string queue)
    {
        lock (_sync)
        {
            return _queues.TryGetValue(queue, out var q) ? q.Count : 0;
        }
    }

    public List<string> GetEnqueuedJobIds(string queue, int from, int count)
    {
        lock (_sync)
        {
            return _queues.TryGetValue(queue, out var q) ? q.Skip(from).Take(count).ToList() : [];
        }
    }

    public List<(string JobId, DateTime FetchedAt)> GetFetchedJobs(string queue, int from, int count)
    {
        lock (_sync)
        {
            return _fetched.Values
                .Where(f => f.Queue == queue)
                .OrderBy(f => f.FetchedAt)
                .ThenBy(f => f.JobId, StringComparer.Ordinal)
                .Skip(from)
                .Take(count)
                .Select(f => (f.JobId, f.FetchedAt))
                .ToList();
        }
    }

    public long GetFetchedCount(string queue)
    {
        lock (_sync)
        {
            return _fetched.Values.Count(f => f.Queue == queue);
        }
    }

    /// <summary>
    /// Fetched-lease counts grouped by queue, computed in a single pass. The dashboard lists every
    /// queue with its fetched count on each render; calling <see cref="GetFetchedCount"/> per queue
    /// would re-scan all leases once per queue (O(queues x leases)), so the monitoring API uses this.
    /// </summary>
    public Dictionary<string, long> GetFetchedCountsByQueue()
    {
        lock (_sync)
        {
            var result = new Dictionary<string, long>(StringComparer.Ordinal);
            foreach (var fetched in _fetched.Values)
                result[fetched.Queue] = result.TryGetValue(fetched.Queue, out var c) ? c + 1 : 1;
            return result;
        }
    }

    public List<ServerSnapshot> GetServers()
    {
        lock (_sync)
        {
            return _servers
                .OrderBy(kv => kv.Key, StringComparer.Ordinal)
                .Select(kv => new ServerSnapshot(kv.Key, kv.Value.WorkerCount, kv.Value.Queues, kv.Value.StartedAt, kv.Value.LastHeartbeat))
                .ToList();
        }
    }

    public List<JobSnapshot> GetJobsByState(string stateName, int from, int count, bool ascending)
    {
        lock (_sync)
        {
            if (!_jobsByState.TryGetValue(stateName, out var index)) return [];
            var source = ascending ? index.AsEnumerable() : index.Reverse();
            return source.Skip(from).Take(count).Select(Snapshot).ToList();
        }
    }

    public long GetStateCount(string stateName)
    {
        lock (_sync)
        {
            return _jobsByState.TryGetValue(stateName, out var index) ? index.Count : 0;
        }
    }

    public StatisticsSnapshot GetStatistics()
    {
        lock (_sync)
        {
            return new StatisticsSnapshot(
                Servers: _servers.Count,
                Queues: _queues.Count,
                Enqueued: _queues.Values.Sum(q => (long)q.Count),
                Fetched: _fetched.Count,
                // State names come from Hangfire's own constants so a typo is a compile error. The
                // counter and set keys below ("stats:succeeded", "recurring-jobs", ...) have no public
                // constant and mirror the literals baked into Hangfire core, matched verbatim.
                Scheduled: StateCount(ScheduledState.StateName),
                Processing: StateCount(ProcessingState.StateName),
                Failed: StateCount(FailedState.StateName),
                Awaiting: StateCount(AwaitingState.StateName),
                Succeeded: _counters.TryGetValue("stats:succeeded", out var s) ? s.Value : 0,
                Deleted: _counters.TryGetValue("stats:deleted", out var d) ? d.Value : 0,
                Recurring: _sets.TryGetValue("recurring-jobs", out var r) ? r.Scores.Count : 0,
                Retries: _sets.TryGetValue("retries", out var rt) ? rt.Scores.Count : 0);

            long StateCount(string name) => _jobsByState.TryGetValue(name, out var index) ? index.Count : 0;
        }
    }

    private static JobSnapshot Snapshot(JobEntry job) => new(
        job.Id,
        job.InvocationData,
        job.CreatedAt,
        job.ExpireAt,
        new Dictionary<string, string?>(job.Parameters),
        job.CurrentState,
        [.. job.History]);
}

internal sealed record StatisticsSnapshot(
    long Servers,
    long Queues,
    long Enqueued,
    long Fetched,
    long Scheduled,
    long Processing,
    long Failed,
    long Awaiting,
    long Succeeded,
    long Deleted,
    long Recurring,
    long Retries);
