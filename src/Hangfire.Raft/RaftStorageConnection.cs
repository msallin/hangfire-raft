using Hangfire.Common;
using Hangfire.Raft.Cluster;
using Hangfire.Raft.Commands;
using Hangfire.Server;
using Hangfire.Storage;

namespace Hangfire.Raft;

/// <summary>
/// Hangfire connection over the Raft cluster. Writes are replicated commands; reads are served
/// from the local store, which the submit pipeline keeps read-your-writes consistent. Connections
/// are cheap: they share the storage's cluster and hold no resources themselves.
/// </summary>
internal sealed class RaftStorageConnection(RaftJobStorage storage) : JobStorageConnection
{
    private RaftStorageCluster Cluster => storage.Cluster;

    public override IWriteOnlyTransaction CreateWriteTransaction() => new RaftWriteOnlyTransaction(storage);

    public override IDisposable AcquireDistributedLock(string resource, TimeSpan timeout)
    {
        ArgumentException.ThrowIfNullOrEmpty(resource);
        return RaftDistributedLock.Acquire(Cluster, resource, timeout);
    }

    public override string CreateExpiredJob(Job job, IDictionary<string, string> parameters, DateTime createdAt, TimeSpan expireIn)
    {
        ArgumentNullException.ThrowIfNull(job);
        ArgumentNullException.ThrowIfNull(parameters);

        var jobId = Guid.NewGuid().ToString("n");
        var payload = InvocationData.SerializeJob(job).SerializePayload();
        var parameterList = parameters.Select(p => new KeyValuePair<string, string?>(p.Key, p.Value)).ToList();

        Cluster.Submit(Command.Single(new CreateJobOp(jobId, payload, parameterList, createdAt, createdAt + expireIn)));
        return jobId;
    }

    public override IFetchedJob FetchNextJob(string[] queues, CancellationToken cancellationToken)
    {
        if (queues is null || queues.Length == 0) throw new ArgumentException("At least one queue is required.", nameof(queues));

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            // Capture the wait BEFORE probing, so an enqueue that lands between the probe and the
            // wait still pulses this task instead of being lost.
            var enqueued = Cluster.Signals.WaitForQueues(queues);

            // Probe and fetch under the per-node gate so a single enqueue does not fan out into one
            // consensus round per idle worker. The probe must be inside the gate: once the winner
            // has taken the only job, the next worker re-probes an empty queue and skips the submit.
            Cluster.FetchGate.Wait(cancellationToken);
            try
            {
                if (Cluster.Store.HasQueuedJob(queues))
                {
                    var token = Guid.NewGuid();
                    var result = Cluster.Submit(Command.Single(new FetchOp(queues, token)), cancellationToken);
                    if (result is FetchResult fetched)
                        return new RaftFetchedJob(Cluster, token, fetched.JobId, fetched.Queue);
                }
            }
            finally
            {
                Cluster.FetchGate.Release();
            }

            // Wait OUTSIDE the gate so an idle worker does not block others. The 1s cap covers remote
            // enqueues this node has not applied yet and fetch races.
            try
            {
                enqueued.Wait(TimeSpan.FromSeconds(1), cancellationToken);
            }
            catch (OperationCanceledException)
            {
                cancellationToken.ThrowIfCancellationRequested();
                throw;
            }
        }
    }

    public override void SetJobParameter(string id, string name, string value)
    {
        ArgumentException.ThrowIfNullOrEmpty(id);
        ArgumentException.ThrowIfNullOrEmpty(name);
        Cluster.Submit(Command.Single(new SetJobParameterOp(id, name, value)));
    }

    public override string? GetJobParameter(string id, string name)
    {
        ArgumentException.ThrowIfNullOrEmpty(id);
        ArgumentException.ThrowIfNullOrEmpty(name);
        return Cluster.Store.GetJobParameter(id, name);
    }

    public override JobData? GetJobData(string jobId)
    {
        ArgumentException.ThrowIfNullOrEmpty(jobId);
        var snapshot = Cluster.Store.GetJob(jobId);
        if (snapshot is null) return null;

        var data = new JobData
        {
            State = snapshot.CurrentState?.Name,
            CreatedAt = snapshot.CreatedAt,
        };

        try
        {
            data.Job = InvocationData.DeserializePayload(snapshot.InvocationData).DeserializeJob();
        }
        catch (JobLoadException ex)
        {
            data.LoadException = ex;
        }

        return data;
    }

    public override StateData? GetStateData(string jobId)
    {
        ArgumentException.ThrowIfNullOrEmpty(jobId);
        var state = Cluster.Store.GetJob(jobId)?.CurrentState;
        if (state is null) return null;

        // Case-insensitive (like SQL Server storage) and last-wins on a duplicate key, matching the
        // dashboard's RaftMonitoringApi reader so the two paths cannot disagree on the same data.
        var data = new Dictionary<string, string>(state.Data.Count, StringComparer.OrdinalIgnoreCase);
        foreach (var (k, v) in state.Data) data[k] = v!;

        return new StateData
        {
            Name = state.Name,
            Reason = state.Reason,
            Data = data,
        };
    }

    public override void AnnounceServer(string serverId, ServerContext context)
    {
        ArgumentException.ThrowIfNullOrEmpty(serverId);
        ArgumentNullException.ThrowIfNull(context);
        Cluster.Submit(Command.Single(new AnnounceServerOp(serverId, context.WorkerCount, context.Queues ?? [])));
    }

    public override void RemoveServer(string serverId)
    {
        ArgumentException.ThrowIfNullOrEmpty(serverId);
        Cluster.Submit(Command.Single(new RemoveServerOp(serverId)));
    }

    public override void Heartbeat(string serverId)
    {
        ArgumentException.ThrowIfNullOrEmpty(serverId);
        var known = (bool)Cluster.Submit(Command.Single(new HeartbeatOp(serverId)))!;
        if (!known) throw new BackgroundServerGoneException();
    }

    public override int RemoveTimedOutServers(TimeSpan timeOut)
    {
        if (timeOut < TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(timeOut), "The timeout must be non-negative.");
        return (int)Cluster.Submit(Command.Single(new RemoveTimedOutServersOp(timeOut)))!;
    }

    // ----- sets -----

    public override HashSet<string> GetAllItemsFromSet(string key)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        return Cluster.Store.GetAllItemsFromSet(key);
    }

    public override string? GetFirstByLowestScoreFromSet(string key, double fromScore, double toScore)
        => GetFirstByLowestScoreFromSet(key, fromScore, toScore, 1).FirstOrDefault();

    public override List<string> GetFirstByLowestScoreFromSet(string key, double fromScore, double toScore, int count)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        if (count <= 0) throw new ArgumentException("The count must be positive.", nameof(count));
        if (fromScore > toScore) throw new ArgumentException("The `toScore` value must be higher or equal to the `fromScore` value.", nameof(toScore));
        return Cluster.Store.GetFirstByLowestScoreFromSet(key, fromScore, toScore, count);
    }

    public override long GetSetCount(string key)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        return Cluster.Store.GetSetCount(key);
    }

    public override long GetSetCount(IEnumerable<string> keys, int limit)
    {
        ArgumentNullException.ThrowIfNull(keys);
        if (limit < 0) throw new ArgumentOutOfRangeException(nameof(limit), "The limit must be non-negative.");
        return Cluster.Store.GetSetCount(keys, limit);
    }

    public override bool GetSetContains(string key, string value)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        return Cluster.Store.GetSetContains(key, value);
    }

    public override List<string> GetRangeFromSet(string key, int startingFrom, int endingAt)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        return Cluster.Store.GetRangeFromSet(key, startingFrom, endingAt);
    }

    public override TimeSpan GetSetTtl(string key)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        return Cluster.Store.GetSetTtl(key, DateTime.UtcNow);
    }

    // ----- counters -----

    public override long GetCounter(string key)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        return Cluster.Store.GetCounter(key);
    }

    // ----- hashes -----

    public override void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        ArgumentNullException.ThrowIfNull(keyValuePairs);
        var fields = keyValuePairs.Select(p => new KeyValuePair<string, string?>(p.Key, p.Value)).ToList();
        Cluster.Submit(Command.Single(new SetRangeInHashOp(key, fields)));
    }

    public override Dictionary<string, string>? GetAllEntriesFromHash(string key)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        var fields = Cluster.Store.GetAllEntriesFromHash(key);
        // Hangfire's un-annotated contract allows null values inside the dictionary.
        return fields?.ToDictionary(p => p.Key, p => p.Value!);
    }

    public override long GetHashCount(string key)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        return Cluster.Store.GetHashCount(key);
    }

    public override TimeSpan GetHashTtl(string key)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        return Cluster.Store.GetHashTtl(key, DateTime.UtcNow);
    }

    public override string? GetValueFromHash(string key, string name)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        ArgumentNullException.ThrowIfNull(name);
        return Cluster.Store.GetValueFromHash(key, name);
    }

    // ----- lists -----

    public override List<string> GetAllItemsFromList(string key)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        return Cluster.Store.GetAllItemsFromList(key);
    }

    public override long GetListCount(string key)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        return Cluster.Store.GetListCount(key);
    }

    public override TimeSpan GetListTtl(string key)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        return Cluster.Store.GetListTtl(key, DateTime.UtcNow);
    }

    public override List<string> GetRangeFromList(string key, int startingFrom, int endingAt)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        return Cluster.Store.GetRangeFromList(key, startingFrom, endingAt);
    }
}
