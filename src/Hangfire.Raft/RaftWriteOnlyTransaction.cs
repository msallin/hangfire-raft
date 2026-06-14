using Hangfire.Common;
using Hangfire.Raft.Commands;
using Hangfire.States;
using Hangfire.Storage;

namespace Hangfire.Raft;

/// <summary>
/// Collects ops and replicates them as a single command on <see cref="Commit"/>, so the whole
/// transaction applies atomically on every node or not at all.
/// </summary>
internal sealed class RaftWriteOnlyTransaction(RaftJobStorage storage) : JobStorageTransaction
{
    private readonly List<StoreOp> _ops = [];
    private bool _committed;

    /// <summary>The ops queued so far. Exposed for tests to assert the Hangfire-call-to-StoreOp mapping without a round-trip.</summary>
    internal IReadOnlyList<StoreOp> PendingOps => _ops;

    public override void Commit()
    {
        if (_committed) throw new InvalidOperationException("The transaction was already committed.");
        _committed = true;
        if (_ops.Count == 0) return;
        storage.Cluster.Submit(Command.Batch(_ops.ToArray()));
    }

    // ----- jobs -----

    public override string CreateJob(Job job, IDictionary<string, string> parameters, DateTime createdAt, TimeSpan expireIn)
    {
        ArgumentNullException.ThrowIfNull(job);
        ArgumentNullException.ThrowIfNull(parameters);

        var jobId = Guid.NewGuid().ToString("n");
        var payload = InvocationData.SerializeJob(job).SerializePayload();
        var parameterList = parameters.Select(p => new KeyValuePair<string, string?>(p.Key, p.Value)).ToList();
        _ops.Add(new CreateJobOp(jobId, payload, parameterList, createdAt, createdAt + expireIn));
        return jobId;
    }

    public override void SetJobParameter(string jobId, string name, string value)
    {
        ArgumentException.ThrowIfNullOrEmpty(jobId);
        ArgumentException.ThrowIfNullOrEmpty(name);
        _ops.Add(new SetJobParameterOp(jobId, name, value));
    }

    public override void SetJobState(string jobId, IState state)
    {
        ArgumentException.ThrowIfNullOrEmpty(jobId);
        ArgumentNullException.ThrowIfNull(state);
        _ops.Add(new SetJobStateOp(jobId, ToRecord(state)));
    }

    public override void AddJobState(string jobId, IState state)
    {
        ArgumentException.ThrowIfNullOrEmpty(jobId);
        ArgumentNullException.ThrowIfNull(state);
        _ops.Add(new AddJobStateOp(jobId, ToRecord(state)));
    }

    public override void ExpireJob(string jobId, TimeSpan expireIn)
    {
        ArgumentException.ThrowIfNullOrEmpty(jobId);
        _ops.Add(new ExpireJobOp(jobId, DateTime.UtcNow + expireIn));
    }

    public override void PersistJob(string jobId)
    {
        ArgumentException.ThrowIfNullOrEmpty(jobId);
        _ops.Add(new PersistJobOp(jobId));
    }

    public override void AddToQueue(string queue, string jobId)
    {
        ArgumentException.ThrowIfNullOrEmpty(queue);
        ArgumentException.ThrowIfNullOrEmpty(jobId);
        _ops.Add(new EnqueueOp(queue, jobId));
    }

    // ----- counters -----

    public override void IncrementCounter(string key)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        _ops.Add(new IncrementCounterOp(key, 1, null));
    }

    public override void IncrementCounter(string key, TimeSpan expireIn)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        _ops.Add(new IncrementCounterOp(key, 1, DateTime.UtcNow + expireIn));
    }

    public override void DecrementCounter(string key)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        _ops.Add(new IncrementCounterOp(key, -1, null));
    }

    public override void DecrementCounter(string key, TimeSpan expireIn)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        _ops.Add(new IncrementCounterOp(key, -1, DateTime.UtcNow + expireIn));
    }

    // ----- sets -----

    public override void AddToSet(string key, string value) => AddToSet(key, value, 0.0d);

    public override void AddToSet(string key, string value, double score)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        ArgumentNullException.ThrowIfNull(value);
        _ops.Add(new AddToSetOp(key, value, score));
    }

    public override void AddRangeToSet(string key, IList<string> items)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        ArgumentNullException.ThrowIfNull(items);
        _ops.Add(new AddRangeToSetOp(key, items.ToArray()));
    }

    public override void RemoveFromSet(string key, string value)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        ArgumentNullException.ThrowIfNull(value);
        _ops.Add(new RemoveFromSetOp(key, value));
    }

    public override void RemoveSet(string key)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        _ops.Add(new RemoveSetOp(key));
    }

    public override void ExpireSet(string key, TimeSpan expireIn)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        _ops.Add(new ExpireSetOp(key, DateTime.UtcNow + expireIn));
    }

    public override void PersistSet(string key)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        _ops.Add(new PersistSetOp(key));
    }

    // ----- lists -----

    public override void InsertToList(string key, string value)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        ArgumentNullException.ThrowIfNull(value);
        _ops.Add(new InsertToListOp(key, value));
    }

    public override void RemoveFromList(string key, string value)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        ArgumentNullException.ThrowIfNull(value);
        _ops.Add(new RemoveFromListOp(key, value));
    }

    public override void TrimList(string key, int keepStartingFrom, int keepEndingAt)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        _ops.Add(new TrimListOp(key, keepStartingFrom, keepEndingAt));
    }

    public override void ExpireList(string key, TimeSpan expireIn)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        _ops.Add(new ExpireListOp(key, DateTime.UtcNow + expireIn));
    }

    public override void PersistList(string key)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        _ops.Add(new PersistListOp(key));
    }

    // ----- hashes -----

    public override void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        ArgumentNullException.ThrowIfNull(keyValuePairs);
        var fields = keyValuePairs.Select(p => new KeyValuePair<string, string?>(p.Key, p.Value)).ToList();
        _ops.Add(new SetRangeInHashOp(key, fields));
    }

    public override void RemoveHash(string key)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        _ops.Add(new RemoveHashOp(key));
    }

    public override void ExpireHash(string key, TimeSpan expireIn)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        _ops.Add(new ExpireHashOp(key, DateTime.UtcNow + expireIn));
    }

    public override void PersistHash(string key)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        _ops.Add(new PersistHashOp(key));
    }

    private static StateRecord ToRecord(IState state)
    {
        var data = state.SerializeData();
        var pairs = data is null
            ? Array.Empty<KeyValuePair<string, string?>>()
            : data.Select(p => new KeyValuePair<string, string?>(p.Key, p.Value)).ToArray();
        return new StateRecord(state.Name, state.Reason, pairs, DateTime.UtcNow);
    }
}
