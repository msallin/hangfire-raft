using Hangfire.Raft.Cluster;
using Hangfire.Raft.Commands;
using Hangfire.Storage;
using Microsoft.Extensions.Logging;

namespace Hangfire.Raft;

/// <summary>
/// A dequeued job held under a fetch lease. While the worker processes the job, a background timer
/// renews the lease; if the process dies, the lease expires and maintenance requeues the job after
/// the invisibility timeout. Acknowledging (RemoveFromQueue) deletes the lease, Requeue puts the
/// job back at the head of its queue. Disposing without an explicit outcome requeues, matching the
/// behavior Hangfire workers expect on processing failure.
/// </summary>
internal sealed class RaftFetchedJob : IFetchedJob
{
    private readonly RaftStorageCluster _cluster;
    private readonly Guid _token;
    private readonly Timer _renewal;
    // volatile: written on the worker thread (RemoveFromQueue/Requeue), read on the timer thread (Renew).
    private volatile bool _completed;
    private volatile bool _disposed;
    private int _renewing;

    public RaftFetchedJob(RaftStorageCluster cluster, Guid token, string jobId, string queue)
    {
        _cluster = cluster;
        _token = token;
        JobId = jobId;
        Queue = queue;

        var period = TimeSpan.FromTicks(cluster.Options.FetchInvisibilityTimeout.Ticks / 3);
        _renewal = new Timer(Renew, null, period, period);
    }

    public string JobId { get; }

    public string Queue { get; }

    public void RemoveFromQueue()
    {
        _renewal.Change(Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
        _cluster.Submit(Command.Single(new AckFetchedOp(_token)));
        _completed = true;
    }

    public void Requeue()
    {
        _renewal.Change(Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
        _cluster.Submit(Command.Single(new RequeueFetchedOp(_token)));
        _completed = true;
    }

    private void Renew(object? state)
    {
        // Skip when completed/disposed or when the previous renewal is still in flight, then run the
        // submit asynchronously so the timer callback never blocks a thread-pool thread for up to
        // SubmitTimeout (which, multiplied by every in-flight lease during an outage, would starve
        // the pool). The _renewing guard is cleared in RenewAsync's finally.
        if (_disposed || _completed || Interlocked.Exchange(ref _renewing, 1) == 1) return;
        _ = RenewAsync();
    }

    private async Task RenewAsync()
    {
        try
        {
            var renewed = (bool)(await _cluster.SubmitAsync(Command.Single(new RenewFetchedOp(_token))).ConfigureAwait(false))!;
            if (!renewed)
            {
                // Maintenance already reclaimed the lease (e.g. after a long stall): the job will
                // run again elsewhere. Stop renewing; this worker's eventual ack becomes a no-op.
                _renewal.Change(Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
                _cluster.Logger.LogWarning("Fetch lease for job {JobId} expired and was reclaimed; the job may run a second time.", JobId);
            }
        }
        catch (Exception ex)
        {
            // Transient cluster trouble: the next timer tick retries; if renewal keeps failing the
            // lease expires and the job is requeued, which is the safe outcome. Logged so duplicate
            // executions after an outage can be correlated to the renewal failures.
            _cluster.Logger.LogWarning(ex, "Failed to renew the fetch lease for job {JobId}; it will be retried.", JobId);
        }
        finally
        {
            Interlocked.Exchange(ref _renewing, 0);
        }
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _renewal.Dispose();

        // No need to drain an in-flight async renewal here (unlike the distributed lock): RenewFetchedOp
        // is update-only, so once Requeue/RemoveFromQueue removes the lease a late renewal is a no-op
        // and cannot resurrect it. Either commit order leaves the job requeued with no lingering lease.
        if (!_completed)
        {
            try
            {
                Requeue();
            }
            catch (Exception)
            {
                // The invisibility timeout reclaims the job if the requeue cannot reach the cluster.
            }
        }
    }
}
