using Hangfire.Raft.Cluster;
using Hangfire.Raft.Commands;
using Hangfire.Storage;
using Microsoft.Extensions.Logging;

namespace Hangfire.Raft;

/// <summary>
/// Cluster-wide lock backed by a replicated lease. Acquisition retries until the configured
/// timeout, waking early when any lock is released. The holder renews the lease at a third of the
/// lease duration; a crashed holder's lease simply expires, so locks cannot leak.
/// </summary>
internal sealed class RaftDistributedLock : IDisposable
{
    private readonly RaftStorageCluster _cluster;
    private readonly string _resource;
    private readonly Guid _owner;
    private readonly Timer _renewal;
    private volatile bool _disposed;
    private int _renewing;

    // The most recently started async renewal, so Dispose can wait for it to fully commit before
    // releasing the lock. Published from the timer callback and read after the timer is drained.
    private Task _renewalInFlight = Task.CompletedTask;

    private RaftDistributedLock(RaftStorageCluster cluster, string resource, Guid owner)
    {
        _cluster = cluster;
        _resource = resource;
        _owner = owner;

        var period = TimeSpan.FromTicks(cluster.Options.LockLeaseTimeout.Ticks / 3);
        _renewal = new Timer(Renew, null, period, period);
    }

    public static RaftDistributedLock Acquire(RaftStorageCluster cluster, string resource, TimeSpan timeout)
    {
        var owner = Guid.NewGuid();
        var deadline = DateTime.UtcNow + timeout;

        while (true)
        {
            var released = cluster.Signals.LockReleased; // capture before the attempt to avoid missing a release
            var acquired = (bool)cluster.Submit(Command.Single(new TryAcquireLockOp(resource, owner, cluster.Options.LockLeaseTimeout)))!;
            if (acquired) return new RaftDistributedLock(cluster, resource, owner);

            var remaining = deadline - DateTime.UtcNow;
            if (remaining <= TimeSpan.Zero) throw new DistributedLockTimeoutException(resource);

            var wait = remaining < TimeSpan.FromMilliseconds(200) ? remaining : TimeSpan.FromMilliseconds(200);
            released.Wait(wait);
        }
    }

    private void Renew(object? state)
    {
        // Skip when disposed or when the previous renewal is still in flight, then run the submit
        // asynchronously so the timer callback never blocks a thread-pool thread for up to
        // SubmitTimeout. The _renewing guard is cleared in RenewAsync's finally. The task is
        // published so Dispose can wait it out before releasing.
        if (_disposed || Interlocked.Exchange(ref _renewing, 1) == 1) return;
        _renewalInFlight = RenewAsync();
    }

    private async Task RenewAsync()
    {
        try
        {
            var renewed = (bool)(await _cluster.SubmitAsync(Command.Single(new TryAcquireLockOp(_resource, _owner, _cluster.Options.LockLeaseTimeout))).ConfigureAwait(false))!;
            if (!renewed)
            {
                // Another owner holds the lock now (our lease expired during an outage). Mutual
                // exclusion is already broken for this holder; stop renewing so we do not steal
                // the lock back when the new owner releases it.
                _renewal.Change(Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
                _cluster.Logger.LogWarning("Distributed lock '{Resource}' was lost to another owner; renewal stopped.", _resource);
            }
        }
        catch (Exception ex)
        {
            // Transient cluster trouble: the next tick retries; if renewal keeps failing the lease
            // expires, which is the documented behavior for a holder that lost the cluster. Logged
            // so a holder running without a confirmed lease is visible during the outage window.
            _cluster.Logger.LogWarning(ex, "Failed to renew distributed lock '{Resource}'; it will be retried.", _resource);
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

        // Wait for any in-flight renewal to fully commit before releasing. TryAcquireLockOp is an
        // upsert, so a renewal that committed AFTER the release would re-create the lock for this
        // (now dead) owner and hold it until the lease expires. Draining the timer alone is not
        // enough now that the renewal is async: the timer callback returns immediately, so we first
        // wait for the running callback to publish its task, then wait for that task. Bounded by
        // SubmitTimeout in the worst case.
        using (var drained = new ManualResetEvent(false))
        {
            if (_renewal.Dispose(drained)) drained.WaitOne();
        }

        try
        {
            _renewalInFlight.Wait();
        }
        catch (Exception)
        {
            // RenewAsync handles and logs its own failures; its task never faults, but guard anyway.
        }

        try
        {
            _cluster.Submit(Command.Single(new ReleaseLockOp(_resource, _owner)));
        }
        catch (Exception)
        {
            // Lease expiry releases the lock if the cluster is unreachable right now.
        }
    }
}
