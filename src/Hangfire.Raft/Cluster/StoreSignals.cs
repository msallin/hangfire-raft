namespace Hangfire.Raft.Cluster;

/// <summary>
/// In-process wake-up signals pulsed after the local state machine applies a command. Fetch loops
/// wait on queue signals instead of polling tightly; lock waiters wake on lock releases. Signals
/// are advisory: a missed pulse only delays the next poll, never loses data.
/// </summary>
internal sealed class StoreSignals
{
    private readonly object _sync = new();
    private readonly Dictionary<string, PulseSource> _queues = [];
    private readonly PulseSource _locks = new();

    /// <summary>Returns a task that completes when any of the given queues receives a pulse.</summary>
    public Task WaitForQueues(IReadOnlyList<string> queues)
    {
        lock (_sync)
        {
            var waits = new Task[queues.Count];
            for (var i = 0; i < queues.Count; i++)
            {
                if (!_queues.TryGetValue(queues[i], out var source)) _queues[queues[i]] = source = new PulseSource();
                waits[i] = source.Wait;
            }
            return Task.WhenAny(waits);
        }
    }

    public Task LockReleased
    {
        get
        {
            lock (_sync)
            {
                return _locks.Wait;
            }
        }
    }

    public void PulseQueues(IEnumerable<string> queues)
    {
        lock (_sync)
        {
            foreach (var queue in queues)
            {
                if (_queues.TryGetValue(queue, out var source)) source.Pulse();
            }
        }
    }

    public void PulseLocks()
    {
        lock (_sync)
        {
            _locks.Pulse();
        }
    }

    private sealed class PulseSource
    {
        private TaskCompletionSource _tcs = NewSource();

        public Task Wait => _tcs.Task;

        public void Pulse()
        {
            var previous = Interlocked.Exchange(ref _tcs, NewSource());
            previous.TrySetResult();
        }

        private static TaskCompletionSource NewSource() => new(TaskCreationOptions.RunContinuationsAsynchronously);
    }
}
