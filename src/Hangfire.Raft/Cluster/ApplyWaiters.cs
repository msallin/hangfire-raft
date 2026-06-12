using System.Collections.Concurrent;

namespace Hangfire.Raft.Cluster;

/// <summary>
/// Correlates submitted commands with their local apply. The submitting node registers a waiter
/// under the command id before replication; when the local state machine applies that entry it
/// completes the waiter with the op result. Commands applied without a registered waiter (entries
/// originating from other nodes, or log replay) complete nobody, which is the normal case.
/// </summary>
internal sealed class ApplyWaiters
{
    private readonly ConcurrentDictionary<Guid, TaskCompletionSource<object?>> _waiters = new();

    public Task<object?> Register(Guid commandId)
    {
        var tcs = new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously);
        if (!_waiters.TryAdd(commandId, tcs)) throw new InvalidOperationException($"Duplicate command id {commandId}.");
        return tcs.Task;
    }

    public void Remove(Guid commandId) => _waiters.TryRemove(commandId, out _);

    public void Complete(Guid commandId, object? result)
    {
        if (_waiters.TryRemove(commandId, out var tcs)) tcs.TrySetResult(result);
    }
}
