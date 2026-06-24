using System.Buffers;
using System.Text;
using DotNext.IO;
using DotNext.Net.Cluster.Consensus.Raft.StateMachine;
using Hangfire.Raft.Commands;
using Hangfire.Raft.State;
using Microsoft.Extensions.Logging;

namespace Hangfire.Raft.Cluster;

/// <summary>
/// The Raft state machine: each committed log entry is deserialized into a command and applied to the
/// in-memory <see cref="RaftStore"/>; the whole store is persisted as a snapshot for log compaction
/// and follower catch-up, and restored on startup. DotNext's <see cref="WriteAheadLog"/> drives apply,
/// snapshot and restore; this type supplies the Hangfire-specific (de)serialization and apply logic and
/// keeps the full dataset in memory (the <see cref="SimpleStateMachine"/> model).
/// </summary>
internal sealed class HangfireStateMachine : SimpleStateMachine
{
    private readonly ILogger _logger;
    private readonly long _snapshotInterval;
    private volatile bool _faulted;

    public RaftStore Store { get; } = new();
    public ApplyWaiters Waiters { get; } = new();
    public StoreSignals Signals { get; } = new();

    /// <summary>
    /// True once an apply or snapshot restore has thrown. The node cannot safely make progress after that
    /// (it is behind the committed log, or has no valid base state), so health probes should report it
    /// unhealthy and let the orchestrator restart or replace the node.
    /// </summary>
    public bool IsFaulted => _faulted;

    /// <param name="location">Directory holding the state-machine snapshots.</param>
    /// <param name="snapshotInterval">Take a snapshot every this many applied entries, to keep the log compacted.</param>
    /// <param name="logger">Diagnostics sink.</param>
    public HangfireStateMachine(DirectoryInfo location, long snapshotInterval, ILogger logger)
        : base(location)
    {
        _logger = logger;
        _snapshotInterval = snapshotInterval;
    }

    /// <summary>
    /// Applies one committed command to the store and signals the submitting waiter. An undecodable
    /// committed entry is treated as a fault, not skipped: the entry is already committed, so dropping it
    /// would leave this replica permanently behind the log (stale reads, divergent snapshots). Failing
    /// loudly lets the node re-sync from the leader instead. The boolean result asks the log to snapshot
    /// periodically so the WAL stays compacted.
    /// </summary>
    protected override ValueTask<bool> ApplyAsync(LogEntry entry, CancellationToken token)
    {
        if (entry.TryGetPayload(out var payload))
        {
            Command? command;
            try
            {
                command = CommandSerializer.TryDeserialize(payload.IsSingleSegment ? payload.First : payload.ToArray());
            }
            catch (Exception ex)
            {
                // A committed entry that carries our magic but cannot be decoded means this node's copy of
                // an already-agreed entry is unusable (local on-disk corruption, or a node on an older build
                // replaying an op a newer one wrote). Skipping it is NOT safe: other nodes applied it, so
                // dropping it here leaves this replica behind the log and folds the divergence into its next
                // snapshot. Fail loudly instead. Recovery is PER NODE, one at a time: clear THIS node's WAL
                // so it re-syncs from the leader's snapshot; never clear every node's WAL at once. The leader
                // pre-validates forwarded commands (see HandleForwardedCommand) and authors its own entries
                // from round-trip-tested commands, so a healthy same-version cluster never reaches here.
                _logger.LogError(ex, "Failed to decode the committed log entry at index {Index}; the on-disk state is corrupt or from an incompatible version.", entry.Index);
                _faulted = true;
                throw new RaftStorageException($"Failed to decode the committed Raft log entry at index {entry.Index}; the on-disk state may be corrupt or from an incompatible version.", ex);
            }

            if (command is not null)
            {
                try
                {
                    var effects = Store.Apply(command);
                    Waiters.Complete(command.Id, effects.Result);
                    if (effects.SignaledQueues is { } queues) Signals.PulseQueues(queues);
                    if (effects.LocksReleased) Signals.PulseLocks();
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    // Applying a committed entry must not throw (Store.Apply is deterministic in-memory
                    // mutation). If it does, this replica is now behind the committed log just like the
                    // decode failure above, so fault the node instead of letting the exception escape while
                    // the health probe still reports healthy.
                    _logger.LogError(ex, "Failed to apply the committed log entry at index {Index}.", entry.Index);
                    _faulted = true;
                    throw new RaftStorageException($"Failed to apply the committed Raft log entry at index {entry.Index}.", ex);
                }
            }
        }

        return new(ShouldSnapshot(entry.Index));
    }

    private bool ShouldSnapshot(long index) => _snapshotInterval > 0 && index % _snapshotInterval == 0;

    /// <summary>Serializes the whole store as the snapshot payload.</summary>
    protected override async ValueTask PersistAsync(IAsyncBinaryWriter writer, CancellationToken token)
    {
        byte[] snapshot;
        using (var buffer = new MemoryStream())
        {
            using (var w = new BinaryWriter(buffer, Encoding.UTF8, leaveOpen: true)) Store.WriteSnapshot(w);
            snapshot = buffer.ToArray();
        }

        await writer.Invoke(snapshot, token).ConfigureAwait(false);
    }

    /// <summary>
    /// Rebuilds the store from a snapshot file on startup or follower install. Like the undecodable
    /// command path above, a corrupt snapshot is fatal rather than skipped: it is the base state, so
    /// failing loudly with a typed error beats bringing the node up with missing data.
    /// </summary>
    protected override async ValueTask RestoreAsync(FileInfo snapshotFile, CancellationToken token)
    {
        try
        {
            var snapshot = await File.ReadAllBytesAsync(snapshotFile.FullName, token).ConfigureAwait(false);
            using var reader = new BinaryReader(new MemoryStream(snapshot, writable: false), Encoding.UTF8);
            Store.LoadSnapshot(reader);

            // A restore-from-snapshot boot (or a follower installing a leader-pushed snapshot) is otherwise
            // indistinguishable in the logs from a fresh empty start; record it so an operator can confirm
            // how much state the node came up on.
            _logger.LogInformation("Restored state-machine snapshot from {File} ({Bytes} bytes).", snapshotFile.FullName, snapshot.Length);
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
            throw; // a cancelled restore (shutdown or aborted startup) is not on-disk corruption
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to load the snapshot at {File}; the on-disk state is corrupt or from an incompatible version.", snapshotFile.FullName);
            _faulted = true;
            throw new RaftStorageException($"Failed to load the Raft snapshot at {snapshotFile.FullName}; the on-disk state may be corrupt.", ex);
        }
    }
}
