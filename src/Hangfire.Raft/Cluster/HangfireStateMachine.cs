using System.Text;
using DotNext.IO;
using DotNext.Net.Cluster.Consensus.Raft;
using Hangfire.Raft.Commands;
using Hangfire.Raft.State;
using Microsoft.Extensions.Logging;

namespace Hangfire.Raft.Cluster;

/// <summary>
/// DotNext write-ahead log + state machine. Committed entries are deserialized into commands and
/// applied to the live <see cref="RaftStore"/>; snapshots serialize the full store for log
/// compaction and follower catch-up. Apply is also invoked during startup replay, which is how a
/// restarted node recovers its state from disk.
/// </summary>
internal sealed class HangfireStateMachine : MemoryBasedStateMachine
{
    private readonly ILogger _logger;

    public RaftStore Store { get; } = new();
    public ApplyWaiters Waiters { get; } = new();
    public StoreSignals Signals { get; } = new();

    public HangfireStateMachine(string path, int recordsPerPartition, ILogger logger)
        : base(path, recordsPerPartition, CreateOptions())
    {
        _logger = logger;
    }

    private static Options CreateOptions() => new()
    {
        CompactionMode = CompactionMode.Sequential,
        UseCaching = true,
        // Replays the snapshot + committed log into the store during startup, which is how a
        // restarted node recovers its state before rejoining the cluster.
        ReplayOnInitialize = true,
    };

    public BinaryLogEntry CreateCommandEntry(ReadOnlyMemory<byte> payload) => CreateBinaryLogEntry(payload);

    protected override async ValueTask ApplyAsync(LogEntry entry)
    {
        if (entry.IsSnapshot)
        {
            var snapshot = await ReadPayload(entry).ConfigureAwait(false);
            try
            {
                using var reader = new BinaryReader(BinaryFormat.CreateReadStream(snapshot), Encoding.UTF8);
                Store.LoadSnapshot(reader);
            }
            catch (Exception ex)
            {
                // Unlike an undecodable command (skipped below), a corrupt snapshot cannot be skipped:
                // the snapshot IS the base state, so continuing would bring the node up with silently
                // missing/wrong data. Fail loudly with a typed, logged error instead of letting a raw
                // reader exception (EndOfStream/InvalidData/NotSupported) fault the pipeline opaquely.
                // Recovery is operational: restore or clear the WAL directory so the node re-syncs from
                // the leader.
                _logger.LogError(ex, "Failed to load the committed snapshot at index {Index}; the on-disk state is corrupt or from an incompatible version.", entry.Index);
                throw new RaftStorageException($"Failed to load the Raft snapshot at index {entry.Index}; the on-disk state may be corrupt.", ex);
            }

            return;
        }

        if (entry.Length == 0) return;

        var payload = await ReadPayload(entry).ConfigureAwait(false);

        Command? command;
        try
        {
            command = CommandSerializer.TryDeserialize(payload);
        }
        catch (Exception ex)
        {
            // A committed entry that carries our magic but cannot be decoded (corruption on disk, or
            // an op from a newer command format reaching an older node during a version-skewed
            // upgrade) must NOT fault the apply pipeline: that would crash every node and, via replay
            // on restart, brick the whole cluster. The decode result is a deterministic function of
            // the bytes, so every node skips the same entry identically. The leader also rejects
            // undecodable commands before append (see HandleForwardedCommand), so reaching here means
            // either local corruption or genuine version skew.
            _logger.LogError(ex, "Skipping an undecodable committed log entry at index {Index}.", entry.Index);
            return;
        }

        if (command is null) return; // not one of ours (e.g. Raft bookkeeping entry)

        var effects = Store.Apply(command);
        Waiters.Complete(command.Id, effects.Result);
        if (effects.SignaledQueues is { } queues) Signals.PulseQueues(queues);
        if (effects.LocksReleased) Signals.PulseLocks();
    }

    private static async ValueTask<ReadOnlyMemory<byte>> ReadPayload(LogEntry entry)
        => entry.TryGetMemory(out var memory) ? memory : await entry.ToByteArrayAsync().ConfigureAwait(false);

    protected override SnapshotBuilder CreateSnapshotBuilder(in SnapshotBuilderContext context) => new Builder(context);

    /// <summary>
    /// Rebuilds the store from the previous snapshot plus the compacted entries in a shadow
    /// instance, then serializes it. The live store is not touched.
    /// </summary>
    private sealed class Builder(in SnapshotBuilderContext context) : IncrementalSnapshotBuilder(context)
    {
        private readonly RaftStore _shadow = new();

        protected override async ValueTask ApplyAsync(LogEntry entry)
        {
            var payload = await ReadPayload(entry).ConfigureAwait(false);
            if (entry.IsSnapshot)
            {
                using var reader = new BinaryReader(BinaryFormat.CreateReadStream(payload), Encoding.UTF8);
                _shadow.LoadSnapshot(reader);
                return;
            }

            var command = CommandSerializer.TryDeserialize(payload);
            if (command is not null) _shadow.Apply(command);
        }

        public override async ValueTask WriteToAsync<TWriter>(TWriter writer, CancellationToken token)
        {
            using var buffer = new MemoryStream(64 * 1024);
            using (var w = new BinaryWriter(buffer, Encoding.UTF8, leaveOpen: true))
            {
                _shadow.WriteSnapshot(w);
            }

            await writer.WriteAsync(buffer.GetBuffer().AsMemory(0, (int)buffer.Length), null, token).ConfigureAwait(false);
        }
    }
}
