using System.Net;
using DotNext.Net.Cluster.Consensus.Raft;
using DotNext.Net.Cluster.Consensus.Raft.Membership;
using DotNext.Net.Cluster.Consensus.Raft.StateMachine;
using Hangfire.Raft.Commands;
using Hangfire.Raft.State;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Hangfire.Raft.Cluster;

/// <summary>
/// Owns the Raft node and everything around it: the write-ahead log, the state machine (in-memory
/// store), the command-forwarding channel and the leader-driven maintenance loop. The central
/// operation is <see cref="SubmitAsync"/>: serialize a command, get it committed via the leader, then
/// wait until the local state machine has applied it and hand back the op result. Waiting for the local
/// apply is what gives every node read-your-writes consistency over its local store.
/// </summary>
internal sealed class RaftStorageCluster : IAsyncDisposable
{
    private readonly RaftStorageOptions _options;
    private readonly HangfireStateMachine _stateMachine;
    private readonly RaftCluster.TcpConfiguration _configuration;
    private readonly IClusterConfigurationStorage<EndPoint> _configStorage;
    private readonly IReadOnlyList<EndPoint> _clusterMembers;
    private readonly bool _coldStart;
    private readonly string _logPath;
    private readonly ForwardingServer _forwardingServer;
    private readonly ForwardingClient _forwardingClient = new();
    private readonly CancellationTokenSource _lifetime = new();
    private readonly ILogger _logger;

    // Lets only one fetch be in consensus flight per node at a time. An enqueue wakes every idle
    // worker on the queue, but without this gate they would each submit a FetchOp, turning one job
    // into N wasted consensus rounds (N = idle workers). With it, losers wait for the winner's
    // outcome and re-probe a possibly-empty queue instead.
    private readonly SemaphoreSlim _fetchGate = new(1, 1);
    private Task _maintenanceLoop = Task.CompletedTask;

    // Created in StartAsync after the state machine has restored its snapshot (the WAL must be built
    // over an already-restored state machine). Null only before a successful StartAsync.
    private WriteAheadLog? _wal;
    private RaftCluster? _cluster;

    public RaftStore Store => _stateMachine.Store;
    public StoreSignals Signals => _stateMachine.Signals;
    public RaftStorageOptions Options => _options;
    public ILogger Logger => _logger;
    public SemaphoreSlim FetchGate => _fetchGate;

    /// <summary>True when this node currently believes itself to be the cluster leader.</summary>
    public bool IsLeader => _cluster?.Leader is { IsRemote: false };

    /// <summary>
    /// True when the cluster currently has a leader this node can submit to (itself or a remote).
    /// A cheap probe the fetch loop uses to avoid starting a write that would only block until the
    /// submit timeout when there is no quorum.
    /// </summary>
    public bool HasLeader => _cluster?.Leader is not null;

    /// <summary>Point-in-time view of cluster health for readiness/liveness checks.</summary>
    public RaftClusterHealth GetHealth()
    {
        var cluster = _cluster;
        if (cluster is null)
            return new RaftClusterHealth { HasLeader = false, IsLeader = false, LeaderEndpoint = null, Term = 0, MemberCount = 0 };

        var leader = cluster.Leader;
        return new RaftClusterHealth
        {
            HasLeader = leader is not null,
            IsLeader = leader is { IsRemote: false },
            LeaderEndpoint = leader?.EndPoint,
            Term = cluster.Term,
            MemberCount = cluster.Members.Count,
        };
    }

    private RaftStorageCluster(RaftStorageOptions options)
    {
        _options = options;
        _logger = (ILoggerFactory?)options.LoggerFactory is { } factory
            ? factory.CreateLogger("Hangfire.Raft")
            : NullLogger.Instance;

        var self = options.Self;                                   // EndPoint: DnsEndPoint for host names, IPEndPoint for IP literals
        var raftPort = RaftStorageOptions.PortOf(self);
        var bindAddress = RaftStorageOptions.BindAddressFor(self);

        // The write-ahead log and the state-machine snapshots live in separate subdirectories of
        // WalPath so the WAL's chunk files and the snapshot files never collide.
        Directory.CreateDirectory(options.WalPath);
        _logPath = Path.Combine(options.WalPath, "log");
        Directory.CreateDirectory(_logPath);
        _stateMachine = new HangfireStateMachine(new DirectoryInfo(Path.Combine(options.WalPath, "state")), options.WalRecordsPerPartition, _logger);

        // Bind to a concrete IP, but advertise `self` (a DnsEndPoint when configured by host name) as
        // the public endpoint. DotNext derives member identity from the public endpoint's host, not
        // its resolved address, so identity is stable across IP changes, and it re-resolves the
        // DnsEndPoint on each (re)connection.
        _clusterMembers = options.ClusterMembers;
        var configFile = Path.Combine(options.WalPath, "config");

        // A lone node on its FIRST start cold-starts: it adds itself to the configuration in committed
        // state and elects itself immediately (term 0), which avoids a contended first election. "First
        // start" is detected by the absence of the persisted config file; on a restart the file exists, so
        // the node resumes (ColdStart=false) and loads its committed membership from disk. Multi-node
        // clusters coordinate the first election (false) and are seeded in SeedConfigurationAsync.
        _coldStart = _clusterMembers.Count == 1 && !File.Exists(configFile);

        // Persist the cluster membership to disk so a restarted node resumes its committed membership
        // instead of re-seeding it. DotNext's built-in configuration storage is in-memory and forgets it on
        // restart; persisting is the pattern the maintainer recommends (DotNext discussion #207).
        _configStorage = new EndPointPersistentConfigurationStorage(configFile, EqualityComparer<EndPoint>.Default);
        _configuration = new RaftCluster.TcpConfiguration(new IPEndPoint(bindAddress, raftPort))
        {
            LowerElectionTimeout = options.LowerElectionTimeoutMs,
            UpperElectionTimeout = options.UpperElectionTimeoutMs,
            PublicEndPoint = self,
            ColdStart = _coldStart,
            LoggerFactory = options.LoggerFactory as ILoggerFactory ?? NullLoggerFactory.Instance,
            ConfigurationStorage = _configStorage,
        };

        _forwardingServer = new ForwardingServer(
            new IPEndPoint(bindAddress, raftPort + options.RpcPortOffset),
            HandleForwardedCommand,
            _logger);
    }

    public static async Task<RaftStorageCluster> StartAsync(RaftStorageOptions options, CancellationToken token)
    {
        var cluster = new RaftStorageCluster(options);
        try
        {
            // Restore the latest snapshot into the state machine before building the WAL over it. The
            // SimpleStateMachine snapshot must be restored explicitly, and the committed log replayed
            // (below) before we serve, or a restart loses entries committed after the last snapshot.
            // RaftCluster.StartAsync also calls AuditTrail.InitializeAsync, but that alone is not
            // sufficient here (verified: dropping these calls fails the WAL-replay tests in isolation).
            await cluster._stateMachine.RestoreAsync(token).ConfigureAwait(false);
            var hasExistingLog = Directory.EnumerateFileSystemEntries(cluster._logPath).Any();

            // FlushInterval.Zero checkpoints the log on every commit (durable-on-commit): a single node is
            // its own majority and must flush locally for a write to complete and survive a restart.
            cluster._wal = new WriteAheadLog(
                new WriteAheadLog.Options { Location = cluster._logPath, FlushInterval = TimeSpan.Zero },
                cluster._stateMachine);

            // Replay committed-but-unsnapshotted entries on a restart (a fresh node has nothing to replay).
            if (hasExistingLog)
                await cluster._wal.InitializeAsync(token).ConfigureAwait(false);

            cluster._cluster = new RaftCluster(cluster._configuration) { AuditTrail = cluster._wal };

            // Seed a multi-node cluster's committed membership on first start so it forms regardless of
            // start order (a lone node cold-starts instead; a restart loads its membership from disk). Must
            // run before StartAsync, which loads the configuration. See DotNext discussion #207.
            await cluster.SeedConfigurationAsync(token).ConfigureAwait(false);

            cluster._forwardingServer.Start();
            await cluster._cluster.StartAsync(token).ConfigureAwait(false);
            cluster._maintenanceLoop = cluster.MaintenanceLoop();
            return cluster;
        }
        catch
        {
            await cluster.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>
    /// Writes the static membership into the persistent configuration storage as the committed genesis
    /// configuration, but only on first start: on a restart the storage already holds it (loaded from
    /// disk) and is left untouched. Every node seeds the same member set, so a single node ends up as the
    /// sole committed member (and elects itself) and a multi-node cluster shares one committed configuration.
    /// </summary>
    private async Task SeedConfigurationAsync(CancellationToken token)
    {
        // A cold-starting lone node adds and persists its sole member via the bootstrap, so skip it here.
        if (_coldStart) return;

        var config = await _configStorage.LoadConfigurationAsync(token).ConfigureAwait(false);
        if (config.Members.Count is not 0) return; // a restart already has its membership on disk

        foreach (var member in _clusterMembers) config = config.Add(member);
        await _configStorage.SaveConfigurationAsync(config, configurationVersion: 0L, token).ConfigureAwait(false);
    }

    /// <summary>
    /// Replicates a command through the leader and returns the op result computed by the local
    /// apply. The send loop retries only failures where the command provably was not appended;
    /// once the outcome is ambiguous (request delivered, acknowledgement lost) it stops sending
    /// and lets the apply waiter decide: if the command was committed after all, the local apply
    /// completes the waiter, otherwise the timeout surfaces an error. Resending an ambiguous
    /// command could apply a non-idempotent op (such as a fetch) twice and lose jobs.
    /// </summary>
    public async Task<object?> SubmitAsync(Command command, CancellationToken token = default)
    {
        // A worker that submits during/after shutdown gets the documented storage exception rather
        // than a raw ObjectDisposedException from the cancellation source it is about to link.
        if (_lifetime.IsCancellationRequested) throw new RaftStorageException("The storage is shutting down.");

        var cluster = _cluster!; // non-null once StartAsync has returned, which is before any submit

        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(token, _lifetime.Token);
        timeout.CancelAfter(_options.SubmitTimeout);
        var linked = timeout.Token;

        var replicated = false; // true once the command reached a committed state (local or forwarded)
        var waiter = _stateMachine.Waiters.Register(command.Id);
        try
        {
            ReadOnlyMemory<byte> payload = CommandSerializer.Serialize(command);

            while (true)
            {
                linked.ThrowIfCancellationRequested();
                try
                {
                    var leader = cluster.Leader;
                    if (leader is null)
                    {
                        await cluster.WaitForLeaderAsync(_options.SubmitTimeout, linked).ConfigureAwait(false);
                        continue;
                    }

                    if (leader.IsRemote)
                    {
                        // Forward by the leader's own endpoint form (a DnsEndPoint re-resolves, so a
                        // rescheduled leader is reached at its new IP without a restart).
                        var rpcEndpoint = RaftStorageOptions.RpcEndpoint(leader.EndPoint, _options.RpcPortOffset);
                        await _forwardingClient.SubmitAsync(rpcEndpoint, payload, linked).ConfigureAwait(false);
                        replicated = true;
                        break; // acknowledged as committed
                    }

                    try
                    {
                        // Returns once the entry is committed; throws otherwise. Either way stop sending
                        // and let the local apply waiter return the result (or time out).
                        await cluster.ReplicateAsync(payload, null, linked).ConfigureAwait(false);
                    }
                    catch (Exception ex) when (ex is not (OperationCanceledException or ObjectDisposedException) && !IsRetryable(ex))
                    {
                        // The entry was appended before this throw, so it may still commit. Mirror the
                        // forward path (HandleForwardedCommand): a flat failure here would let Hangfire
                        // retry into a possible double-apply of a non-idempotent op (a leader-local fetch
                        // or counter increment), so defer to the local apply waiter instead of rethrowing.
                        // ObjectDisposedException is excluded so a shutdown-time dispose surfaces as the
                        // documented "shutting down" error rather than waiting out the whole timeout.
                        _logger.LogWarning(ex, "Local replication of command {CommandId} threw after append; treating the outcome as ambiguous and waiting for the local apply.", command.Id);
                    }

                    replicated = true;
                    break;
                }
                catch (AmbiguousCommandException ex)
                {
                    _logger.LogWarning(ex, "Command {CommandId} outcome is ambiguous; waiting for the local apply to decide.", command.Id);
                    break;
                }
                catch (Exception ex) when (IsRetryable(ex) && !linked.IsCancellationRequested)
                {
                    _logger.LogWarning(ex, "Command {CommandId} submission failed before reaching the leader, retrying.", command.Id);
                    await Task.Delay(TimeSpan.FromMilliseconds(100), linked).ConfigureAwait(false);
                }
            }

            return await waiter.WaitAsync(linked).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (!token.IsCancellationRequested)
        {
            throw _lifetime.IsCancellationRequested
                ? new RaftStorageException("The storage is shutting down.")
                : new RaftStorageException($"The storage write did not complete within {_options.SubmitTimeout} (replicated={replicated}). " + (replicated ? "The entry committed but was not applied in time." : "The cluster may have no leader or no quorum."));
        }
        catch (Exception ex) when (ex is not (RaftStorageException or OperationCanceledException))
        {
            // Surface every failure as the documented storage exception so Hangfire treats it as
            // transient and retries, instead of crashing a worker with a raw cluster/IO exception.
            throw _lifetime.IsCancellationRequested
                ? new RaftStorageException("The storage is shutting down.", ex)
                : new RaftStorageException("The storage write failed unexpectedly.", ex);
        }
        finally
        {
            _stateMachine.Waiters.Remove(command.Id);
        }
    }

    public object? Submit(Command command, CancellationToken token = default)
        => SubmitAsync(command, token).GetAwaiter().GetResult();

    /// <summary>
    /// Failures where the command provably never reached the leader's log, so resending is safe.
    /// DotNext's <see cref="NotLeaderException"/> is thrown by the leadership pre-check before any
    /// append; <see cref="TimeoutException"/> comes from WaitForLeaderAsync. Matched explicitly
    /// rather than via their <c>InvalidOperationException</c> base so that other IOE-shaped failures
    /// (e.g. ObjectDisposedException during shutdown) are not silently retried for the full timeout.
    /// </summary>
    internal static bool IsRetryable(Exception ex) => ex is NotLeaderResponseException
        or RetryableForwardingException
        or NotLeaderException
        or TimeoutException;

    private async Task<(byte Status, string? Message)> HandleForwardedCommand(ReadOnlyMemory<byte> payload, CancellationToken token)
    {
        var cluster = _cluster;
        if (cluster is null || cluster.Leader is not { IsRemote: false }) return (ForwardingProtocol.StatusNotLeader, null);

        // Reject undecodable payloads before they enter the replicated log: a buggy or hostile peer
        // must not be able to get a poison entry committed that would then fault apply on every node.
        try
        {
            if (CommandSerializer.TryDeserialize(payload) is null)
                return (ForwardingProtocol.StatusError, "not a command");
        }
        catch (Exception ex)
        {
            // Detail is logged locally, not echoed to the remote caller.
            _logger.LogWarning(ex, "Rejected an undecodable forwarded command from a peer.");
            return (ForwardingProtocol.StatusError, "undecodable command");
        }

        // Bound the replicate by the same budget the submitter gives its own writes. Without this the
        // replicate is bounded only by node lifetime, so under quorum loss an entry the follower already
        // abandoned (its SubmitTimeout elapsed) would keep a pending replicate alive on the leader.
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(token);
        deadline.CancelAfter(_options.SubmitTimeout);
        try
        {
            // Returns once the entry is committed; throws otherwise. Success is reported as committed; a
            // leadership loss before append is retryable, anything else is ambiguous (the entry may
            // already sit in the log, including when the deadline above fires).
            await cluster.ReplicateAsync(payload, null, deadline.Token).ConfigureAwait(false);
            return (ForwardingProtocol.StatusOk, null);
        }
        catch (NotLeaderException)
        {
            // Leadership was lost before the entry was appended: safe for the submitter to re-resolve
            // the new leader and retry, so this is NotLeader, not an ambiguous outcome.
            return (ForwardingProtocol.StatusNotLeader, null);
        }
        catch (Exception)
        {
            // From here the entry may already sit in the log; only the submitter's local apply can
            // tell whether it committed (this includes hitting the deadline above). Detail stays local
            // rather than leaking to the caller.
            return (ForwardingProtocol.StatusAmbiguous, "outcome unknown");
        }
    }

    /// <summary>
    /// Leader-only periodic cleanup. Every node runs the loop, but only the current leader submits, so
    /// the cluster performs maintenance approximately once per interval regardless of size. A leadership
    /// change can run it twice for one interval; that is harmless because maintenance is convergent
    /// (re-eviction is idempotent and already-requeued fetches are gone), applied identically on every
    /// replica from the command's envelope time.
    /// </summary>
    private async Task MaintenanceLoop()
    {
        while (!_lifetime.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(_options.MaintenanceInterval, _lifetime.Token).ConfigureAwait(false);
                if (_cluster?.Leader is { IsRemote: false })
                {
                    await SubmitAsync(Command.Single(new MaintenanceOp(_options.FetchInvisibilityTimeout)), _lifetime.Token).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException)
            {
                return;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Storage maintenance failed; it will be retried on the next interval.");
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        await _lifetime.CancelAsync().ConfigureAwait(false);
        try
        {
            await _maintenanceLoop.ConfigureAwait(false);
        }
        catch (Exception)
        {
            // loop exceptions are already logged
        }

        await _forwardingServer.DisposeAsync().ConfigureAwait(false);
        _forwardingClient.Dispose();
        if (_cluster is not null)
        {
            await _cluster.StopAsync(CancellationToken.None).ConfigureAwait(false);
            await _cluster.DisposeAsync().ConfigureAwait(false);
        }

        // No shutdown flush: FlushInterval.Zero already checkpointed every commit. An explicit FlushAsync
        // here would advance the applied checkpoint past the not-yet-snapshotted entries and make the
        // restart replay skip them.
        if (_wal is not null) await _wal.DisposeAsync().ConfigureAwait(false);
        await _stateMachine.DisposeAsync().ConfigureAwait(false);
        _configStorage.Dispose();
        _fetchGate.Dispose();
        _lifetime.Dispose();
    }
}
