using System.Net;
using DotNext.Net.Cluster.Consensus.Raft;
using Hangfire.Raft.Commands;
using Hangfire.Raft.State;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Hangfire.Raft.Cluster;

/// <summary>
/// Owns the Raft node and everything around it: the state machine (WAL + in-memory store), the
/// command-forwarding channel and the leader-driven maintenance loop. The central operation is
/// <see cref="SubmitAsync"/>: serialize a command, get it committed via the leader, then wait until
/// the local state machine has applied it and hand back the op result. Waiting for the local apply
/// is what gives every node read-your-writes consistency over its local store.
/// </summary>
internal sealed class RaftStorageCluster : IAsyncDisposable
{
    private readonly RaftStorageOptions _options;
    private readonly HangfireStateMachine _stateMachine;
    private readonly RaftCluster _cluster;
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

    public RaftStore Store => _stateMachine.Store;
    public StoreSignals Signals => _stateMachine.Signals;
    public RaftStorageOptions Options => _options;
    public ILogger Logger => _logger;
    public SemaphoreSlim FetchGate => _fetchGate;

    /// <summary>True when this node currently believes itself to be the cluster leader.</summary>
    public bool IsLeader => _cluster.Leader is { IsRemote: false };

    /// <summary>
    /// True when the cluster currently has a leader this node can submit to (itself or a remote).
    /// A cheap probe the fetch loop uses to avoid starting a write that would only block until the
    /// submit timeout when there is no quorum.
    /// </summary>
    public bool HasLeader => _cluster.Leader is not null;

    /// <summary>Point-in-time view of cluster health for readiness/liveness checks.</summary>
    public RaftClusterHealth GetHealth()
    {
        var leader = _cluster.Leader;
        return new RaftClusterHealth
        {
            HasLeader = leader is not null,
            IsLeader = leader is { IsRemote: false },
            LeaderEndpoint = leader?.EndPoint,
            Term = _cluster.Term,
            MemberCount = _cluster.Members.Count,
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
        Directory.CreateDirectory(options.WalPath);
        _stateMachine = new HangfireStateMachine(options.WalPath, options.WalRecordsPerPartition, _logger);

        // Bind to a concrete IP, but advertise `self` (a DnsEndPoint when configured by host name) as
        // the public endpoint. DotNext derives member identity from the public endpoint's host, not
        // its resolved address, so identity is stable across IP changes, and it re-resolves the
        // DnsEndPoint on each (re)connection.
        var configuration = new RaftCluster.TcpConfiguration(new IPEndPoint(bindAddress, raftPort))
        {
            LowerElectionTimeout = options.LowerElectionTimeoutMs,
            UpperElectionTimeout = options.UpperElectionTimeoutMs,
            PublicEndPoint = self,
            ColdStart = false,
        };
        if (options.LoggerFactory is { } loggerFactory) configuration.LoggerFactory = loggerFactory;

        var membership = configuration.UseInMemoryConfigurationStorage();
        var members = membership.CreateActiveConfigurationBuilder();
        foreach (var member in options.ClusterMembers) members.Add(member);
        members.Build();

        _cluster = new RaftCluster(configuration) { AuditTrail = _stateMachine };

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
    /// Replicates a command through the leader and returns the op result computed by the local
    /// apply. The send loop retries only failures where the command provably was not appended;
    /// once the outcome is ambiguous (request delivered, acknowledgement lost) it stops sending
    /// and lets the apply waiter decide: if the command was committed after all, the local apply
    /// completes the waiter, otherwise the timeout surfaces an error. Resending an ambiguous
    /// command could apply a non-idempotent op (such as a fetch) twice and lose jobs.
    /// The whole operation is bounded by a single SubmitTimeout. A timeout after the command was handed
    /// to the cluster (it may already be committed) is surfaced as a distinct ambiguous error, separate
    /// from the always-safe-to-retry "no leader" timeout, so the two cannot be confused.
    /// </summary>
    public async Task<object?> SubmitAsync(Command command, CancellationToken token = default)
    {
        // A worker that submits during/after shutdown gets the documented storage exception rather
        // than a raw ObjectDisposedException from the cancellation source it is about to link.
        if (_lifetime.IsCancellationRequested) throw new RaftStorageException("The storage is shutting down.");

        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(token, _lifetime.Token);
        timeout.CancelAfter(_options.SubmitTimeout);
        var linked = timeout.Token;

        var waiter = _stateMachine.Waiters.Register(command.Id);
        // True once the command has been handed to the cluster (replicated locally or forwarded), after
        // which a timeout is ambiguous (it may already be committed) rather than safe to retry.
        var appended = false;
        try
        {
            var payload = CommandSerializer.Serialize(command);

            while (true)
            {
                linked.ThrowIfCancellationRequested();
                try
                {
                    var leader = _cluster.Leader;
                    if (leader is null)
                    {
                        await _cluster.WaitForLeaderAsync(_options.SubmitTimeout, linked).ConfigureAwait(false);
                        continue;
                    }

                    if (leader.IsRemote)
                    {
                        // Forward by the leader's own endpoint form (a DnsEndPoint re-resolves, so a
                        // rescheduled leader is reached at its new IP without a restart).
                        var rpcEndpoint = RaftStorageOptions.RpcEndpoint(leader.EndPoint, _options.RpcPortOffset);
                        await _forwardingClient.SubmitAsync(rpcEndpoint, payload, linked).ConfigureAwait(false);
                        appended = true;
                        break; // acknowledged as committed
                    }

                    var entry = _stateMachine.CreateCommandEntry(payload);
                    try
                    {
                        // true: committed. false: the entry was appended but the commit outcome is
                        // unknown (leadership changed mid-replication). Either way stop sending and let
                        // the local apply waiter return the result (or time out).
                        await _cluster.ReplicateAsync(entry, linked).ConfigureAwait(false);
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

                    appended = true;
                    break;
                }
                catch (AmbiguousCommandException ex)
                {
                    _logger.LogWarning(ex, "Command {CommandId} outcome is ambiguous; waiting for the local apply to decide.", command.Id);
                    appended = true;
                    break;
                }
                catch (Exception ex) when (IsRetryable(ex) && !linked.IsCancellationRequested)
                {
                    _logger.LogWarning(ex, "Command {CommandId} submission failed before reaching the leader, retrying.", command.Id);
                    await Task.Delay(TimeSpan.FromMilliseconds(100), linked).ConfigureAwait(false);
                }
            }

            // The command has now been handed to the cluster; from here we only wait for THIS node's
            // apply, under the remaining shared SubmitTimeout budget. On the leader path the apply has
            // already run by the time ReplicateAsync returns, so this wait is normally immediate; on the
            // forward path it lasts until the local commit index catches up to the entry.
            return await waiter.WaitAsync(linked).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (!token.IsCancellationRequested)
        {
            if (_lifetime.IsCancellationRequested) throw new RaftStorageException("The storage is shutting down.");

            // A timeout AFTER the command was handed to the cluster is ambiguous: it may already be
            // committed, so it is surfaced distinctly from a pre-append timeout (no leader / no quorum),
            // which never reached the log and is always safe to retry.
            throw appended
                ? new RaftStorageException($"The storage write for command {command.Id} was submitted but its local apply did not complete within {_options.SubmitTimeout}; the command may already be committed (ambiguous outcome).")
                : new RaftStorageException($"The storage write did not reach a leader within {_options.SubmitTimeout}. The cluster may have no leader or no quorum.");
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
        var leader = _cluster.Leader;
        if (leader is null || leader.IsRemote) return (ForwardingProtocol.StatusNotLeader, null);

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

        BinaryLogEntry entry;
        try
        {
            entry = _stateMachine.CreateCommandEntry(payload);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Rejected a forwarded command that could not be wrapped as a log entry.");
            return (ForwardingProtocol.StatusError, "command rejected");
        }

        // Bound the replicate by the same budget the submitter gives its own writes. Without this the
        // replicate is bounded only by node lifetime, so under quorum loss an entry the follower already
        // abandoned (its SubmitTimeout elapsed) would keep a pending replicate alive on the leader.
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(token);
        deadline.CancelAfter(_options.SubmitTimeout);
        try
        {
            return await _cluster.ReplicateAsync(entry, deadline.Token).ConfigureAwait(false)
                ? (ForwardingProtocol.StatusOk, null)
                : (ForwardingProtocol.StatusAmbiguous, "replication did not complete");
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
                if (_cluster.Leader is { IsRemote: false })
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
        await _cluster.StopAsync(CancellationToken.None).ConfigureAwait(false);
        _cluster.Dispose();
        _stateMachine.Dispose();
        _fetchGate.Dispose();
        _lifetime.Dispose();
    }
}
