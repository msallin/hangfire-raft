using System.Net;
using Microsoft.Extensions.Logging;

namespace Hangfire.Raft;

/// <summary>
/// Configuration for <see cref="RaftJobStorage"/>. Every node of the cluster must list the same
/// <see cref="Members"/> (including its own endpoint, given as <see cref="SelfEndpoint"/>).
/// Raft traffic uses the configured port; command forwarding to the leader uses port + <see cref="RpcPortOffset"/>.
/// </summary>
public sealed class RaftStorageOptions
{
    /// <summary>Raft endpoint of this node, e.g. "127.0.0.1:3000" or "node1.local:3000".</summary>
    public required string SelfEndpoint { get; set; }

    /// <summary>
    /// Raft endpoints of all cluster members, including this node. Must be identical on every node.
    /// Use an odd number of nodes (3 or 5) in production so a committed write survives a node crash:
    /// durability comes from quorum replication, not from any single node's disk. A single entry forms a
    /// one-node "cluster" with no fault tolerance, suitable only for local development and tests.
    /// </summary>
    public IList<string> Members { get; } = new List<string>();

    /// <summary>Directory for the Raft write-ahead log and snapshots. Must be node-local and persistent.</summary>
    public string WalPath { get; set; } = Path.Combine(AppContext.BaseDirectory, "hangfire-raft");

    /// <summary>Offset added to the Raft port to derive the command-forwarding (RPC) port of each node.</summary>
    public int RpcPortOffset { get; set; } = 1;

    /// <summary>Maximum time a single storage write may take (replication + local apply) before it fails.</summary>
    public TimeSpan SubmitTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Lease duration for distributed locks. Held locks are renewed automatically at a third of this
    /// interval; if the owning process dies, the lock becomes available after the lease expires.
    /// </summary>
    public TimeSpan LockLeaseTimeout { get; set; } = TimeSpan.FromMinutes(2);

    /// <summary>
    /// Time after which a fetched but neither acknowledged nor requeued job becomes visible to other
    /// workers again. Active workers renew their fetch lease at a third of this interval.
    /// </summary>
    public TimeSpan FetchInvisibilityTimeout { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>Interval at which the current leader runs maintenance (eviction of expired entries, reclaim of stale fetches).</summary>
    public TimeSpan MaintenanceInterval { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Take a state-machine snapshot every this many applied log entries; the write-ahead log can then
    /// compact everything up to that snapshot. Smaller values snapshot more often (smaller log, more
    /// snapshot churn). Mainly a tuning and testing knob.
    /// </summary>
    public int SnapshotInterval { get; set; } = 4096;

    /// <summary>
    /// How the write-ahead log persists committed entries to this node's local disk. Durability does not
    /// depend on this: a committed entry is held by a majority of nodes, and a node that restarts recovers
    /// any not-yet-flushed tail from the current leader. <see cref="TimeSpan.Zero"/> (the default) flushes
    /// eagerly in the background as each entry commits, which keeps the node's on-disk copy current without
    /// blocking the writer. A positive value batches commits into one fsync per interval (higher write
    /// throughput, but a crash can lose up to that interval of locally-unflushed entries, which a
    /// multi-node cluster re-replicates from the quorum on restart). <see cref="Timeout.InfiniteTimeSpan"/>
    /// disables background flushing entirely: this node then never persists committed entries to disk on its
    /// own, so after a restart it recovers everything past its last snapshot from the quorum.
    /// </summary>
    public TimeSpan FlushInterval { get; set; } = TimeSpan.Zero;

    /// <summary>Raft election timeout lower bound in milliseconds.</summary>
    public int LowerElectionTimeoutMs { get; set; } = 1500;

    /// <summary>Raft election timeout upper bound in milliseconds.</summary>
    public int UpperElectionTimeoutMs { get; set; } = 3000;

    /// <summary>Optional logger factory for cluster and storage diagnostics.</summary>
    public ILoggerFactory? LoggerFactory { get; set; }

    internal EndPoint Self => ParseEndpoint(SelfEndpoint);

    internal IReadOnlyList<EndPoint> ClusterMembers
    {
        get
        {
            if (Members.Count == 0) throw new InvalidOperationException("RaftStorageOptions.Members must contain at least one endpoint.");
            var result = Members.Select(ParseEndpoint).ToList();
            if (!result.Contains(Self)) throw new InvalidOperationException($"RaftStorageOptions.Members must include SelfEndpoint ({SelfEndpoint}).");
            return result;
        }
    }

    /// <summary>
    /// Parses "host:port" into an endpoint WITHOUT resolving DNS. An IP literal becomes an
    /// <see cref="IPEndPoint"/>; a host name becomes a <see cref="DnsEndPoint"/>, which the Raft
    /// transport re-resolves on every (re)connection. That is what lets a member whose IP changes
    /// (for example a rescheduled Kubernetes pod that keeps its DNS name) be reached again without
    /// restarting the rest of the cluster, and it also means startup does not fail when a peer's
    /// name is not yet resolvable.
    /// Input:  "127.0.0.1:3000"   -> IPEndPoint 127.0.0.1:3000
    /// Input:  "[::1]:3000"        -> IPEndPoint ::1:3000   (IPv6 literals must be bracketed)
    /// Input:  "node1.local:3000" -> DnsEndPoint node1.local:3000
    /// </summary>
    internal static EndPoint ParseEndpoint(string endpoint)
    {
        var idx = endpoint.LastIndexOf(':');
        if (idx <= 0 || idx == endpoint.Length - 1)
            throw new FormatException($"Endpoint '{endpoint}' must have the form host:port.");

        var host = endpoint[..idx];
        if (!int.TryParse(endpoint[(idx + 1)..], out var port) || port is < 0 or > 65535)
            throw new FormatException($"Endpoint '{endpoint}' has an invalid port; expected a number in 0-65535.");

        // An IPv6 literal must be bracketed so the colons in the address are not mistaken for the
        // host:port separator; a bare IPv6 literal is rejected rather than silently misparsed.
        if (host.StartsWith('[') && host.EndsWith(']'))
            host = host[1..^1];
        else if (host.Contains(':'))
            throw new FormatException($"Endpoint '{endpoint}': an IPv6 literal must be bracketed, e.g. [::1]:{port}.");

        return IPAddress.TryParse(host, out var ip)
            ? new IPEndPoint(ip, port)
            : new DnsEndPoint(host, port);
    }

    internal static int PortOf(EndPoint endpoint) => endpoint switch
    {
        IPEndPoint ip => ip.Port,
        DnsEndPoint dns => dns.Port,
        _ => throw new NotSupportedException($"Unsupported endpoint type {endpoint.GetType()}."),
    };

    /// <summary>
    /// The address a node binds its Raft and forwarding listeners to: the IP itself for an IP literal
    /// (so loopback clusters keep their exact address), or all interfaces for a host name — in the
    /// latter case the node advertises its <see cref="DnsEndPoint"/> as the public endpoint, so the
    /// bind address never needs to be resolved.
    /// </summary>
    internal static IPAddress BindAddressFor(EndPoint endpoint) => endpoint is IPEndPoint ip ? ip.Address : IPAddress.Any;

    /// <summary>Derives the forwarding endpoint (Raft port + offset), preserving the address form.</summary>
    internal static EndPoint RpcEndpoint(EndPoint raftEndpoint, int offset)
    {
        // Validate the combined port up front so a too-large RpcPortOffset fails with a message that
        // names the offset, rather than a bare ArgumentOutOfRangeException from the endpoint constructor.
        var rpcPort = PortOf(raftEndpoint) + offset;
        if (rpcPort is < 0 or > 65535)
            throw new InvalidOperationException(
                $"Forwarding port {rpcPort} (Raft port {PortOf(raftEndpoint)} + RpcPortOffset {offset}) is outside the valid range 0-65535.");

        return raftEndpoint switch
        {
            IPEndPoint ip => new IPEndPoint(ip.Address, rpcPort),
            DnsEndPoint dns => new DnsEndPoint(dns.Host, rpcPort, dns.AddressFamily),
            _ => throw new NotSupportedException($"Unsupported endpoint type {raftEndpoint.GetType()}."),
        };
    }
}
