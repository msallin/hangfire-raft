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
    /// A single entry creates a single-node cluster (still durable through the write-ahead log).
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
    /// Raft log entries per partition file. Compaction squashes full partitions into a snapshot,
    /// so smaller values snapshot more often. Mainly a tuning and testing knob.
    /// </summary>
    public int WalRecordsPerPartition { get; set; } = 4096;

    /// <summary>Raft election timeout lower bound in milliseconds.</summary>
    public int LowerElectionTimeoutMs { get; set; } = 1500;

    /// <summary>Raft election timeout upper bound in milliseconds.</summary>
    public int UpperElectionTimeoutMs { get; set; } = 3000;

    /// <summary>Optional logger factory for cluster and storage diagnostics.</summary>
    public ILoggerFactory? LoggerFactory { get; set; }

    internal IPEndPoint ResolvedSelf => ResolveEndpoint(SelfEndpoint);

    internal IReadOnlyList<IPEndPoint> ResolvedMembers
    {
        get
        {
            if (Members.Count == 0) throw new InvalidOperationException("RaftStorageOptions.Members must contain at least one endpoint.");
            var result = Members.Select(ResolveEndpoint).ToList();
            if (!result.Contains(ResolvedSelf)) throw new InvalidOperationException($"RaftStorageOptions.Members must include SelfEndpoint ({SelfEndpoint}).");
            return result;
        }
    }

    /// <summary>
    /// Parses "host:port" into an IPEndPoint, resolving host names via DNS once at startup.
    /// Input:  "127.0.0.1:3000"  -> 127.0.0.1:3000
    /// Input:  "node1.local:3000" -> first resolved IPv4 address:3000
    /// </summary>
    internal static IPEndPoint ResolveEndpoint(string endpoint)
    {
        var idx = endpoint.LastIndexOf(':');
        if (idx <= 0 || idx == endpoint.Length - 1)
            throw new FormatException($"Endpoint '{endpoint}' must have the form host:port.");

        var host = endpoint[..idx];
        var port = int.Parse(endpoint[(idx + 1)..]);

        if (IPAddress.TryParse(host, out var ip)) return new IPEndPoint(ip, port);

        var addresses = Dns.GetHostAddresses(host);
        var address = addresses.FirstOrDefault(a => a.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork)
                      ?? addresses.FirstOrDefault()
                      ?? throw new InvalidOperationException($"Cannot resolve host '{host}'.");
        return new IPEndPoint(address, port);
    }

    internal static IPEndPoint RpcEndpoint(IPEndPoint raftEndpoint, int offset) => new(raftEndpoint.Address, raftEndpoint.Port + offset);
}
