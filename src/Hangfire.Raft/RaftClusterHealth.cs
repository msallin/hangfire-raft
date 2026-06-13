using System.Net;

namespace Hangfire.Raft;

/// <summary>
/// Point-in-time view of the local node's Raft cluster health, for wiring readiness and liveness
/// probes. Obtained from <see cref="RaftJobStorage.GetHealth"/>.
/// </summary>
public sealed record RaftClusterHealth
{
    /// <summary>
    /// True when this node currently knows a leader (this node or a peer). This is the recommended
    /// readiness signal: it means the node can submit writes — directly if it is the leader, or by
    /// forwarding to the leader otherwise. A leaderless node returns false and should be considered
    /// not ready to serve writes.
    /// </summary>
    public required bool HasLeader { get; init; }

    /// <summary>True when this node is the current leader.</summary>
    public required bool IsLeader { get; init; }

    /// <summary>The current leader's Raft endpoint, or null when there is no known leader.</summary>
    public EndPoint? LeaderEndpoint { get; init; }

    /// <summary>The current Raft term.</summary>
    public required long Term { get; init; }

    /// <summary>Number of configured cluster members.</summary>
    public required int MemberCount { get; init; }
}
