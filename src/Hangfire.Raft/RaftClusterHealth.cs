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

    /// <summary>
    /// Index of the last log entry this node has applied to its local in-memory state. Local reads
    /// (job data, sets, the dashboard) reflect exactly this prefix of the log.
    /// </summary>
    public required long AppliedIndex { get; init; }

    /// <summary>
    /// Index of the last committed log entry known to this node. <c>CommitIndex - AppliedIndex</c> is the
    /// local apply lag: how far this node's reads trail the committed log. A node that reports
    /// <see cref="HasLeader"/> can still show a growing gap here (for example a partitioned follower), so
    /// a readiness probe that cares about read freshness should bound this difference.
    /// </summary>
    public required long CommitIndex { get; init; }

    /// <summary>
    /// True when the local state machine has faulted (a committed entry failed to apply, or a snapshot
    /// failed to restore). A faulted node cannot make safe progress, so a liveness probe should fail on
    /// this and let the orchestrator restart the node; readiness should also treat it as not ready.
    /// </summary>
    public required bool Faulted { get; init; }
}
