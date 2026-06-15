using System.Diagnostics.Metrics;

namespace Hangfire.Raft;

/// <summary>
/// Counters for the operational events that would otherwise only surface as warning logs: writes
/// whose commit could not be confirmed, fetch leases reclaimed under another worker (a possible
/// duplicate execution) and distributed locks lost to another owner. They are published through a
/// <see cref="Meter"/> named <see cref="MeterName"/> so an OpenTelemetry pipeline or
/// <c>dotnet-counters</c> can alert on them instead of grepping logs.
/// </summary>
internal static class RaftMetrics
{
    /// <summary>Meter name to subscribe to (e.g. in an OpenTelemetry metrics provider).</summary>
    public const string MeterName = "Hangfire.Raft";

    private static readonly Meter Meter = new(MeterName);

    /// <summary>
    /// Incremented when a write is surfaced to the caller as ambiguous: it was handed to the cluster and
    /// may already be committed, but its commit could not be confirmed within the submit timeout, so
    /// Hangfire retries it under a fresh command (which can double-apply non-idempotent effects). Transient
    /// ambiguity that the local apply waiter goes on to resolve successfully is not counted.
    /// </summary>
    public static readonly Counter<long> AmbiguousWrites = Meter.CreateCounter<long>(
        "hangfire.raft.ambiguous_writes",
        unit: "{write}",
        description: "Writes surfaced to the caller as ambiguous (commit unconfirmed within the submit timeout), which Hangfire retries under a fresh command.");

    /// <summary>Incremented when a worker's fetch lease was reclaimed by maintenance, so the job may run a second time.</summary>
    public static readonly Counter<long> FetchLeaseReclaims = Meter.CreateCounter<long>(
        "hangfire.raft.fetch_lease_reclaims",
        unit: "{job}",
        description: "Fetch leases reclaimed while a worker still held the job; the job may run a second time.");

    /// <summary>Incremented when a held distributed lock was lost to another owner (the lease expired during an outage).</summary>
    public static readonly Counter<long> LockLosses = Meter.CreateCounter<long>(
        "hangfire.raft.lock_losses",
        unit: "{lock}",
        description: "Held distributed locks lost to another owner because the lease expired before it could be renewed.");
}
