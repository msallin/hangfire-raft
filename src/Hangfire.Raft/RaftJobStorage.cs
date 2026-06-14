using Hangfire.Raft.Cluster;
using Hangfire.Raft.Monitoring;
using Hangfire.Storage;

namespace Hangfire.Raft;

/// <summary>
/// Hangfire job storage backed by a DotNext Raft cluster: state lives in replicated memory with a
/// write-ahead log and snapshots on local disk, so no external database is needed. Use
/// <see cref="StartAsync"/> (or <see cref="Start"/>) to boot the cluster node, pass the instance to
/// <c>GlobalConfiguration.Configuration.UseStorage(...)</c>, and dispose it on application shutdown.
/// </summary>
public sealed class RaftJobStorage : JobStorage, IAsyncDisposable
{
    internal RaftStorageCluster Cluster { get; }

    private RaftJobStorage(RaftStorageCluster cluster)
    {
        Cluster = cluster;
    }

    /// <summary>Boots the local cluster node: opens the write-ahead log, joins the cluster and starts the forwarding channel.</summary>
    public static async Task<RaftJobStorage> StartAsync(RaftStorageOptions options, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(options);
        var cluster = await RaftStorageCluster.StartAsync(options, cancellationToken).ConfigureAwait(false);
        return new RaftJobStorage(cluster);
    }

    /// <summary>Synchronous convenience wrapper for <see cref="StartAsync"/>.</summary>
    public static RaftJobStorage Start(RaftStorageOptions options)
        => StartAsync(options).GetAwaiter().GetResult();

    /// <summary>
    /// Returns a point-in-time view of cluster health, suitable for a readiness probe. Treat
    /// <see cref="RaftClusterHealth.HasLeader"/> as "ready to serve writes".
    /// </summary>
    public RaftClusterHealth GetHealth() => Cluster.GetHealth();

    /// <inheritdoc />
    public override IStorageConnection GetConnection() => new RaftStorageConnection(this);

    /// <inheritdoc />
    public override IMonitoringApi GetMonitoringApi() => new RaftMonitoringApi(Cluster.Store);

    private static readonly HashSet<string> SupportedFeatures = new(StringComparer.OrdinalIgnoreCase)
    {
        JobStorageFeatures.ExtendedApi,
        JobStorageFeatures.JobQueueProperty,
        JobStorageFeatures.Connection.BatchedGetFirstByLowest,
        JobStorageFeatures.Connection.GetSetContains,
        JobStorageFeatures.Connection.LimitedGetSetCount,
        JobStorageFeatures.Transaction.CreateJob,
        JobStorageFeatures.Transaction.SetJobParameter,
        JobStorageFeatures.Monitoring.DeletedStateGraphs,
        JobStorageFeatures.Monitoring.AwaitingJobs,
    };

    /// <inheritdoc />
    public override bool HasFeature(string featureId)
    {
        ArgumentNullException.ThrowIfNull(featureId);
        return SupportedFeatures.Contains(featureId) || base.HasFeature(featureId);
    }

    /// <inheritdoc />
    public override string ToString() => $"Raft cluster storage ({Cluster.Options.SelfEndpoint})";

    /// <summary>Leaves the cluster gracefully and releases the write-ahead log.</summary>
    public ValueTask DisposeAsync() => Cluster.DisposeAsync();
}
