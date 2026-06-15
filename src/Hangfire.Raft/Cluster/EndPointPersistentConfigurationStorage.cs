using System.Net;
using DotNext.Buffers;
using DotNext.IO;
using DotNext.Net;
using DotNext.Net.Cluster.Consensus.Raft.Membership;

namespace Hangfire.Raft.Cluster;

/// <summary>
/// Persistent storage for the cluster's <see cref="EndPoint"/> membership. DotNext's built-in
/// configuration storage is in-memory and forgets the membership on restart; persisting it to disk lets a
/// restarted node resume its committed membership rather than re-seeding it, which is the pattern the
/// DotNext maintainer recommends (https://github.com/dotnet/dotNext/discussions/207). Endpoint
/// (de)serialization reuses DotNext's own <see cref="EndPointFormatter"/>, matching the framework's
/// internal in-memory implementation.
/// </summary>
internal sealed class EndPointPersistentConfigurationStorage(string fileName, IEqualityComparer<EndPoint> comparer)
    : PersistentClusterConfigurationStorage<EndPoint>(fileName)
{
    protected override void Encode(EndPoint address, ref BufferWriterSlim<byte> writer) => writer.WriteEndPoint(address);

    protected override EndPoint Decode(ref SequenceReader reader) => reader.ReadEndPoint();

    protected override IEqualityComparer<EndPoint> Comparer => comparer;
}
