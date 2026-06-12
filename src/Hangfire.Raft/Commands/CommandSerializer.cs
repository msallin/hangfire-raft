using System.Text;
using static Hangfire.Raft.Commands.BinaryFormat;

namespace Hangfire.Raft.Commands;

/// <summary>
/// Hand-rolled binary serialization for replicated commands. The format is versioned via a leading
/// magic + version byte so the log stays readable across upgrades.
///
/// Layout: [0xC1][version][guid 16B][nowUtc ticks 8B][opCount 7-bit]{[opcode 1B][payload]}*
/// Strings use the BinaryWriter 7-bit length prefix; nullable strings carry a presence flag.
/// </summary>
internal static class CommandSerializer
{
    private const byte Magic = 0xC1;
    private const byte Version = 1;

    public static byte[] Serialize(Command command)
    {
        using var stream = new MemoryStream(256);
        using var writer = new BinaryWriter(stream, Encoding.UTF8, leaveOpen: true);

        writer.Write(Magic);
        writer.Write(Version);
        WriteGuid(writer, command.Id);
        writer.Write(command.NowUtc.Ticks);
        writer.Write7BitEncodedInt(command.Ops.Count);
        foreach (var op in command.Ops) WriteOp(writer, op);

        writer.Flush();
        return stream.ToArray();
    }

    /// <summary>Returns null when the payload is not a command (e.g. an empty Raft bookkeeping entry).</summary>
    public static Command? TryDeserialize(ReadOnlyMemory<byte> payload)
    {
        if (payload.Length < 2 || payload.Span[0] != Magic) return null;

        using var stream = BinaryFormat.CreateReadStream(payload);
        using var reader = new BinaryReader(stream, Encoding.UTF8, leaveOpen: true);

        reader.ReadByte(); // magic, already checked
        var version = reader.ReadByte();
        if (version != Version) throw new NotSupportedException($"Unknown command format version {version}.");

        var id = new Guid(reader.ReadBytes(16));
        var nowUtc = new DateTime(reader.ReadInt64(), DateTimeKind.Utc);
        var count = ReadCount(reader);
        var ops = new StoreOp[count];
        for (var i = 0; i < count; i++) ops[i] = ReadOp(reader);

        return new Command { Id = id, NowUtc = nowUtc, Ops = ops };
    }

    private static void WriteOp(BinaryWriter w, StoreOp op)
    {
        switch (op)
        {
            case CreateJobOp o:
                w.Write((byte)OpCode.CreateJob);
                w.Write(o.JobId);
                w.Write(o.InvocationData);
                WritePairs(w, o.Parameters);
                w.Write(o.CreatedAt.Ticks);
                w.Write(o.ExpireAt.Ticks);
                break;
            case SetJobParameterOp o:
                w.Write((byte)OpCode.SetJobParameter);
                w.Write(o.JobId);
                w.Write(o.Name);
                WriteNullable(w, o.Value);
                break;
            case SetJobStateOp o:
                w.Write((byte)OpCode.SetJobState);
                w.Write(o.JobId);
                WriteState(w, o.State);
                break;
            case AddJobStateOp o:
                w.Write((byte)OpCode.AddJobState);
                w.Write(o.JobId);
                WriteState(w, o.State);
                break;
            case ExpireJobOp o:
                w.Write((byte)OpCode.ExpireJob);
                w.Write(o.JobId);
                w.Write(o.ExpireAt.Ticks);
                break;
            case PersistJobOp o:
                w.Write((byte)OpCode.PersistJob);
                w.Write(o.JobId);
                break;
            case EnqueueOp o:
                w.Write((byte)OpCode.Enqueue);
                w.Write(o.Queue);
                w.Write(o.JobId);
                break;
            case FetchOp o:
                w.Write((byte)OpCode.Fetch);
                WriteStrings(w, o.Queues);
                WriteGuid(w, o.FetchToken);
                break;
            case AckFetchedOp o:
                w.Write((byte)OpCode.AckFetched);
                WriteGuid(w, o.FetchToken);
                break;
            case RequeueFetchedOp o:
                w.Write((byte)OpCode.RequeueFetched);
                WriteGuid(w, o.FetchToken);
                break;
            case RenewFetchedOp o:
                w.Write((byte)OpCode.RenewFetched);
                WriteGuid(w, o.FetchToken);
                break;
            case IncrementCounterOp o:
                w.Write((byte)OpCode.IncrementCounter);
                w.Write(o.Key);
                w.Write(o.Delta);
                WriteNullable(w, o.ExpireAt);
                break;
            case AddToSetOp o:
                w.Write((byte)OpCode.AddToSet);
                w.Write(o.Key);
                w.Write(o.Value);
                w.Write(o.Score);
                break;
            case AddRangeToSetOp o:
                w.Write((byte)OpCode.AddRangeToSet);
                w.Write(o.Key);
                WriteStrings(w, o.Values);
                break;
            case RemoveFromSetOp o:
                w.Write((byte)OpCode.RemoveFromSet);
                w.Write(o.Key);
                w.Write(o.Value);
                break;
            case RemoveSetOp o:
                w.Write((byte)OpCode.RemoveSet);
                w.Write(o.Key);
                break;
            case ExpireSetOp o:
                w.Write((byte)OpCode.ExpireSet);
                w.Write(o.Key);
                w.Write(o.ExpireAt.Ticks);
                break;
            case PersistSetOp o:
                w.Write((byte)OpCode.PersistSet);
                w.Write(o.Key);
                break;
            case InsertToListOp o:
                w.Write((byte)OpCode.InsertToList);
                w.Write(o.Key);
                w.Write(o.Value);
                break;
            case RemoveFromListOp o:
                w.Write((byte)OpCode.RemoveFromList);
                w.Write(o.Key);
                w.Write(o.Value);
                break;
            case TrimListOp o:
                w.Write((byte)OpCode.TrimList);
                w.Write(o.Key);
                w.Write(o.KeepStartingFrom);
                w.Write(o.KeepEndingAt);
                break;
            case ExpireListOp o:
                w.Write((byte)OpCode.ExpireList);
                w.Write(o.Key);
                w.Write(o.ExpireAt.Ticks);
                break;
            case PersistListOp o:
                w.Write((byte)OpCode.PersistList);
                w.Write(o.Key);
                break;
            case SetRangeInHashOp o:
                w.Write((byte)OpCode.SetRangeInHash);
                w.Write(o.Key);
                WritePairs(w, o.Fields);
                break;
            case RemoveHashOp o:
                w.Write((byte)OpCode.RemoveHash);
                w.Write(o.Key);
                break;
            case ExpireHashOp o:
                w.Write((byte)OpCode.ExpireHash);
                w.Write(o.Key);
                w.Write(o.ExpireAt.Ticks);
                break;
            case PersistHashOp o:
                w.Write((byte)OpCode.PersistHash);
                w.Write(o.Key);
                break;
            case AnnounceServerOp o:
                w.Write((byte)OpCode.AnnounceServer);
                w.Write(o.ServerId);
                w.Write(o.WorkerCount);
                WriteStrings(w, o.Queues);
                break;
            case RemoveServerOp o:
                w.Write((byte)OpCode.RemoveServer);
                w.Write(o.ServerId);
                break;
            case HeartbeatOp o:
                w.Write((byte)OpCode.Heartbeat);
                w.Write(o.ServerId);
                break;
            case RemoveTimedOutServersOp o:
                w.Write((byte)OpCode.RemoveTimedOutServers);
                w.Write(o.Timeout.Ticks);
                break;
            case TryAcquireLockOp o:
                w.Write((byte)OpCode.TryAcquireLock);
                w.Write(o.Resource);
                WriteGuid(w, o.Owner);
                w.Write(o.Lease.Ticks);
                break;
            case ReleaseLockOp o:
                w.Write((byte)OpCode.ReleaseLock);
                w.Write(o.Resource);
                WriteGuid(w, o.Owner);
                break;
            case MaintenanceOp o:
                w.Write((byte)OpCode.Maintenance);
                w.Write(o.FetchInvisibilityTimeout.Ticks);
                break;
            default:
                throw new NotSupportedException($"Op {op.GetType().Name} has no serializer.");
        }
    }

    private static StoreOp ReadOp(BinaryReader r)
    {
        var code = (OpCode)r.ReadByte();
        return code switch
        {
            OpCode.CreateJob => new CreateJobOp(r.ReadString(), r.ReadString(), ReadPairs(r), ReadDate(r), ReadDate(r)),
            OpCode.SetJobParameter => new SetJobParameterOp(r.ReadString(), r.ReadString(), ReadNullableString(r)),
            OpCode.SetJobState => new SetJobStateOp(r.ReadString(), ReadState(r)),
            OpCode.AddJobState => new AddJobStateOp(r.ReadString(), ReadState(r)),
            OpCode.ExpireJob => new ExpireJobOp(r.ReadString(), ReadDate(r)),
            OpCode.PersistJob => new PersistJobOp(r.ReadString()),
            OpCode.Enqueue => new EnqueueOp(r.ReadString(), r.ReadString()),
            OpCode.Fetch => new FetchOp(ReadStrings(r), ReadGuid(r)),
            OpCode.AckFetched => new AckFetchedOp(ReadGuid(r)),
            OpCode.RequeueFetched => new RequeueFetchedOp(ReadGuid(r)),
            OpCode.RenewFetched => new RenewFetchedOp(ReadGuid(r)),
            OpCode.IncrementCounter => new IncrementCounterOp(r.ReadString(), r.ReadInt64(), ReadNullableDate(r)),
            OpCode.AddToSet => new AddToSetOp(r.ReadString(), r.ReadString(), r.ReadDouble()),
            OpCode.AddRangeToSet => new AddRangeToSetOp(r.ReadString(), ReadStrings(r)),
            OpCode.RemoveFromSet => new RemoveFromSetOp(r.ReadString(), r.ReadString()),
            OpCode.RemoveSet => new RemoveSetOp(r.ReadString()),
            OpCode.ExpireSet => new ExpireSetOp(r.ReadString(), ReadDate(r)),
            OpCode.PersistSet => new PersistSetOp(r.ReadString()),
            OpCode.InsertToList => new InsertToListOp(r.ReadString(), r.ReadString()),
            OpCode.RemoveFromList => new RemoveFromListOp(r.ReadString(), r.ReadString()),
            OpCode.TrimList => new TrimListOp(r.ReadString(), r.ReadInt32(), r.ReadInt32()),
            OpCode.ExpireList => new ExpireListOp(r.ReadString(), ReadDate(r)),
            OpCode.PersistList => new PersistListOp(r.ReadString()),
            OpCode.SetRangeInHash => new SetRangeInHashOp(r.ReadString(), ReadPairs(r)),
            OpCode.RemoveHash => new RemoveHashOp(r.ReadString()),
            OpCode.ExpireHash => new ExpireHashOp(r.ReadString(), ReadDate(r)),
            OpCode.PersistHash => new PersistHashOp(r.ReadString()),
            OpCode.AnnounceServer => new AnnounceServerOp(r.ReadString(), r.ReadInt32(), ReadStrings(r)),
            OpCode.RemoveServer => new RemoveServerOp(r.ReadString()),
            OpCode.Heartbeat => new HeartbeatOp(r.ReadString()),
            OpCode.RemoveTimedOutServers => new RemoveTimedOutServersOp(new TimeSpan(r.ReadInt64())),
            OpCode.TryAcquireLock => new TryAcquireLockOp(r.ReadString(), ReadGuid(r), new TimeSpan(r.ReadInt64())),
            OpCode.ReleaseLock => new ReleaseLockOp(r.ReadString(), ReadGuid(r)),
            OpCode.Maintenance => new MaintenanceOp(new TimeSpan(r.ReadInt64())),
            _ => throw new NotSupportedException($"Unknown op code {code}."),
        };
    }

}
