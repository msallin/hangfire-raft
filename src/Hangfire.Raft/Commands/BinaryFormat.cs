using System.Runtime.InteropServices;

namespace Hangfire.Raft.Commands;

/// <summary>
/// Shared low-level binary primitives used by the command serializer and the store snapshot format.
/// Strings use BinaryWriter's 7-bit length prefix; nullable values carry a presence flag;
/// DateTimes are stored as UTC ticks.
/// </summary>
internal static class BinaryFormat
{
    public static void WriteStrings(BinaryWriter w, IReadOnlyList<string> values)
    {
        w.Write7BitEncodedInt(values.Count);
        foreach (var v in values) w.Write(v);
    }

    public static IReadOnlyList<string> ReadStrings(BinaryReader r)
    {
        var count = ReadCount(r);
        var result = new string[count];
        for (var i = 0; i < count; i++) result[i] = r.ReadString();
        return result;
    }

    public static void WritePairs(BinaryWriter w, IReadOnlyList<KeyValuePair<string, string?>> pairs)
    {
        w.Write7BitEncodedInt(pairs.Count);
        foreach (var (key, value) in pairs)
        {
            w.Write(key);
            WriteNullable(w, value);
        }
    }

    public static IReadOnlyList<KeyValuePair<string, string?>> ReadPairs(BinaryReader r)
    {
        var count = ReadCount(r);
        var result = new KeyValuePair<string, string?>[count];
        for (var i = 0; i < count; i++) result[i] = new(r.ReadString(), ReadNullableString(r));
        return result;
    }

    public static void WriteState(BinaryWriter w, StateRecord state)
    {
        w.Write(state.Name);
        WriteNullable(w, state.Reason);
        WritePairs(w, state.Data);
        w.Write(state.CreatedAt.Ticks);
    }

    public static StateRecord ReadState(BinaryReader r) => new(r.ReadString(), ReadNullableString(r), ReadPairs(r), ReadDate(r));

    public static void WriteNullable(BinaryWriter w, string? value)
    {
        w.Write(value is not null);
        if (value is not null) w.Write(value);
    }

    public static string? ReadNullableString(BinaryReader r) => r.ReadBoolean() ? r.ReadString() : null;

    public static void WriteNullable(BinaryWriter w, DateTime? value)
    {
        w.Write(value.HasValue);
        if (value.HasValue) w.Write(value.Value.Ticks);
    }

    public static DateTime? ReadNullableDate(BinaryReader r) => r.ReadBoolean() ? ReadDate(r) : null;

    public static DateTime ReadDate(BinaryReader r) => new(r.ReadInt64(), DateTimeKind.Utc);

    public static void WriteGuid(BinaryWriter w, Guid value)
    {
        Span<byte> buffer = stackalloc byte[16];
        value.TryWriteBytes(buffer);
        w.Write(buffer);
    }

    public static Guid ReadGuid(BinaryReader r) => new(r.ReadBytes(16));

    /// <summary>
    /// Reads a collection-count prefix and rejects values that cannot possibly be backed by the
    /// bytes left in the payload. Every element costs at least one byte on the wire, so a count
    /// larger than the remaining length is corrupt or hostile; without this guard a tiny payload
    /// claiming count=int.MaxValue would drive a multi-gigabyte array allocation on every applying
    /// node. Callers must read from a seekable stream (the command and snapshot readers do).
    /// This guards collection prefixes that drive an up-front allocation; <see cref="BinaryReader.ReadString"/>
    /// has its own 7-bit length prefix but reads in fixed-size chunks (no pre-allocation from the
    /// prefix), so a hostile string length fails with EndOfStreamException rather than an OOM and does
    /// not need to pass through here.
    /// </summary>
    public static int ReadCount(BinaryReader r)
    {
        var count = r.Read7BitEncodedInt();
        var remaining = r.BaseStream.Length - r.BaseStream.Position;
        if (count < 0 || count > remaining)
            throw new InvalidDataException($"Element count {count} exceeds the {remaining} bytes remaining in the payload.");
        return count;
    }

    /// <summary>Read-only stream over the payload without copying when it is array-backed (the usual case for log entries).</summary>
    public static MemoryStream CreateReadStream(ReadOnlyMemory<byte> payload)
        => MemoryMarshal.TryGetArray(payload, out var segment)
            ? new MemoryStream(segment.Array!, segment.Offset, segment.Count, writable: false)
            : new MemoryStream(payload.ToArray(), writable: false);
}
