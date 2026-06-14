using System.Text;
using Hangfire.Raft.Commands;

namespace Hangfire.Raft.Tests;

/// <summary>
/// Failure-path coverage for the wire format. These guards are what let HangfireStateMachine.ApplyAsync
/// safely skip an undecodable committed entry instead of faulting (and bricking) the apply pipeline,
/// and what bounds allocation against a hostile length prefix.
/// </summary>
public class CommandSerializerFailureTests
{
    private const byte Magic = 0xC1; // matches CommandSerializer.Magic

    private static readonly DateTime T0 = new(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    [Fact]
    public void TryDeserialize_Throws_OnUnsupportedVersion()
    {
        var bytes = CommandSerializer.Serialize(Command.Single(new PersistJobOp("j")));
        bytes[1] = 0xFE; // corrupt the version byte (index 1, right after the magic)
        Assert.Throws<NotSupportedException>(() => CommandSerializer.TryDeserialize(bytes));
    }

    [Fact]
    public void TryDeserialize_Throws_OnTruncatedPayload()
    {
        var bytes = CommandSerializer.Serialize(Command.Single(new CreateJobOp("j", "data", [], T0, T0)));
        Assert.ThrowsAny<Exception>(() => CommandSerializer.TryDeserialize(bytes[..^3])); // EndOfStreamException
    }

    [Fact]
    public void TryDeserialize_Throws_OnUnknownOpcode()
    {
        using var ms = new MemoryStream();
        using (var w = new BinaryWriter(ms, Encoding.UTF8, leaveOpen: true))
        {
            w.Write(Magic);
            w.Write((byte)1);                          // version
            w.Write(Guid.NewGuid().ToByteArray());     // command id
            w.Write(T0.Ticks);                         // NowUtc
            w.Write7BitEncodedInt(1);                  // op count
            w.Write((byte)0xFF);                       // opcode that does not exist
        }

        Assert.Throws<NotSupportedException>(() => CommandSerializer.TryDeserialize(ms.ToArray()));
    }

    [Fact]
    public void ReadCount_Rejects_CountLargerThanRemainingBytes()
    {
        using var ms = new MemoryStream();
        using (var w = new BinaryWriter(ms, Encoding.UTF8, leaveOpen: true)) w.Write7BitEncodedInt(int.MaxValue);
        ms.Position = 0;
        using var r = new BinaryReader(ms);
        Assert.Throws<InvalidDataException>(() => BinaryFormat.ReadCount(r));
    }

    [Fact]
    public void ReadCount_Accepts_CountUpToRemainingBytes()
    {
        using var ms = new MemoryStream();
        using (var w = new BinaryWriter(ms, Encoding.UTF8, leaveOpen: true))
        {
            w.Write7BitEncodedInt(3);
            w.Write(new byte[3]); // exactly three bytes follow
        }
        ms.Position = 0;
        using var r = new BinaryReader(ms);
        Assert.Equal(3, BinaryFormat.ReadCount(r));
    }

    [Fact]
    public void Roundtrip_HandlesLargeMultibyteStrings_AndEmptyCollections()
    {
        var big = string.Concat(Enumerable.Repeat("emoji-\U0001F600-柳-", 500)); // multi-KB, multibyte UTF-8
        var command = new Command
        {
            Id = Guid.NewGuid(),
            NowUtc = T0,
            Ops =
            [
                new CreateJobOp("j", big, [], T0, T0.AddDays(1)), // empty parameters
                new AddRangeToSetOp("s", []),                     // empty values
                new SetRangeInHashOp("h", []),                    // empty fields
            ],
        };

        var bytes = CommandSerializer.Serialize(command);
        var restored = CommandSerializer.TryDeserialize(bytes)!;
        Assert.Equal(big, Assert.IsType<CreateJobOp>(restored.Ops[0]).InvocationData);
        Assert.Equal(bytes, CommandSerializer.Serialize(restored)); // byte-stable through the multibyte/empty paths
    }
}
