using System.Text;
using Hangfire.Raft.Commands;
using TUnit.Assertions.Enums;

namespace Hangfire.Raft.Tests;

/// <summary>
/// Failure-path coverage for the wire format. These guards are what let HangfireStateMachine.ApplyAsync
/// detect an undecodable committed entry and fail fast (so the node re-syncs from the leader) rather
/// than silently diverging, and what bounds allocation against a hostile length prefix.
/// </summary>
public class CommandSerializerFailureTests
{
    private const byte Magic = 0xC1; // matches CommandSerializer.Magic

    private static readonly DateTime T0 = new(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    [Test]
    public async Task TryDeserialize_Throws_OnUnsupportedVersion()
    {
        var bytes = CommandSerializer.Serialize(Command.Single(new PersistJobOp("j")));
        bytes[1] = 0xFE; // corrupt the version byte (index 1, right after the magic)
        await Assert.That(() => CommandSerializer.TryDeserialize(bytes)).ThrowsExactly<NotSupportedException>();
    }

    [Test]
    public async Task TryDeserialize_Throws_OnTruncatedPayload()
    {
        var bytes = CommandSerializer.Serialize(Command.Single(new CreateJobOp("j", "data", [], T0, T0)));
        await Assert.That(() => CommandSerializer.TryDeserialize(bytes[..^3])).Throws<Exception>(); // EndOfStreamException
    }

    [Test]
    public async Task TryDeserialize_Throws_OnUnknownOpcode()
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

        await Assert.That(() => CommandSerializer.TryDeserialize(ms.ToArray())).ThrowsExactly<NotSupportedException>();
    }

    [Test]
    public async Task ReadCount_Rejects_CountLargerThanRemainingBytes()
    {
        using var ms = new MemoryStream();
        using (var w = new BinaryWriter(ms, Encoding.UTF8, leaveOpen: true)) w.Write7BitEncodedInt(int.MaxValue);
        ms.Position = 0;
        using var r = new BinaryReader(ms);
        await Assert.That(() => BinaryFormat.ReadCount(r)).ThrowsExactly<InvalidDataException>();
    }

    [Test]
    public async Task ReadCount_Accepts_CountUpToRemainingBytes()
    {
        using var ms = new MemoryStream();
        using (var w = new BinaryWriter(ms, Encoding.UTF8, leaveOpen: true))
        {
            w.Write7BitEncodedInt(3);
            w.Write(new byte[3]); // exactly three bytes follow
        }
        ms.Position = 0;
        using var r = new BinaryReader(ms);
        await Assert.That(BinaryFormat.ReadCount(r)).IsEqualTo(3);
    }

    [Test]
    public async Task Roundtrip_HandlesLargeMultibyteStrings_AndEmptyCollections()
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
        await Assert.That(restored.Ops[0]).IsTypeOf<CreateJobOp>();
        var createJobOp = (CreateJobOp)restored.Ops[0];
        await Assert.That(createJobOp.InvocationData).IsEqualTo(big);
        await Assert.That(CommandSerializer.Serialize(restored)).IsEquivalentTo(bytes, CollectionOrdering.Matching); // byte-stable through the multibyte/empty paths
    }
}
