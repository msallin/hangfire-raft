namespace Hangfire.Raft.Tests;

public class RaftStorageExceptionTests
{
    [Test]
    public async Task MessageConstructor_SetsMessage()
    {
        var ex = new RaftStorageException("boom");
        await Assert.That(ex.Message).IsEqualTo("boom");
        await Assert.That(ex.InnerException).IsNull();
    }

    [Test]
    public async Task InnerConstructor_SetsMessageAndInner()
    {
        var inner = new InvalidOperationException("cause");
        var ex = new RaftStorageException("boom", inner);
        await Assert.That(ex.Message).IsEqualTo("boom");
        await Assert.That(ex.InnerException).IsSameReferenceAs(inner);
    }
}
