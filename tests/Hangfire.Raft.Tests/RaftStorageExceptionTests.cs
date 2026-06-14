namespace Hangfire.Raft.Tests;

public class RaftStorageExceptionTests
{
    [Fact]
    public void MessageConstructor_SetsMessage()
    {
        var ex = new RaftStorageException("boom");
        Assert.Equal("boom", ex.Message);
        Assert.Null(ex.InnerException);
    }

    [Fact]
    public void InnerConstructor_SetsMessageAndInner()
    {
        var inner = new InvalidOperationException("cause");
        var ex = new RaftStorageException("boom", inner);
        Assert.Equal("boom", ex.Message);
        Assert.Same(inner, ex.InnerException);
    }
}
