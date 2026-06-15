namespace Hangfire.Raft.Tests;

/// <summary>
/// Retries a failing test up to <paramref name="times"/> times, pausing for a fixed delay before each
/// retry. The real-cluster integration tests boot loopback Raft clusters whose election and lease timing
/// is sensitive to CI thread-pool contention; a short pause lets a transiently overloaded pool settle
/// before the next attempt. A deterministic failure still fails every attempt, so real bugs are not masked.
/// </summary>
public sealed class RetryWithDelayAttribute(int times, int delayMs) : RetryAttribute(times)
{
    public override async Task<bool> ShouldRetry(TestContext context, Exception exception, int currentRetryCount)
    {
        await Task.Delay(delayMs);
        return true;
    }
}
