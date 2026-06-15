namespace Hangfire.Raft.Tests;

/// <summary>
/// Background-job target shared by the storage tests. Several test classes enqueue
/// <see cref="Run"/> as a job body, so it lives in its own file rather than being owned by one of them.
/// </summary>
public static class TestJobs
{
    public static void Run(string argument)
    {
    }
}
