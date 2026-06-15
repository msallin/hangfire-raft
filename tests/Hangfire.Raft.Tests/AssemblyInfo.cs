using System.Runtime.CompilerServices;

// The integration tests boot real Raft clusters that fsync on every commit (durability-on-commit).
// Running them in parallel with the other tests contends for disk and CPU on a single machine and
// intermittently times out a cluster write - an artifact of co-locating many clusters on one disk,
// not a product issue (a deployment runs one cluster per node). Disable parallelization so the cluster
// tests run without that contention.
[assembly: NotInParallel]

namespace Hangfire.Raft.Tests;

internal static class TestModuleInitializer
{
    // The storage exposes Hangfire's synchronous API over an async Raft cluster, so each write blocks a
    // thread on a sync-over-async Submit while the cluster's apply/flush continuations run on the thread
    // pool. Booting many clusters in a run can outpace the default min thread count (~CPU count), which
    // grows only ~1 thread/sec, so a burst starves the async machinery and a write stalls to the
    // SubmitTimeout. Raise the floor so the test process always has threads for the continuations.
    [ModuleInitializer]
    public static void Init()
    {
        ThreadPool.GetMinThreads(out var worker, out var io);
        ThreadPool.SetMinThreads(Math.Max(worker, 64), Math.Max(io, 64));
    }
}
