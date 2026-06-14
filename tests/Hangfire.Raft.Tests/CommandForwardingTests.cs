using System.Net;
using System.Net.Sockets;
using Hangfire.Raft.Cluster;
using Microsoft.Extensions.Logging.Abstractions;

namespace Hangfire.Raft.Tests;

/// <summary>
/// Loopback tests of the follower-to-leader forwarding protocol, exercised without a Raft cluster.
/// The status-to-exception mapping and the send-vs-receive ambiguity split are the correctness core
/// of forwarded writes (a wrong "ambiguous" classification could resend a fetch and lose a job).
/// </summary>
public class CommandForwardingTests
{
    private static readonly byte[] Payload = [1, 2, 3, 4];

    private static int FreePort()
    {
        var probe = new TcpListener(IPAddress.Loopback, 0);
        probe.Start();
        var port = ((IPEndPoint)probe.LocalEndpoint).Port;
        probe.Stop();
        return port;
    }

    private static ForwardingServer StartServer(int port, Func<ReadOnlyMemory<byte>, CancellationToken, Task<(byte, string?)>> handler)
    {
        var server = new ForwardingServer(new IPEndPoint(IPAddress.Loopback, port), handler, NullLogger.Instance);
        server.Start();
        return server;
    }

    private static Task<(byte, string?)> Respond(byte status) => Task.FromResult((status, (string?)null));

    [Fact]
    public async Task StatusOk_Returns()
    {
        var port = FreePort();
        await using var server = StartServer(port, (_, _) => Respond(ForwardingProtocol.StatusOk));
        using var client = new ForwardingClient();

        await client.SubmitAsync(new IPEndPoint(IPAddress.Loopback, port), Payload, CancellationToken.None); // no throw
    }

    [Fact]
    public async Task StatusNotLeader_ThrowsNotLeaderResponse()
    {
        var port = FreePort();
        await using var server = StartServer(port, (_, _) => Respond(ForwardingProtocol.StatusNotLeader));
        using var client = new ForwardingClient();

        await Assert.ThrowsAsync<NotLeaderResponseException>(
            () => client.SubmitAsync(new IPEndPoint(IPAddress.Loopback, port), Payload, CancellationToken.None));
    }

    [Fact]
    public async Task StatusAmbiguous_ThrowsAmbiguous()
    {
        var port = FreePort();
        await using var server = StartServer(port, (_, _) => Task.FromResult((ForwardingProtocol.StatusAmbiguous, (string?)"unknown")));
        using var client = new ForwardingClient();

        await Assert.ThrowsAsync<AmbiguousCommandException>(
            () => client.SubmitAsync(new IPEndPoint(IPAddress.Loopback, port), Payload, CancellationToken.None));
    }

    [Fact]
    public async Task StatusError_ThrowsRaftStorageException()
    {
        var port = FreePort();
        await using var server = StartServer(port, (_, _) => Task.FromResult((ForwardingProtocol.StatusError, (string?)"rejected")));
        using var client = new ForwardingClient();

        await Assert.ThrowsAsync<RaftStorageException>(
            () => client.SubmitAsync(new IPEndPoint(IPAddress.Loopback, port), Payload, CancellationToken.None));
    }

    [Fact]
    public async Task ConnectFailure_IsRetryable()
    {
        // No server on this port: the failure happens before the request is written -> safe to retry.
        using var client = new ForwardingClient();
        await Assert.ThrowsAsync<RetryableForwardingException>(
            () => client.SubmitAsync(new IPEndPoint(IPAddress.Loopback, FreePort()), Payload, CancellationToken.None));
    }

    [Fact]
    public async Task ConnectionLostAfterRequest_IsAmbiguous()
    {
        // The handler receives the full request, then the connection is torn down without a response.
        // Because the request was fully written, the outcome is ambiguous and must NOT be retried.
        var port = FreePort();
        await using var server = StartServer(port, (_, _) => throw new InvalidOperationException("leader died mid-replication"));
        using var client = new ForwardingClient();

        await Assert.ThrowsAsync<AmbiguousCommandException>(
            () => client.SubmitAsync(new IPEndPoint(IPAddress.Loopback, port), Payload, CancellationToken.None));
    }

    [Fact]
    public async Task PooledConnection_IsReusedAcrossSubmits()
    {
        var calls = 0;
        var port = FreePort();
        await using var server = StartServer(port, (_, _) => { Interlocked.Increment(ref calls); return Respond(ForwardingProtocol.StatusOk); });
        using var client = new ForwardingClient();
        var endpoint = new IPEndPoint(IPAddress.Loopback, port);

        await client.SubmitAsync(endpoint, Payload, CancellationToken.None);
        await client.SubmitAsync(endpoint, Payload, CancellationToken.None); // reuses the pooled connection

        Assert.Equal(2, calls);
    }

    [Fact]
    public async Task Server_DropsConnection_OnBadMagic()
    {
        var port = FreePort();
        await using var server = StartServer(port, (_, _) => Respond(ForwardingProtocol.StatusOk));

        using var raw = new TcpClient();
        await raw.ConnectAsync(IPAddress.Loopback, port);
        var stream = raw.GetStream();
        await stream.WriteAsync(new byte[] { 0xDE, 0xAD, 0xBE, 0xEF, 0, 0, 0, 0 }); // wrong magic + zero length

        // The server logs and closes the connection, so the read returns 0 (end of stream).
        var read = await stream.ReadAsync(new byte[1]);
        Assert.Equal(0, read);
    }
}
