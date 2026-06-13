using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Text;
using Microsoft.Extensions.Logging;

namespace Hangfire.Raft.Cluster;

/// <summary>
/// Minimal TCP channel that lets follower nodes forward write commands to the current leader.
/// DotNext's raw transport carries only Raft traffic, so command forwarding needs its own port
/// (Raft port + RpcPortOffset).
///
/// Wire format, all integers little-endian:
///   request:  [4B magic "HFR1"][4B payload length][payload]
///   response: [1B status][4B message length][UTF-8 message]
///
/// The status separates outcomes by what the submitter may safely do next: NotLeader and Error
/// mean the command was definitely not appended (safe to retry), Ambiguous means the append may
/// or may not have been committed (must NOT be resent; the submitter resolves the outcome by
/// waiting for its local apply).
/// </summary>
internal static class ForwardingProtocol
{
    public static ReadOnlySpan<byte> Magic => "HFR1"u8;
    public const int MaxPayloadLength = 64 * 1024 * 1024;

    public const byte StatusOk = 0;
    public const byte StatusNotLeader = 1;
    public const byte StatusError = 2;
    public const byte StatusAmbiguous = 3;
}

/// <summary>
/// The contacted node is not the leader and did not append the command; the submit loop
/// re-resolves the leader and retries. Distinct from DotNext's NotLeaderException, which the
/// retry logic matches separately.
/// </summary>
internal sealed class NotLeaderResponseException : Exception
{
    public NotLeaderResponseException() : base("The contacted node is not the cluster leader.")
    {
    }
}

/// <summary>The command was definitely not sent or appended; resending it is safe.</summary>
internal sealed class RetryableForwardingException : Exception
{
    public RetryableForwardingException(string message, Exception inner) : base(message, inner)
    {
    }
}

/// <summary>
/// The command reached the leader, but whether it was committed is unknown (connection lost while
/// waiting for the acknowledgement, or replication aborted mid-flight). It must not be resent: a
/// second committed copy of a non-idempotent op (e.g. a fetch) could lose jobs. The submitter
/// waits for its local apply to learn the actual outcome.
/// </summary>
internal sealed class AmbiguousCommandException : Exception
{
    public AmbiguousCommandException(string message, Exception? inner = null) : base(message, inner)
    {
    }
}

internal sealed class ForwardingServer : IAsyncDisposable
{
    private readonly TcpListener _listener;
    private readonly Func<ReadOnlyMemory<byte>, CancellationToken, Task<(byte Status, string? Message)>> _handler;
    private readonly ILogger _logger;
    private readonly CancellationTokenSource _lifetime = new();
    private Task? _acceptLoop;

    public ForwardingServer(
        IPEndPoint endpoint,
        Func<ReadOnlyMemory<byte>, CancellationToken, Task<(byte Status, string? Message)>> handler,
        ILogger logger)
    {
        _listener = new TcpListener(endpoint);
        _handler = handler;
        _logger = logger;
    }

    public void Start()
    {
        _listener.Start();
        _acceptLoop = AcceptLoop();
    }

    private async Task AcceptLoop()
    {
        while (!_lifetime.IsCancellationRequested)
        {
            TcpClient client;
            try
            {
                client = await _listener.AcceptTcpClientAsync(_lifetime.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return;
            }
            catch (Exception ex)
            {
                if (_lifetime.IsCancellationRequested) return;
                _logger.LogWarning(ex, "Command forwarding listener failed to accept a connection.");
                continue;
            }

            _ = ServeConnection(client);
        }
    }

    private async Task ServeConnection(TcpClient client)
    {
        using var _ = client;
        client.NoDelay = true;
        var stream = client.GetStream();
        var header = new byte[8];

        try
        {
            while (!_lifetime.IsCancellationRequested)
            {
                try
                {
                    await stream.ReadExactlyAsync(header, _lifetime.Token).ConfigureAwait(false);
                }
                catch (EndOfStreamException)
                {
                    return; // client closed the connection between requests
                }

                if (!header.AsSpan(0, 4).SequenceEqual(ForwardingProtocol.Magic))
                {
                    _logger.LogWarning("Command forwarding connection from {Remote} sent an invalid header.", client.Client.RemoteEndPoint);
                    return;
                }

                var length = BinaryPrimitives.ReadInt32LittleEndian(header.AsSpan(4));
                if (length is < 0 or > ForwardingProtocol.MaxPayloadLength)
                {
                    _logger.LogWarning("Command forwarding connection from {Remote} sent an oversized payload ({Length} bytes).", client.Client.RemoteEndPoint, length);
                    return;
                }

                var payload = new byte[length];
                await stream.ReadExactlyAsync(payload, _lifetime.Token).ConfigureAwait(false);

                var (status, message) = await _handler(payload, _lifetime.Token).ConfigureAwait(false);

                var messageBytes = message is null ? [] : Encoding.UTF8.GetBytes(message);
                var response = new byte[1 + 4 + messageBytes.Length];
                response[0] = status;
                BinaryPrimitives.WriteInt32LittleEndian(response.AsSpan(1), messageBytes.Length);
                messageBytes.CopyTo(response.AsSpan(5));
                await stream.WriteAsync(response, _lifetime.Token).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Command forwarding connection from {Remote} failed.", client.Client.RemoteEndPoint);
        }
    }

    public async ValueTask DisposeAsync()
    {
        await _lifetime.CancelAsync().ConfigureAwait(false);
        _listener.Stop();
        if (_acceptLoop is not null)
        {
            try
            {
                await _acceptLoop.ConfigureAwait(false);
            }
            catch (Exception)
            {
                // the loop already logged; shutdown must not throw
            }
        }

        _lifetime.Dispose();
    }
}

internal sealed class ForwardingClient : IDisposable
{
    // Keyed by EndPoint so a DnsEndPoint and an IPEndPoint pool independently. Connecting by a
    // DnsEndPoint re-resolves it each time, so a pooled connection to a rescheduled peer's old IP
    // is dropped on first failure and the next attempt reaches the new IP.
    private readonly ConcurrentDictionary<EndPoint, ConcurrentBag<TcpClient>> _pool = new();
    private volatile bool _disposed;

    /// <summary>Sends a command to the given node and waits for the commit acknowledgement.</summary>
    public async Task SubmitAsync(EndPoint endpoint, ReadOnlyMemory<byte> payload, CancellationToken token)
    {
        TcpClient? client = null;
        var requestFullyWritten = false;
        try
        {
            client = await RentAsync(endpoint, token).ConfigureAwait(false);
            var stream = client.GetStream();

            // Send phase: a failure here means the leader saw at most a partial frame, which its
            // framing discards, so the command was not appended and resending is safe.
            var header = new byte[8];
            ForwardingProtocol.Magic.CopyTo(header);
            BinaryPrimitives.WriteInt32LittleEndian(header.AsSpan(4), payload.Length);
            await stream.WriteAsync(header, token).ConfigureAwait(false);
            await stream.WriteAsync(payload, token).ConfigureAwait(false);
            requestFullyWritten = true;

            // Receive phase: the leader has the full command; a failure here is ambiguous.
            var responseHeader = new byte[5];
            await stream.ReadExactlyAsync(responseHeader, token).ConfigureAwait(false);
            var status = responseHeader[0];
            var messageLength = BinaryPrimitives.ReadInt32LittleEndian(responseHeader.AsSpan(1));
            if (messageLength is < 0 or > ForwardingProtocol.MaxPayloadLength) throw new IOException("Invalid forwarding response.");
            var message = Array.Empty<byte>();
            if (messageLength > 0)
            {
                message = new byte[messageLength];
                await stream.ReadExactlyAsync(message, token).ConfigureAwait(false);
            }

            Return(endpoint, client);
            client = null;

            switch (status)
            {
                case ForwardingProtocol.StatusOk:
                    return;
                case ForwardingProtocol.StatusNotLeader:
                    throw new NotLeaderResponseException();
                case ForwardingProtocol.StatusAmbiguous:
                    throw new AmbiguousCommandException($"The leader could not confirm the command: {Encoding.UTF8.GetString(message)}");
                default:
                    throw new RaftStorageException($"The leader rejected the command: {Encoding.UTF8.GetString(message)}");
            }
        }
        catch (Exception ex) when (ex is not (NotLeaderResponseException or AmbiguousCommandException or RaftStorageException or OperationCanceledException))
        {
            throw requestFullyWritten
                ? new AmbiguousCommandException("The connection to the leader was lost while waiting for the acknowledgement.", ex)
                : new RetryableForwardingException($"Could not deliver the command to the leader at {endpoint}.", ex);
        }
        finally
        {
            client?.Dispose(); // only on failure paths; successful calls return the connection to the pool
        }
    }

    private async Task<TcpClient> RentAsync(EndPoint endpoint, CancellationToken token)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_pool.TryGetValue(endpoint, out var bag))
        {
            while (bag.TryTake(out var pooled))
            {
                if (IsAlive(pooled)) return pooled;
                pooled.Dispose();
            }
        }

        var client = new TcpClient();
        try
        {
            client.NoDelay = true;
            // Connecting a DnsEndPoint by host name re-resolves it (so a peer's new IP is picked up);
            // an IPEndPoint connects directly.
            if (endpoint is DnsEndPoint dns)
                await client.ConnectAsync(dns.Host, dns.Port, token).ConfigureAwait(false);
            else
                await client.ConnectAsync((IPEndPoint)endpoint, token).ConfigureAwait(false);
            return client;
        }
        catch
        {
            client.Dispose();
            throw;
        }
    }

    /// <summary>
    /// A pooled connection is dead when the peer closed it: the socket then selects as readable
    /// with no buffered data. <c>TcpClient.Connected</c> alone only reflects the last operation.
    /// </summary>
    private static bool IsAlive(TcpClient client)
    {
        try
        {
            return client.Connected
                   && !(client.Client.Poll(0, SelectMode.SelectRead) && client.Available == 0);
        }
        catch (Exception)
        {
            return false;
        }
    }

    private void Return(EndPoint endpoint, TcpClient client)
    {
        _pool.GetOrAdd(endpoint, static _ => []).Add(client);

        // Re-check after adding: Dispose may have drained the pool between the add and the flag
        // flip, which would otherwise leak this connection until process exit.
        if (_disposed) DrainPool();
    }

    public void Dispose()
    {
        _disposed = true;
        DrainPool();
    }

    private void DrainPool()
    {
        foreach (var bag in _pool.Values)
        {
            while (bag.TryTake(out var client)) client.Dispose();
        }
    }
}
