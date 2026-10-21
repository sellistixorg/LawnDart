using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using System.Text;

namespace LawnDart.Backends.Contract.Tests;

/// <summary>
/// Sits in front of Postgres and injects one closure error on the append call.
/// One mode drops the call before Postgres sees it. The other lets the commit
/// finish, drops the ready message, and reports a fatal protocol error.
/// </summary>
sealed class PostgresClosureProxy : IAsyncDisposable
{
    public enum Mode
    {
        BeforeAppendReachesServer,
        AfterCommit
    }

    const int SslRequestCode = 80877103;
    const byte ReadyForQuery = (byte)'Z';
    const byte CommandComplete = (byte)'C';
    static readonly byte[] AppendCall = "SELECT global_position, version"u8.ToArray();

    readonly TcpListener _listener;
    readonly string _upstreamHost;
    readonly int _upstreamPort;
    readonly Mode _mode;
    readonly CancellationTokenSource _stop = new();
    int _armed;
    int _skip;
    int _poisoned;
    Task? _accept;

    public PostgresClosureProxy(string upstreamHost, int upstreamPort, Mode mode)
    {
        _upstreamHost = upstreamHost;
        _upstreamPort = upstreamPort;
        _mode = mode;
        _listener = new TcpListener(IPAddress.Loopback, 0);
    }

    public int Port { get; private set; }

    public async Task StartAsync()
    {
        _listener.Start();
        Port = ((IPEndPoint)_listener.LocalEndpoint).Port;
        _accept = Task.Run(AcceptAsync);
        await Task.CompletedTask;
    }

    /// <summary>
    /// Start injecting on the next append call. Earlier append calls are forwarded.
    /// </summary>
    public void Arm(int appendsToForwardFirst = 0)
    {
        Volatile.Write(ref _skip, appendsToForwardFirst);
        Volatile.Write(ref _armed, 1);
    }

    public async ValueTask DisposeAsync()
    {
        _stop.Cancel();
        _listener.Stop();
        if (_accept is not null)
        {
            try
            {
                await _accept.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
            }
            catch (ObjectDisposedException)
            {
            }
        }

        _stop.Dispose();
    }

    async Task AcceptAsync()
    {
        while (!_stop.IsCancellationRequested)
        {
            TcpClient client;
            try
            {
                client = await _listener.AcceptTcpClientAsync(_stop.Token).ConfigureAwait(false);
            }
            catch (Exception) when (_stop.IsCancellationRequested)
            {
                return;
            }

            _ = Task.Run(() => RelayAsync(client));
        }
    }

    async Task RelayAsync(TcpClient client)
    {
        using var _ = client;
        using var server = new TcpClient();
        try
        {
            await server.ConnectAsync(_upstreamHost, _upstreamPort, _stop.Token).ConfigureAwait(false);
            var clientStream = client.GetStream();
            var serverStream = server.GetStream();
            await ForwardStartupAsync(clientStream, serverStream).ConfigureAwait(false);

            using var relayStop = CancellationTokenSource.CreateLinkedTokenSource(_stop.Token);
            var state = new RelayState();
            var gate = new SemaphoreSlim(1, 1);
            var inbound = PumpClientToServerAsync(clientStream, serverStream, state, gate, relayStop);
            var outbound = PumpServerToClientAsync(clientStream, serverStream, state, gate, relayStop);
            await Task.WhenAll(inbound, outbound).ConfigureAwait(false);
        }
        catch (Exception) when (_stop.IsCancellationRequested)
        {
        }
        catch (IOException)
        {
        }
        catch (SocketException)
        {
        }
    }

    async Task PumpClientToServerAsync(
        NetworkStream clientStream,
        NetworkStream serverStream,
        RelayState state,
        SemaphoreSlim gate,
        CancellationTokenSource relayStop)
    {
        try
        {
            while (!relayStop.IsCancellationRequested)
            {
                var message = await ReadMessageAsync(clientStream, relayStop.Token).ConfigureAwait(false);
                if (IsAppendCall(message) && ShouldPoison())
                {
                    if (_mode == Mode.BeforeAppendReachesServer)
                    {
                        await FailAsync(clientStream, gate, "57P01", "terminating connection due to administrator command", relayStop)
                            .ConfigureAwait(false);
                        return;
                    }

                    state.SwallowReady = true;
                }

                await serverStream.WriteAsync(message, relayStop.Token).ConfigureAwait(false);
            }
        }
        catch (Exception) when (relayStop.IsCancellationRequested)
        {
        }
    }

    async Task PumpServerToClientAsync(
        NetworkStream clientStream,
        NetworkStream serverStream,
        RelayState state,
        SemaphoreSlim gate,
        CancellationTokenSource relayStop)
    {
        try
        {
            while (!relayStop.IsCancellationRequested)
            {
                var message = await ReadMessageAsync(serverStream, relayStop.Token).ConfigureAwait(false);
                if (state.SwallowReady && message.Length > 0 && message[0] == CommandComplete)
                    state.SawCommandComplete = true;
                if (state.SwallowReady && message.Length > 0 && message[0] == ReadyForQuery)
                {
                    var replace = state.SawCommandComplete;
                    state.SwallowReady = false;
                    state.SawCommandComplete = false;
                    if (replace)
                    {
                        await FailAsync(clientStream, gate, "08P01", "server conn crashed?", relayStop).ConfigureAwait(false);
                        return;
                    }
                }

                await WriteClientAsync(clientStream, gate, message, relayStop.Token).ConfigureAwait(false);
            }
        }
        catch (Exception) when (relayStop.IsCancellationRequested)
        {
        }
    }

    bool ShouldPoison()
    {
        if (Volatile.Read(ref _armed) == 0)
            return false;
        if (Interlocked.Decrement(ref _skip) >= 0)
            return false;
        return Interlocked.Exchange(ref _poisoned, 1) == 0;
    }

    static bool IsAppendCall(byte[] message)
        => message.AsSpan().IndexOf(AppendCall) >= 0;

    static async Task FailAsync(
        NetworkStream clientStream,
        SemaphoreSlim gate,
        string sqlState,
        string message,
        CancellationTokenSource relayStop)
    {
        await WriteClientAsync(clientStream, gate, Fatal(sqlState, message), CancellationToken.None).ConfigureAwait(false);
        relayStop.Cancel();
        clientStream.Close();
    }

    static async Task WriteClientAsync(NetworkStream clientStream, SemaphoreSlim gate, byte[] message, CancellationToken cancellationToken)
    {
        await gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await clientStream.WriteAsync(message, cancellationToken).ConfigureAwait(false);
            await clientStream.FlushAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            gate.Release();
        }
    }

    static async Task ForwardStartupAsync(NetworkStream clientStream, NetworkStream serverStream)
    {
        var lengthBytes = await ReadExactlyAsync(clientStream, 4, CancellationToken.None).ConfigureAwait(false);
        var length = BinaryPrimitives.ReadInt32BigEndian(lengthBytes);
        var rest = await ReadExactlyAsync(clientStream, length - 4, CancellationToken.None).ConfigureAwait(false);
        var first = new byte[length];
        lengthBytes.CopyTo(first, 0);
        rest.CopyTo(first, 4);
        await serverStream.WriteAsync(first).ConfigureAwait(false);
        await serverStream.FlushAsync().ConfigureAwait(false);

        if (length == 8 && BinaryPrimitives.ReadInt32BigEndian(rest) == SslRequestCode)
        {
            var answer = await ReadExactlyAsync(serverStream, 1, CancellationToken.None).ConfigureAwait(false);
            await clientStream.WriteAsync(answer).ConfigureAwait(false);
            await clientStream.FlushAsync().ConfigureAwait(false);
            if (answer[0] == (byte)'S')
                throw new InvalidOperationException("The proxy does not speak TLS. Disable SSL on the client.");

            var startupLength = await ReadExactlyAsync(clientStream, 4, CancellationToken.None).ConfigureAwait(false);
            var startupSize = BinaryPrimitives.ReadInt32BigEndian(startupLength);
            var startupRest = await ReadExactlyAsync(clientStream, startupSize - 4, CancellationToken.None).ConfigureAwait(false);
            var startup = new byte[startupSize];
            startupLength.CopyTo(startup, 0);
            startupRest.CopyTo(startup, 4);
            await serverStream.WriteAsync(startup).ConfigureAwait(false);
            await serverStream.FlushAsync().ConfigureAwait(false);
        }
    }

    static async Task<byte[]> ReadMessageAsync(NetworkStream stream, CancellationToken cancellationToken)
    {
        var header = await ReadExactlyAsync(stream, 5, cancellationToken).ConfigureAwait(false);
        var length = BinaryPrimitives.ReadInt32BigEndian(header.AsSpan(1));
        if (length < 4)
            throw new IOException("Postgres message length is too small.");
        var payload = await ReadExactlyAsync(stream, length - 4, cancellationToken).ConfigureAwait(false);
        var message = new byte[1 + length];
        message[0] = header[0];
        header.AsSpan(1, 4).CopyTo(message.AsSpan(1));
        payload.CopyTo(message.AsSpan(5));
        return message;
    }

    static async Task<byte[]> ReadExactlyAsync(NetworkStream stream, int count, CancellationToken cancellationToken)
    {
        var buffer = new byte[count];
        var read = 0;
        while (read < count)
        {
            var n = await stream.ReadAsync(buffer.AsMemory(read, count - read), cancellationToken).ConfigureAwait(false);
            if (n == 0)
                throw new EndOfStreamException();
            read += n;
        }

        return buffer;
    }

    static byte[] Fatal(string sqlState, string message)
    {
        using var body = new MemoryStream();
        WriteField(body, (byte)'S', "FATAL");
        WriteField(body, (byte)'V', "FATAL");
        WriteField(body, (byte)'C', sqlState);
        WriteField(body, (byte)'M', message);
        body.WriteByte(0);
        var payload = body.ToArray();
        var framed = new byte[1 + 4 + payload.Length];
        framed[0] = (byte)'E';
        BinaryPrimitives.WriteInt32BigEndian(framed.AsSpan(1), 4 + payload.Length);
        payload.CopyTo(framed.AsSpan(5));
        return framed;
    }

    static void WriteField(Stream stream, byte code, string value)
    {
        stream.WriteByte(code);
        var bytes = Encoding.UTF8.GetBytes(value);
        stream.Write(bytes);
        stream.WriteByte(0);
    }

    sealed class RelayState
    {
        public volatile bool SwallowReady;
        public volatile bool SawCommandComplete;
    }
}
