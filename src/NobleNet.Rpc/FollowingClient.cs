using System.Net.WebSockets;

namespace NobleNet.Rpc;

/// <summary>
/// Keeps a client on the current primary. A handoff frame replaces the socket
/// and the same <see cref="ProgramRegistry"/> is exported on the new one, so a
/// shout target does not have to be registered again by the caller. A drop
/// without a handoff retries the last known endpoint.
/// </summary>
public sealed class FollowingClient : IAsyncDisposable
{
    private readonly ProgramRegistry _programs;
    private readonly Authenticator? _authenticate;
    private readonly object _gate = new();
    private OneDriver? _session;
    private int _moving;

    private FollowingClient(Uri endpoint, ProgramRegistry programs, Authenticator? authenticate, OneDriver session)
    {
        Endpoint = endpoint;
        _programs = programs;
        _authenticate = authenticate;
        _session = session;
        Watch(session);
    }

    public Uri Endpoint { get; private set; }
    public OneDriver Session => _session ?? throw new NobleNetException("closed", "not connected");

    public event Action<Uri>? Moved;

    public static async Task<FollowingClient> ConnectAsync(
        Uri endpoint,
        ProgramRegistry? localPrograms = null,
        Authenticator? authenticate = null,
        CancellationToken cancellationToken = default)
    {
        var programs = localPrograms ?? new ProgramRegistry();
        var session = await OpenAsync(endpoint, programs, authenticate, cancellationToken).ConfigureAwait(false);
        return new FollowingClient(endpoint, programs, authenticate, session);
    }

    public async ValueTask DisposeAsync()
    {
        OneDriver? session;
        lock (_gate)
        {
            session = _session;
            _session = null;
        }
        if (session is not null)
            await session.DisposeAsync().ConfigureAwait(false);
    }

    private void Watch(OneDriver session)
    {
        session.Handoff += next => { _ = FollowAsync(next); };
        session.Closed += closed => { _ = RetryAsync(closed); };
    }

    private async Task FollowAsync(Uri next)
    {
        if (Interlocked.Exchange(ref _moving, 1) == 1)
            return;
        try
        {
            OneDriver? previous;
            lock (_gate)
                previous = _session;
            if (previous is not null)
                await previous.DisposeAsync().ConfigureAwait(false);
            var session = await OpenAsync(next, _programs, _authenticate, CancellationToken.None).ConfigureAwait(false);
            lock (_gate)
                _session = session;
            Endpoint = next;
            Watch(session);
            Moved?.Invoke(next);
        }
        finally
        {
            Interlocked.Exchange(ref _moving, 0);
        }
    }

    private async Task RetryAsync(OneDriver closed)
    {
        if (Volatile.Read(ref _moving) == 1)
            return;
        lock (_gate)
        {
            if (!ReferenceEquals(_session, closed))
                return;
        }
        if (Interlocked.Exchange(ref _moving, 1) == 1)
            return;
        try
        {
            for (var attempt = 0; attempt < 5; attempt++)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(200 * (attempt + 1))).ConfigureAwait(false);
                try
                {
                    var session = await OpenAsync(Endpoint, _programs, _authenticate, CancellationToken.None).ConfigureAwait(false);
                    lock (_gate)
                        _session = session;
                    Watch(session);
                    Moved?.Invoke(Endpoint);
                    return;
                }
                catch (WebSocketException)
                {
                }
            }
        }
        finally
        {
            Interlocked.Exchange(ref _moving, 0);
        }
    }

    private static async Task<OneDriver> OpenAsync(
        Uri endpoint,
        ProgramRegistry programs,
        Authenticator? authenticate,
        CancellationToken cancellationToken)
    {
        var socket = new ClientWebSocket();
        await socket.ConnectAsync(endpoint, cancellationToken).ConfigureAwait(false);
        var session = new OneDriver(socket, programs, authenticate, "client");
        session.Start();
        return session;
    }
}
