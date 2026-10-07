using System.Collections.Concurrent;
using System.Net.WebSockets;

namespace NobleNet.Rpc;

public static class NobleNetClient
{
    public static async Task<OneDriver> ConnectAsync(
        Uri endpoint,
        ProgramRegistry? localPrograms = null,
        Authenticator? authenticate = null,
        CancellationToken cancellationToken = default)
    {
        var socket = new ClientWebSocket();
        await socket.ConnectAsync(endpoint, cancellationToken).ConfigureAwait(false);
        var session = new OneDriver(
            socket,
            localPrograms ?? new ProgramRegistry(),
            authenticate,
            "client");
        session.Start();
        return session;
    }
}

public sealed class SessionEventArgs(OneDriver session) : EventArgs
{
    public OneDriver Session { get; } = session;
}

/// <summary>
/// Accepts WebSocket upgrades and binds each one to a <see cref="OneDriver"/>.
/// The host (Kestrel, HttpListener, a test double) owns the listen socket;
/// this type only owns the RPC sessions.
/// </summary>
public sealed class SessionHub
{
    private readonly ConcurrentDictionary<OneDriver, byte> _sessions = new();

    public SessionHub(ProgramRegistry programs, Authenticator? authenticate = null)
    {
        Programs = programs;
        Authenticate = authenticate ?? AllowAnonymous;
    }

    public ProgramRegistry Programs { get; }
    public Authenticator Authenticate { get; }
    public IReadOnlyCollection<OneDriver> Sessions => _sessions.Keys.ToArray();

    private static ValueTask<AuthDecision> AllowAnonymous(Credential credential, CancellationToken cancellationToken) =>
        new(new AuthDecision.Allow("anonymous"));

    public event EventHandler<SessionEventArgs>? SessionOpened;
    public event EventHandler<SessionEventArgs>? SessionClosed;

    public OneDriver Adopt(WebSocket socket, string remoteId)
    {
        var session = new OneDriver(socket, Programs, Authenticate, remoteId);
        _sessions[session] = 0;
        session.Closed += closed =>
        {
            _sessions.TryRemove(closed, out _);
            SessionClosed?.Invoke(this, new SessionEventArgs(closed));
        };
        session.Start();
        SessionOpened?.Invoke(this, new SessionEventArgs(session));
        return session;
    }
}
