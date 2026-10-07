using System.Collections.Concurrent;
using System.Net.WebSockets;
using System.Text.Json;
using System.Threading.Channels;

namespace NobleNet.Rpc;

/// <summary>
/// One WebSocket, many in-flight calls. Either peer may register programs,
/// so the server can call back into the client on the same connection
/// (the EZ-RPC callback shape). Sends are funneled through a single reader
/// channel because <see cref="WebSocket.SendAsync"/> is not safe to overlap.
/// </summary>
public sealed class OneDriver : IAsyncDisposable
{
    private readonly WebSocket _socket;
    private readonly ProgramRegistry _programs;
    private readonly Authenticator _authenticate;
    private readonly Channel<byte[]> _outbound = Channel.CreateUnbounded<byte[]>(new UnboundedChannelOptions
    {
        SingleReader = true,
        SingleWriter = false,
    });
    private readonly ConcurrentDictionary<uint, Pending> _pending = new();
    private readonly CancellationTokenSource _lifetime = new();
    private int _xid;
    private Task? _loops;

    public OneDriver(WebSocket socket, ProgramRegistry programs, Authenticator? authenticate, string remoteId)
    {
        _socket = socket;
        _programs = programs;
        _authenticate = authenticate ?? AllowAnonymous;
        RemoteId = remoteId;
        _xid = Random.Shared.Next(1, ushort.MaxValue);
    }

    public string RemoteId { get; }
    public ProgramRegistry Programs => _programs;
    public bool IsOpen => _socket.State == WebSocketState.Open;

    public event Action<OneDriver>? Closed;
    public event Action<Uri>? Handoff;

    public ValueTask SendHandoffAsync(Uri endpoint) =>
        _outbound.Writer.WriteAsync(Wire.EncodeHandoff(endpoint));

    public void Start()
    {
        _loops ??= Task.WhenAll(ReceiveLoop(_lifetime.Token), SendLoop(_lifetime.Token));
    }

    private static ValueTask<AuthDecision> AllowAnonymous(Credential credential, CancellationToken cancellationToken) =>
        new(new AuthDecision.Allow(null));

    public async Task<TResult?> CallAsync<TResult>(
        uint program,
        int version,
        uint procedure,
        object? args,
        Credential? credential = null,
        TimeSpan? timeout = null,
        CancellationToken cancellationToken = default)
    {
        var element = await CallRawAsync(program, version, procedure, args, credential, timeout, cancellationToken)
            .ConfigureAwait(false);
        if (element.ValueKind is JsonValueKind.Null or JsonValueKind.Undefined)
            return default;
        return element.Deserialize<TResult>(Json.Options);
    }

    public async Task<JsonElement> CallRawAsync(
        uint program,
        int version,
        uint procedure,
        object? args,
        Credential? credential = null,
        TimeSpan? timeout = null,
        CancellationToken cancellationToken = default)
    {
        var xid = NextXid();
        var cred = credential ?? Credential.None;
        var frame = Wire.EncodeCall(xid, program, version, procedure, cred, args);

        var pending = new Pending();
        if (!_pending.TryAdd(xid, pending))
            throw new NobleNetException("xid", "xid collision");

        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _lifetime.Token);
        timeoutCts.CancelAfter(timeout ?? TimeSpan.FromSeconds(10));
        await using var _ = timeoutCts.Token.Register(() =>
        {
            if (_pending.TryRemove(xid, out var slot))
                slot.Fail(new NobleNetException("timeout", $"call {xid} timed out"));
        });

        await _outbound.Writer.WriteAsync(frame, timeoutCts.Token).ConfigureAwait(false);
        return await pending.Task.ConfigureAwait(false);
    }

    public async ValueTask DisposeAsync()
    {
        _lifetime.Cancel();
        _outbound.Writer.TryComplete();
        FailAll(new NobleNetException("closed", "connection closed"));
        if (_socket.State is WebSocketState.Open or WebSocketState.CloseReceived)
        {
            try
            {
                await _socket.CloseAsync(WebSocketCloseStatus.NormalClosure, "bye", CancellationToken.None)
                    .ConfigureAwait(false);
            }
            catch (WebSocketException)
            {
                // already gone
            }
        }
        _socket.Dispose();
        _lifetime.Dispose();
        Closed?.Invoke(this);
    }

    private uint NextXid()
    {
        var next = (uint)Interlocked.Increment(ref _xid);
        return next == 0 ? (uint)Interlocked.Increment(ref _xid) : next;
    }

    private async Task SendLoop(CancellationToken cancellationToken)
    {
        try
        {
            await foreach (var frame in _outbound.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
            {
                await _socket.SendAsync(frame, WebSocketMessageType.Text, endOfMessage: true, cancellationToken)
                    .ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
        }
        catch (WebSocketException)
        {
            FailAll(new NobleNetException("closed", "connection closed"));
        }
    }

    private async Task ReceiveLoop(CancellationToken cancellationToken)
    {
        var buffer = new byte[16 * 1024];
        try
        {
            while (!cancellationToken.IsCancellationRequested && _socket.State == WebSocketState.Open)
            {
                using var message = new MemoryStream();
                WebSocketReceiveResult result;
                do
                {
                    result = await _socket.ReceiveAsync(buffer, cancellationToken).ConfigureAwait(false);
                    if (result.MessageType == WebSocketMessageType.Close)
                    {
                        FailAll(new NobleNetException("closed", "connection closed"));
                        Closed?.Invoke(this);
                        return;
                    }
                    message.Write(buffer, 0, result.Count);
                }
                while (!result.EndOfMessage);

                if (result.MessageType != WebSocketMessageType.Text)
                    continue;
                await DispatchAsync(message.ToArray(), cancellationToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
        }
        catch (WebSocketException)
        {
            FailAll(new NobleNetException("closed", "connection closed"));
            Closed?.Invoke(this);
        }
    }

    private async Task DispatchAsync(byte[] payload, CancellationToken cancellationToken)
    {
        WireEnvelope? envelope;
        try
        {
            envelope = JsonSerializer.Deserialize<WireEnvelope>(payload, Json.Options);
        }
        catch (JsonException)
        {
            return;
        }
        if (envelope is null || envelope.NobleNet != Protocol.Version)
            return;

        if (envelope.Mtype == Protocol.Handoff && Uri.TryCreate(envelope.Endpoint, UriKind.Absolute, out var next))
        {
            Handoff?.Invoke(next);
            return;
        }
        if (envelope.Mtype == Protocol.Reply)
        {
            Complete(envelope);
            return;
        }
        if (envelope.Mtype == Protocol.Call)
            await HandleCallAsync(envelope, cancellationToken).ConfigureAwait(false);
    }

    private void Complete(WireEnvelope envelope)
    {
        if (!_pending.TryRemove(envelope.Xid, out var pending))
            return;
        if (envelope.Stat == Protocol.Denied)
        {
            pending.Fail(new NobleNetException(envelope.Deny ?? Protocol.Denied, envelope.Error ?? envelope.Deny ?? "denied"));
            return;
        }
        if (envelope.Accept != Protocol.Success)
        {
            VersionSpan? span = envelope.Mismatch is null
                ? null
                : new VersionSpan(envelope.Mismatch.Low, envelope.Mismatch.High);
            pending.Fail(new NobleNetException(envelope.Accept ?? Protocol.SystemErr, envelope.Error ?? envelope.Accept ?? "failed", span));
            return;
        }
        pending.Succeed(envelope.Result.ValueKind == JsonValueKind.Undefined
            ? Json.Box(null)
            : envelope.Result);
    }

    private async Task HandleCallAsync(WireEnvelope call, CancellationToken cancellationToken)
    {
        if (call.RpcVers != Protocol.RpcVersion)
        {
            await ReplyAsync(new WireEnvelope
            {
                NobleNet = Protocol.Version,
                Xid = call.Xid,
                Mtype = Protocol.Reply,
                Stat = Protocol.Denied,
                Deny = Protocol.RpcMismatch,
                Mismatch = new WireSpan { Low = Protocol.RpcVersion, High = Protocol.RpcVersion },
                Error = $"rpc version {call.RpcVers} is not supported",
            }).ConfigureAwait(false);
            return;
        }

        var cred = new Credential(call.Cred?.Flavor ?? "none", call.Cred?.Body ?? default);
        AuthDecision decision;
        try
        {
            decision = await _authenticate(cred, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            decision = new AuthDecision.Deny(ex.Message);
        }
        if (decision is AuthDecision.Deny denied)
        {
            await ReplyAsync(new WireEnvelope
            {
                NobleNet = Protocol.Version,
                Xid = call.Xid,
                Mtype = Protocol.Reply,
                Stat = Protocol.Denied,
                Deny = Protocol.AuthError,
                Error = denied.Reason,
            }).ConfigureAwait(false);
            return;
        }

        var principal = ((AuthDecision.Allow)decision).Principal;
        switch (_programs.Find(call.Prog, call.Vers, call.Proc))
        {
            case ProgramRegistry.Lookup.NoProgram:
                await RejectAsync(call.Xid, Protocol.ProgUnavail, null).ConfigureAwait(false);
                return;
            case ProgramRegistry.Lookup.BadVersion mismatch:
                await RejectAsync(call.Xid, Protocol.ProgMismatch, new VersionSpan(mismatch.Low, mismatch.High)).ConfigureAwait(false);
                return;
            case ProgramRegistry.Lookup.NoProcedure:
                await RejectAsync(call.Xid, Protocol.ProcUnavail, null).ConfigureAwait(false);
                return;
            case ProgramRegistry.Lookup.Hit hit:
                var context = new CallContext(this, call.Xid, call.Prog, call.Vers, call.Proc, cred, principal);
                try
                {
                    var result = await hit.Handler(call.Args, context, cancellationToken).ConfigureAwait(false);
                    await ReplyAsync(new WireEnvelope
                    {
                        NobleNet = Protocol.Version,
                        Xid = call.Xid,
                        Mtype = Protocol.Reply,
                        Stat = Protocol.Accepted,
                        Accept = Protocol.Success,
                        Result = Json.Box(result),
                    }).ConfigureAwait(false);
                }
                catch (NobleNetException ex)
                {
                    await RejectAsync(call.Xid, ex.Code, ex.Mismatch, ex.Message).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    await RejectAsync(call.Xid, Protocol.SystemErr, null, ex.Message).ConfigureAwait(false);
                }
                return;
        }
    }

    private ValueTask RejectAsync(uint xid, string accept, VersionSpan? mismatch, string? error = null) =>
        ReplyAsync(new WireEnvelope
        {
            NobleNet = Protocol.Version,
            Xid = xid,
            Mtype = Protocol.Reply,
            Stat = Protocol.Accepted,
            Accept = accept,
            Mismatch = mismatch is null ? null : new WireSpan { Low = mismatch.Value.Low, High = mismatch.Value.High },
            Error = error ?? accept,
        });

    private ValueTask ReplyAsync(WireEnvelope envelope) =>
        _outbound.Writer.WriteAsync(Wire.EncodeReply(envelope));

    private void FailAll(Exception error)
    {
        foreach (var xid in _pending.Keys)
        {
            if (_pending.TryRemove(xid, out var pending))
                pending.Fail(error);
        }
    }

    private sealed class Pending
    {
        private readonly TaskCompletionSource<JsonElement> _source = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public Task<JsonElement> Task => _source.Task;
        public void Succeed(JsonElement value) => _source.TrySetResult(value);
        public void Fail(Exception error) => _source.TrySetException(error);
    }
}
