# BougieSocket

Clean-room rebuild of the NobleNet EZ-RPC model on the .NET WebSocket stack. The session, dispatch table, and host are written against `System.Net.WebSockets` and Kestrel. This is not a binary-compatible port of the 1992–1996 C toolkit.

NobleNet (Natick, 1992–1996) multiplexed every API for a client onto one connection and presented it as a single user process. Here that connection is one WebSocket. Frames are JSON, one object per text message. A shout is a server-originated call on sessions that are already open.

## Shape

- `ProgramRegistry` is the server personality table. Procedure 0 is the null procedure. A second `Register` on the same program number is another version, not another connection.
- `OneDriver` is the session. Calls are correlated by `xid` and may be in flight together. Outbound frames go through a single-reader `Channel<byte[]>` because `WebSocket.SendAsync` must not overlap.
- Either side registers programs. The demo server shouts by calling a procedure the client exported, on the socket the client opened.
- `SessionHub` does not listen. Kestrel accepts the upgrade and hands the `WebSocket` to the hub. The protocol library has no ASP.NET dependency.
- Auth is an `Authenticator` returning `AuthDecision.Allow` or `Deny`. A denial is `auth_error`, not an HTTP status.
- A primary move is a `handoff` frame, not a shout to the old address. `SessionHub.MoveToAsync` tells every open session the new WebSocket URL and closes them. `FollowingClient` reconnects and exports the same programs on the new socket. A drop with no handoff retries the last URL. Updates published in the gap are still gone.

Accept codes match the ONC reply status: `success`, `prog_unavail`, `prog_mismatch`, `proc_unavail`, `garbage_args`, `system_err`.

## Run

```bash
dotnet run --project samples/NobleNet.Demo
dotnet run --project samples/NobleNet.Demo -- --serve
```

The self-test covers a pipelined add, a version mismatch, a system error, token auth, and a shout back to the client.

## Register a program

```csharp
var programs = new ProgramRegistry();
programs.Register(0x20000002, 1, "calc", procs =>
{
    procs.Handle<Pair, ValueResult>(1, (args, call, ct) =>
        new ValueTask<ValueResult>(new ValueResult(args.A + args.B)));
});
```

`Handle<TArg, TResult>` deserializes with `System.Text.Json`. A bad body becomes `garbage_args`. An arbitrary throw becomes `system_err`.
