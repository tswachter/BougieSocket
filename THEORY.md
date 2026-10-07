# A note, and the theory

This repository was produced in a chat with an AI, in one sitting, from a request to rebuild NobleNet RPC on WebSockets and then rewrite it in C#. Nobody sat with the ONC specification, a packet capture, or a profiler and designed it. Treat the code as a sketch of a model, not as a finished library. Sorry for the slop: the leftover NobleNet names inside a repo called BougieSocket, the demo host bolted to the protocol types, the self-test that is also the sample server, and the places where a real review would have cut a type in half.

What follows is the theory the code is trying to hold, including the parts it only half implements.

## What it is for

NobleNet EZ-RPC (Natick, 1992–1996) hid the network behind a C function call. OneDriver was the part worth keeping. Several APIs shared one connection. The server saw that connection as one user process, and could wear more than one API personality. A client could export a procedure, and the server could call it. That reverse call is the shout. It is how updates were pushed. It is not a UDP broadcast, and it is not a portmapper walk.

BougieSocket keeps that session and drops the rest. The transport is one WebSocket. The encoding is one JSON object per text frame. There is no stub compiler, no XDR, and no rpcbind.

## How a call should work

A call is four numbers plus a body: program, version, procedure, and an `xid`. Procedure 0 of every program is the null procedure. The reply is either accepted or denied. Accepted is not the same as success. `prog_mismatch` is an accepted reply that carries the low and high version the server actually has. `auth_error` is a denial, and it happens before dispatch.

Many calls may be in flight on one socket. The `xid` is the only correlation. Outbound frames have to be serialized, because a WebSocket send must not overlap itself. A timeout fails the local wait. It does not, today, cancel the handler on the other side. That is a hole, not a feature.

## How a shout should work

A listener is a client that has already opened a socket and exported a procedure. The server holds that socket as one session. A shout is the server invoking the exported procedure, with the update as the arguments. A shout to a set of listeners is that call repeated across the sessions that joined. Each shout has its own `xid`. One dead client fails that call. The others still complete.

There is no topic table in the library. Joining a subject should be an ordinary procedure that records the session. Publishing should walk that set. Order holds per socket, not across sockets. Nothing is replayed to a client that was not connected when the shout went out.

UDP does not belong on this path. A datagram cannot reach a WebSocket peer, and it cannot tell you which listener missed the frame. A UDP listener is only useful as a beacon that says where the WebSocket is.

## What the code does instead of the theory

`ProgramRegistry` is the personality table. `OneDriver` is the session. `SessionHub.Sessions` is the set a shout walks, and the demo does walk it — once, in the self-test, with no join and no subject filter. Auth is a hook that returns a principal or a denial. The sample token is a literal string. None of that is `AUTH_SYS` or GSS.

If this is going to stop being a sketch, the next cuts are a real subscription table on the hub, cancel that reaches the handler, and a split between the protocol library and the Kestrel host so the demo is not the design.
