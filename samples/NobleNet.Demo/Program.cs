using System.Net;
using System.Net.WebSockets;
using System.Text.Json;
using NobleNet.Rpc;

namespace NobleNet.Demo;

public sealed record Pair(double A, double B);
public sealed record ValueResult(double Value, int? Vers = null);
public sealed record EchoArgs(string? Hello, string? From);
public sealed record EchoResult(bool Pong, EchoArgs? Echo, long At);
public sealed record WhoResult(string Session, string Flavor, string? Subject);
public sealed record Shout(string Message);
public sealed record HeardAck(bool Heard);

public static class Program
{
    public static async Task Main(string[] args)
    {
        var serve = args.Contains("--serve");
        var port = int.TryParse(Environment.GetEnvironmentVariable("PORT"), out var p) ? p : 8765;

        var hub = new SessionHub(BuildPrograms(), Authenticate);
        hub.SessionOpened += (_, e) => Console.WriteLine($"session open    {e.Session.RemoteId}");
        hub.SessionClosed += (_, e) => Console.WriteLine($"session close   {e.Session.RemoteId}");

        var builder = WebApplication.CreateBuilder(new WebApplicationOptions
        {
            Args = args,
            ApplicationName = "NobleNet.Demo",
        });
        builder.Logging.SetMinimumLevel(LogLevel.Warning);
        builder.WebHost.UseUrls($"http://127.0.0.1:{port}");
        var app = builder.Build();
        app.UseWebSockets();
        app.Map("/noblenet", async context =>
        {
            if (!context.WebSockets.IsWebSocketRequest)
            {
                context.Response.StatusCode = StatusCodes.Status400BadRequest;
                return;
            }
            var socket = await context.WebSockets.AcceptWebSocketAsync();
            var remote = context.Connection.RemoteIpAddress?.ToString() ?? "?";
            var session = hub.Adopt(socket, $"{remote}:{context.Connection.RemotePort}");
            // The session pumps the socket. Hold the request until it closes
            // so Kestrel does not dispose the WebSocket out from under us.
            var closed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            session.Closed += _ => closed.TrySetResult();
            await closed.Task;
        });

        await app.StartAsync();
        Console.WriteLine($"NobleNet RPC listening on ws://127.0.0.1:{port}/noblenet");
        Console.WriteLine("programs        " + string.Join(", ", hub.Programs.Describe().Select(d => d.Name)));

        await SelfTest(port, hub);

        if (!serve)
        {
            await app.StopAsync();
            Console.WriteLine("self-test ok");
            return;
        }

        Console.WriteLine("serving — Ctrl+C to stop");
        await app.WaitForShutdownAsync();
    }

    private static ProgramRegistry BuildPrograms()
    {
        var registry = new ProgramRegistry();
        registry.Register(WellKnownPrograms.Echo, 1, "echo", procs =>
        {
            procs.Handle<EchoArgs, EchoResult>(1, (args, _, _) =>
                new ValueTask<EchoResult>(new EchoResult(true, args, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds())));
        });
        registry.Register(WellKnownPrograms.Calc, 1, "calc.v1", procs =>
        {
            procs.Handle<Pair, ValueResult>(1, (args, _, _) => new ValueTask<ValueResult>(new ValueResult(args.A + args.B)));
            procs.Handle<Pair, ValueResult>(2, (args, _, _) => new ValueTask<ValueResult>(new ValueResult(args.A * args.B)));
            procs.Handle<Pair, ValueResult>(3, (args, _, _) =>
                args.B == 0
                    ? throw new InvalidOperationException("division by zero")
                    : new ValueTask<ValueResult>(new ValueResult(args.A / args.B)));
        });
        registry.Register(WellKnownPrograms.Calc, 2, "calc.v2", procs =>
        {
            procs.Handle<Pair, ValueResult>(1, (args, _, _) => new ValueTask<ValueResult>(new ValueResult(args.A + args.B, 2)));
        });
        registry.Register(WellKnownPrograms.Session, 1, "session", procs =>
        {
            procs.HandleRaw(1, (args, call, ct) =>
            {
                _ = args;
                _ = ct;
                var subject = call.Principal as string;
                return new ValueTask<object?>(new WhoResult(call.RemoteId, call.Credential.Flavor, subject));
            });
            procs.HandleRaw(2, (args, call, ct) =>
            {
                _ = args;
                _ = call;
                _ = ct;
                return new ValueTask<object?>(registry.Describe());
            });
        });
        return registry;
    }

    private static ValueTask<AuthDecision> Authenticate(Credential cred, CancellationToken _)
    {
        if (cred.Flavor is "none" or "")
            return new ValueTask<AuthDecision>(new AuthDecision.Allow("anonymous"));
        if (cred.Flavor == "token" && cred.AsString() == "demo-token")
            return new ValueTask<AuthDecision>(new AuthDecision.Allow("demo"));
        return new ValueTask<AuthDecision>(new AuthDecision.Deny("authentication failed"));
    }

    private static async Task SelfTest(int port, SessionHub hub)
    {
        var local = new ProgramRegistry();
        var heard = new List<string>();
        local.Register(WellKnownPrograms.Callback, 1, "callback", procs =>
        {
            procs.Handle<Shout, HeardAck>(1, (args, _, _) =>
            {
                heard.Add(args.Message);
                return new ValueTask<HeardAck>(new HeardAck(true));
            });
        });

        await using var client = await NobleNetClient.ConnectAsync(new Uri($"ws://127.0.0.1:{port}/noblenet"), local);

        var pong = await client.CallAsync<EchoResult>(WellKnownPrograms.Echo, 1, 1, new EchoArgs("noblenet", null));
        Console.WriteLine($"echo.ping       {JsonSerializer.Serialize(pong)}");
        Console.WriteLine($"calc.v1.add     {await client.CallAsync<ValueResult>(WellKnownPrograms.Calc, 1, 1, new Pair(20, 22))}");
        Console.WriteLine($"calc.v1.mul     {await client.CallAsync<ValueResult>(WellKnownPrograms.Calc, 1, 2, new Pair(6, 7))}");
        Console.WriteLine($"calc.v2.add     {await client.CallAsync<ValueResult>(WellKnownPrograms.Calc, 2, 1, new Pair(1, 2))}");

        var piped = await Task.WhenAll(
            client.CallAsync<ValueResult>(WellKnownPrograms.Calc, 1, 1, new Pair(1, 1)),
            client.CallAsync<ValueResult>(WellKnownPrograms.Calc, 1, 1, new Pair(2, 2)),
            client.CallAsync<ValueResult>(WellKnownPrograms.Calc, 1, 1, new Pair(3, 3)));
        Console.WriteLine("pipelined       " + string.Join(", ", piped.Select(r => r?.Value)));

        var token = JsonSerializer.SerializeToElement("demo-token");
        var who = await client.CallAsync<WhoResult>(
            WellKnownPrograms.Session, 1, 1, args: null,
            credential: new Credential("token", token));
        Console.WriteLine($"session.whoami  {who}");

        try
        {
            await client.CallAsync<ValueResult>(WellKnownPrograms.Calc, 9, 1, new Pair(1, 1));
        }
        catch (NobleNetException ex)
        {
            Console.WriteLine($"prog_mismatch   {ex.Code} {ex.Mismatch}");
        }

        try
        {
            await client.CallAsync<ValueResult>(WellKnownPrograms.Calc, 1, 3, new Pair(1, 0));
        }
        catch (NobleNetException ex)
        {
            Console.WriteLine($"system_err      {ex.Code} {ex.Message}");
        }

        var serverSide = hub.Sessions.Single();
        var back = await serverSide.CallAsync<HeardAck>(WellKnownPrograms.Callback, 1, 1, new Shout("server-to-client on the same OneDriver socket"));
        Console.WriteLine($"callback        {back} heard: {heard[0]}");
    }
}
