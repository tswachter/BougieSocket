using System.Text.Json;

namespace NobleNet.Rpc;

/// <summary>
/// Wire constants for the NobleNet WebSocket rebuild. Field names match the
/// JSON framing used by the browser client: one object per text frame.
/// </summary>
public static class Protocol
{
    public const int Version = 1;
    public const int RpcVersion = 2;

    public const string Call = "call";
    public const string Reply = "reply";

    public const string Accepted = "accepted";
    public const string Denied = "denied";

    public const string Success = "success";
    public const string ProgUnavail = "prog_unavail";
    public const string ProgMismatch = "prog_mismatch";
    public const string ProcUnavail = "proc_unavail";
    public const string GarbageArgs = "garbage_args";
    public const string SystemErr = "system_err";

    public const string RpcMismatch = "rpc_mismatch";
    public const string AuthError = "auth_error";
}

/// <summary>ONC-style program numbers used by the sample services.</summary>
public static class WellKnownPrograms
{
    public const uint Echo = 0x2000_0001;
    public const uint Calc = 0x2000_0002;
    public const uint Session = 0x2000_0003;
    public const uint Callback = 0x2000_0010;
}

public readonly record struct VersionSpan(int Low, int High);

public readonly record struct Credential(string Flavor, JsonElement Body)
{
    public static Credential None { get; } = new("none", default);

    public string? AsString() =>
        Body.ValueKind == JsonValueKind.String ? Body.GetString() : null;
}

/// <summary>Accepted principal, or a denial. Null is not a valid decision.</summary>
public abstract record AuthDecision
{
    public sealed record Allow(object? Principal) : AuthDecision;
    public sealed record Deny(string Reason) : AuthDecision;
}

public delegate ValueTask<AuthDecision> Authenticator(Credential credential, CancellationToken cancellationToken);
