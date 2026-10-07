using System.Text.Json;
using System.Text.Json.Serialization;

namespace NobleNet.Rpc;

internal static class Json
{
    public static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web)
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.Never,
    };

    public static JsonElement Box(object? value)
    {
        var bytes = JsonSerializer.SerializeToUtf8Bytes(value, Options);
        using var doc = JsonDocument.Parse(bytes);
        return doc.RootElement.Clone();
    }
}

internal sealed class WireEnvelope
{
    [JsonPropertyName("noblenet")] public int NobleNet { get; set; }
    [JsonPropertyName("xid")] public uint Xid { get; set; }
    [JsonPropertyName("mtype")] public string? Mtype { get; set; }
    [JsonPropertyName("rpcvers")] public int RpcVers { get; set; }
    [JsonPropertyName("prog")] public uint Prog { get; set; }
    [JsonPropertyName("vers")] public int Vers { get; set; }
    [JsonPropertyName("proc")] public uint Proc { get; set; }
    [JsonPropertyName("cred")] public WireCred? Cred { get; set; }
    [JsonPropertyName("args")] public JsonElement Args { get; set; }
    [JsonPropertyName("stat")] public string? Stat { get; set; }
    [JsonPropertyName("accept")] public string? Accept { get; set; }
    [JsonPropertyName("deny")] public string? Deny { get; set; }
    [JsonPropertyName("mismatch")] public WireSpan? Mismatch { get; set; }
    [JsonPropertyName("result")] public JsonElement Result { get; set; }
    [JsonPropertyName("error")] public string? Error { get; set; }
}

internal sealed class WireCred
{
    [JsonPropertyName("flavor")] public string Flavor { get; set; } = "none";
    [JsonPropertyName("body")] public JsonElement Body { get; set; }
}

internal sealed class WireSpan
{
    [JsonPropertyName("low")] public int Low { get; set; }
    [JsonPropertyName("high")] public int High { get; set; }
}

internal static class Wire
{
    public static byte[] EncodeCall(uint xid, uint program, int version, uint procedure, Credential cred, object? args)
    {
        using var buffer = new MemoryStream();
        using (var writer = new Utf8JsonWriter(buffer))
        {
            writer.WriteStartObject();
            writer.WriteNumber("noblenet", Protocol.Version);
            writer.WriteNumber("xid", xid);
            writer.WriteString("mtype", Protocol.Call);
            writer.WriteNumber("rpcvers", Protocol.RpcVersion);
            writer.WriteNumber("prog", program);
            writer.WriteNumber("vers", version);
            writer.WriteNumber("proc", procedure);
            writer.WritePropertyName("cred");
            writer.WriteStartObject();
            writer.WriteString("flavor", cred.Flavor);
            writer.WritePropertyName("body");
            WriteElement(writer, cred.Body);
            writer.WriteEndObject();
            writer.WritePropertyName("args");
            if (args is null)
                writer.WriteNullValue();
            else if (args is JsonElement element)
                WriteElement(writer, element);
            else
                JsonSerializer.Serialize(writer, args, args.GetType(), Json.Options);
            writer.WriteEndObject();
        }
        return buffer.ToArray();
    }

    public static byte[] EncodeReply(WireEnvelope envelope)
    {
        using var buffer = new MemoryStream();
        using (var writer = new Utf8JsonWriter(buffer))
        {
            writer.WriteStartObject();
            writer.WriteNumber("noblenet", Protocol.Version);
            writer.WriteNumber("xid", envelope.Xid);
            writer.WriteString("mtype", Protocol.Reply);
            writer.WriteString("stat", envelope.Stat);
            if (envelope.Accept is not null)
                writer.WriteString("accept", envelope.Accept);
            if (envelope.Deny is not null)
                writer.WriteString("deny", envelope.Deny);
            if (envelope.Mismatch is not null)
            {
                writer.WritePropertyName("mismatch");
                writer.WriteStartObject();
                writer.WriteNumber("low", envelope.Mismatch.Low);
                writer.WriteNumber("high", envelope.Mismatch.High);
                writer.WriteEndObject();
            }
            if (envelope.Accept == Protocol.Success)
            {
                writer.WritePropertyName("result");
                WriteElement(writer, envelope.Result);
            }
            if (envelope.Error is not null)
                writer.WriteString("error", envelope.Error);
            writer.WriteEndObject();
        }
        return buffer.ToArray();
    }

    private static void WriteElement(Utf8JsonWriter writer, JsonElement element)
    {
        if (element.ValueKind is JsonValueKind.Undefined)
            writer.WriteNullValue();
        else
            element.WriteTo(writer);
    }
}
