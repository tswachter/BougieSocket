using System.Text.Json;

namespace NobleNet.Rpc;

public sealed class NobleNetException : Exception
{
    public NobleNetException(string code, string message, VersionSpan? mismatch = null)
        : base(message)
    {
        Code = code;
        Mismatch = mismatch;
    }

    public string Code { get; }
    public VersionSpan? Mismatch { get; }
}

public sealed class CallContext
{
    internal CallContext(OneDriver session, uint xid, uint program, int version, uint procedure, Credential credential, object? principal)
    {
        Session = session;
        Xid = xid;
        Program = program;
        Version = version;
        Procedure = procedure;
        Credential = credential;
        Principal = principal;
    }

    public OneDriver Session { get; }
    public uint Xid { get; }
    public uint Program { get; }
    public int Version { get; }
    public uint Procedure { get; }
    public Credential Credential { get; }
    public object? Principal { get; }
    public string RemoteId => Session.RemoteId;
}

public delegate ValueTask<object?> Procedure(JsonElement args, CallContext call, CancellationToken cancellationToken);

public sealed record ProgramInfo(uint Program, int Version, string Name, IReadOnlyList<uint> Procedures);

/// <summary>
/// Programs registered on one side of a OneDriver connection. Procedure 0 is
/// the ONC null procedure and is installed for every version.
/// </summary>
public sealed class ProgramRegistry
{
    private readonly Dictionary<uint, SortedDictionary<int, ProgramVersion>> _programs = new();

    public ProgramRegistry Register(uint program, int version, string name, Action<ProcedureSet> configure)
    {
        if (version < 1) throw new ArgumentOutOfRangeException(nameof(version));
        var set = new ProcedureSet();
        configure(set);
        if (!_programs.TryGetValue(program, out var versions))
        {
            versions = new SortedDictionary<int, ProgramVersion>();
            _programs[program] = versions;
        }
        versions[version] = new ProgramVersion(name, set.Freeze());
        return this;
    }

    public IReadOnlyList<ProgramInfo> Describe()
    {
        var list = new List<ProgramInfo>();
        foreach (var (program, versions) in _programs)
        {
            foreach (var (version, body) in versions)
            {
                list.Add(new ProgramInfo(program, version, body.Name, body.Procedures.Keys.Order().ToArray()));
            }
        }
        return list;
    }

    internal Lookup Find(uint program, int version, uint procedure)
    {
        if (!_programs.TryGetValue(program, out var versions) || versions.Count == 0)
            return new Lookup.NoProgram();
        if (!versions.TryGetValue(version, out var body))
            return new Lookup.BadVersion(versions.Keys.First(), versions.Keys.Last());
        if (!body.Procedures.TryGetValue(procedure, out var handler))
            return new Lookup.NoProcedure();
        return new Lookup.Hit(handler, body.Name);
    }

    private sealed class ProgramVersion(string name, Dictionary<uint, Procedure> procedures)
    {
        public string Name { get; } = name;
        public Dictionary<uint, Procedure> Procedures { get; } = procedures;
    }

    internal abstract record Lookup
    {
        internal sealed record Hit(Procedure Handler, string Name) : Lookup;
        internal sealed record NoProgram : Lookup;
        internal sealed record BadVersion(int Low, int High) : Lookup;
        internal sealed record NoProcedure : Lookup;
    }
}

public sealed class ProcedureSet
{
    private readonly Dictionary<uint, Procedure> _handlers = new()
    {
        [0] = static (_, _, _) => new ValueTask<object?>(result: null),
    };

    public ProcedureSet HandleRaw(uint procedure, Procedure handler)
    {
        _handlers[procedure] = handler;
        return this;
    }

    public ProcedureSet Handle<TArg, TResult>(
        uint procedure,
        Func<TArg, CallContext, CancellationToken, ValueTask<TResult>> handler)
    {
        _handlers[procedure] = async (args, call, ct) =>
        {
            TArg typed;
            try
            {
                typed = args.ValueKind is JsonValueKind.Undefined or JsonValueKind.Null
                    ? Activator.CreateInstance<TArg>()
                    : args.Deserialize<TArg>(Json.Options) ?? throw new JsonException("empty args");
            }
            catch (JsonException ex)
            {
                throw new NobleNetException(Protocol.GarbageArgs, ex.Message);
            }
            return await handler(typed, call, ct).ConfigureAwait(false);
        };
        return this;
    }

    internal Dictionary<uint, Procedure> Freeze() => _handlers;
}
