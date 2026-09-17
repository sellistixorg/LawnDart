using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.Diagnostics;

namespace LawnDart.Analyzers;

/// <summary>
/// Compile-time checks for event schema versioning. Mirrors
/// <c>EventTypeCatalog.Materialize</c> and <c>EventUpcastPipeline.Materialize</c>
/// for the three L17 rules. Warmup is the runtime authority if they drift.
/// </summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class EventSchemaAnalyzer : DiagnosticAnalyzer
{
    private const string AttributeMetadataName = "LawnDart.EventStore.EventTypeNameAttribute";
    private const string UpcasterMetadataName = "LawnDart.EventStore.IEventUpcaster`2";
    private const string LawnDartAssemblyName = "LawnDart";

    /// <inheritdoc />
    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics { get; } =
        ImmutableArray.Create(
            EventSchemaDescriptors.TwoCurrents,
            EventSchemaDescriptors.MissingCurrent,
            EventSchemaDescriptors.IncompleteChain);

    /// <inheritdoc />
    public override void Initialize(AnalysisContext context)
    {
        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
        context.EnableConcurrentExecution();
        context.RegisterCompilationAction(AnalyzeCompilation);
    }

    private static void AnalyzeCompilation(CompilationAnalysisContext context)
    {
        var compilation = context.Compilation;
        var attributeType = compilation.GetTypeByMetadataName(AttributeMetadataName);
        if (attributeType is null)
            return;

        var upcasterInterface = compilation.GetTypeByMetadataName(UpcasterMetadataName);
        var events = new List<CatalogType>();
        var hops = new List<(INamedTypeSymbol From, INamedTypeSymbol To)>();

        CollectFromNamespace(
            compilation.Assembly.GlobalNamespace,
            attributeType,
            upcasterInterface,
            sourceAssembly: true,
            events,
            hops,
            context.CancellationToken);

        foreach (var referenced in compilation.SourceModule.ReferencedAssemblySymbols)
        {
            context.CancellationToken.ThrowIfCancellationRequested();
            if (!ShouldScanReferenced(referenced))
                continue;

            CollectFromNamespace(
                referenced.GlobalNamespace,
                attributeType,
                upcasterInterface,
                sourceAssembly: false,
                events,
                hops,
                context.CancellationToken);
        }

        if (events.Count == 0)
            return;

        AnalyzeFamilies(events, hops, context);
    }

    private static bool ShouldScanReferenced(IAssemblySymbol assembly)
    {
        if (assembly.IsImplicitlyDeclared || assembly.Name == LawnDartAssemblyName)
            return false;

        // In-ticket: group families across referenced domain assemblies (V1 in
        // a package, V2 in the host). Skip BCL / LawnDart itself (attribute
        // only). Walk other refs and keep only types that have the attribute.
        return !IsFrameworkAssembly(assembly.Name);
    }

    private static bool IsFrameworkAssembly(string name)
        => name.StartsWith("System", System.StringComparison.Ordinal)
           || name.StartsWith("Microsoft.", System.StringComparison.Ordinal)
           || name is "netstandard" or "mscorlib" or "WindowsBase";

    private static void CollectFromNamespace(
        INamespaceSymbol ns,
        INamedTypeSymbol attributeType,
        INamedTypeSymbol? upcasterInterface,
        bool sourceAssembly,
        List<CatalogType> events,
        List<(INamedTypeSymbol From, INamedTypeSymbol To)> hops,
        CancellationToken cancellationToken)
    {
        foreach (var type in ns.GetTypeMembers())
        {
            cancellationToken.ThrowIfCancellationRequested();
            CollectFromType(
                type,
                attributeType,
                upcasterInterface,
                sourceAssembly,
                events,
                hops,
                cancellationToken);
        }

        foreach (var child in ns.GetNamespaceMembers())
        {
            CollectFromNamespace(
                child,
                attributeType,
                upcasterInterface,
                sourceAssembly,
                events,
                hops,
                cancellationToken);
        }
    }

    private static void CollectFromType(
        INamedTypeSymbol type,
        INamedTypeSymbol attributeType,
        INamedTypeSymbol? upcasterInterface,
        bool sourceAssembly,
        List<CatalogType> events,
        List<(INamedTypeSymbol From, INamedTypeSymbol To)> hops,
        CancellationToken cancellationToken)
    {
        var includeAsCatalog = sourceAssembly
            || type.DeclaredAccessibility is Accessibility.Public or Accessibility.Internal;
        if (includeAsCatalog && TryReadCatalogType(type, attributeType, out var catalog))
            events.Add(catalog);

        if (upcasterInterface is not null
            && TryReadUpcasterHop(type, upcasterInterface, out var hop))
        {
            hops.Add(hop);
        }

        foreach (var nested in type.GetTypeMembers())
        {
            cancellationToken.ThrowIfCancellationRequested();
            CollectFromType(
                nested,
                attributeType,
                upcasterInterface,
                sourceAssembly,
                events,
                hops,
                cancellationToken);
        }
    }

    private static bool TryReadCatalogType(
        INamedTypeSymbol type,
        INamedTypeSymbol attributeType,
        out CatalogType catalog)
    {
        catalog = default;
        foreach (var attr in type.GetAttributes())
        {
            if (!SymbolEquals(attr.AttributeClass, attributeType))
                continue;
            if (!TryReadAttribute(attr, out var token, out var version, out var current))
                return false;

            catalog = new CatalogType(type, token, version, current);
            return true;
        }

        return false;
    }

    private static bool TryReadAttribute(
        AttributeData attr,
        out string token,
        out int version,
        out bool current)
    {
        token = "";
        version = 1;
        current = false;

        if (attr.ConstructorArguments.Length >= 1
            && attr.ConstructorArguments[0].Value is string name
            && !string.IsNullOrWhiteSpace(name))
        {
            token = name;
        }
        else
        {
            return false;
        }

        if (attr.ConstructorArguments.Length >= 2
            && attr.ConstructorArguments[1].Value is int ctorVersion)
        {
            version = ctorVersion;
        }

        if (attr.ConstructorArguments.Length >= 3
            && attr.ConstructorArguments[2].Value is bool ctorCurrent)
        {
            current = ctorCurrent;
        }

        foreach (var named in attr.NamedArguments)
        {
            if (named.Key is "version" or "Version" && named.Value.Value is int namedVersion)
                version = namedVersion;
            if (named.Key is "current" or "Current" && named.Value.Value is bool namedCurrent)
                current = namedCurrent;
        }

        return version >= 1;
    }

    private static bool TryReadUpcasterHop(
        INamedTypeSymbol type,
        INamedTypeSymbol upcasterInterface,
        out (INamedTypeSymbol From, INamedTypeSymbol To) hop)
    {
        hop = default;
        if (type.TypeKind != TypeKind.Class || type.IsAbstract || type.IsUnboundGenericType)
            return false;

        INamedTypeSymbol? from = null;
        INamedTypeSymbol? to = null;
        foreach (var iface in type.AllInterfaces)
        {
            if (!SymbolEquals(iface.OriginalDefinition, upcasterInterface) || iface.TypeArguments.Length != 2)
                continue;
            if (iface.TypeArguments[0] is not INamedTypeSymbol toType
                || iface.TypeArguments[1] is not INamedTypeSymbol fromType)
                continue;

            // First matching hop wins; warmup rejects a type that maps two sources.
            if (from is not null)
                return false;

            to = toType;
            from = fromType;
        }

        if (from is null || to is null)
            return false;

        hop = (from, to);
        return true;
    }

    private static void AnalyzeFamilies(
        List<CatalogType> events,
        List<(INamedTypeSymbol From, INamedTypeSymbol To)> hops,
        CompilationAnalysisContext context)
    {
        var byToken = events
            .GroupBy(e => e.Token, System.StringComparer.Ordinal)
            .ToList();

        var catalogByType = new Dictionary<INamedTypeSymbol, CatalogType>(SymbolEqualityComparer.Default);
        foreach (var entry in events)
            catalogByType[entry.Type] = entry;

        var validHops = new Dictionary<INamedTypeSymbol, INamedTypeSymbol>(SymbolEqualityComparer.Default);
        foreach (var (from, to) in hops)
        {
            if (!catalogByType.TryGetValue(from, out var fromEntry)
                || !catalogByType.TryGetValue(to, out var toEntry))
            {
                continue;
            }

            if (!string.Equals(fromEntry.Token, toEntry.Token, System.StringComparison.Ordinal))
                continue;
            if (fromEntry.Version >= toEntry.Version)
                continue;
            if (validHops.ContainsKey(from))
                continue;

            validHops[from] = to;
        }

        foreach (var family in byToken)
        {
            context.CancellationToken.ThrowIfCancellationRequested();
            var members = family.ToList();
            if (members.Count == 1)
                continue;

            var currents = members.Where(m => m.Current).ToList();
            if (currents.Count == 0)
            {
                foreach (var member in members)
                {
                    context.ReportDiagnostic(Diagnostic.Create(
                        EventSchemaDescriptors.MissingCurrent,
                        PreferredLocation(member.Type),
                        family.Key,
                        members.Count));
                }

                continue;
            }

            if (currents.Count > 1)
            {
                var names = string.Join(
                    ", ",
                    currents.Select(c => $"'{c.Type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat)}'"));
                foreach (var current in currents)
                {
                    context.ReportDiagnostic(Diagnostic.Create(
                        EventSchemaDescriptors.TwoCurrents,
                        PreferredLocation(current.Type),
                        family.Key,
                        names));
                }

                continue;
            }

            var currentType = currents[0];
            foreach (var member in members)
            {
                if (SymbolEquals(member.Type, currentType.Type))
                    continue;
                if (CanReach(member.Type, currentType.Type, validHops))
                    continue;

                var location = PreferredLocation(member.Type);
                if (location == Location.None)
                    location = PreferredLocation(currentType.Type);

                context.ReportDiagnostic(Diagnostic.Create(
                    EventSchemaDescriptors.IncompleteChain,
                    location,
                    member.Version,
                    currentType.Version,
                    family.Key));
            }
        }
    }

    private static bool CanReach(
        INamedTypeSymbol from,
        INamedTypeSymbol current,
        Dictionary<INamedTypeSymbol, INamedTypeSymbol> hops)
    {
        var seen = new HashSet<INamedTypeSymbol>(SymbolEqualityComparer.Default);
        var cursor = from;
        while (!SymbolEquals(cursor, current))
        {
            if (!seen.Add(cursor) || !hops.TryGetValue(cursor, out var next))
                return false;
            cursor = next;
        }

        return true;
    }

    private static Location PreferredLocation(INamedTypeSymbol type)
    {
        foreach (var location in type.Locations)
        {
            if (location.IsInSource)
                return location;
        }

        return Location.None;
    }

    private static bool SymbolEquals(ISymbol? left, ISymbol? right)
        => SymbolEqualityComparer.Default.Equals(left, right);

    private readonly struct CatalogType
    {
        public CatalogType(INamedTypeSymbol type, string token, int version, bool current)
        {
            Type = type;
            Token = token;
            Version = version;
            Current = current;
        }

        public INamedTypeSymbol Type { get; }
        public string Token { get; }
        public int Version { get; }
        public bool Current { get; }
    }
}
