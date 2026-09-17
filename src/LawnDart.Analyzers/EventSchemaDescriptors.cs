using Microsoft.CodeAnalysis;

namespace LawnDart.Analyzers;

internal static class EventSchemaDescriptors
{
    public const string Category = "LawnDart.EventSchema";

    private const string HelpLink =
        "https://github.com/sellistixorg/LawnDart/blob/main/docs/EVENT_SCHEMA_VERSIONING.md";

    public static readonly DiagnosticDescriptor TwoCurrents = new(
        id: "LDT001",
        title: "Event family has two current types",
        messageFormat:
            "Duplicate current for event type token '{0}' on {1}. " +
            "Mark exactly one [EventTypeName(..., current: true)]. " +
            "Warmup is the runtime authority.",
        category: Category,
        defaultSeverity: DiagnosticSeverity.Error,
        isEnabledByDefault: true,
        description:
            "A family with two or more CLR types must have exactly one current: true. " +
            "A green analyzer is not a substitute for warmup.",
        helpLinkUri: HelpLink,
        customTags: WellKnownDiagnosticTags.CompilationEnd);

    public static readonly DiagnosticDescriptor MissingCurrent = new(
        id: "LDT002",
        title: "Event family is missing current: true",
        messageFormat:
            "Event family '{0}' has {1} types and no [EventTypeName(..., current: true)]. " +
            "Mark exactly one current type. Warmup is the runtime authority.",
        category: Category,
        defaultSeverity: DiagnosticSeverity.Error,
        isEnabledByDefault: true,
        description:
            "A family with two or more CLR types needs exactly one current: true. " +
            "A single one-arg [EventTypeName(\"token\")] is not an error. " +
            "A green analyzer is not a substitute for warmup.",
        helpLinkUri: HelpLink,
        customTags: WellKnownDiagnosticTags.CompilationEnd);

    public static readonly DiagnosticDescriptor IncompleteChain = new(
        id: "LDT003",
        title: "Event family is missing an upcaster to current",
        messageFormat:
            "No upcaster chain from SchemaVersion {0} to {1} for family '{2}'. " +
            "Register IEventUpcaster hops so every historical version reaches current. " +
            "Warmup is the runtime authority.",
        category: Category,
        defaultSeverity: DiagnosticSeverity.Error,
        isEnabledByDefault: true,
        description:
            "Each non-current version must reach current through IEventUpcaster hops " +
            "(direct or chained). The analyzer sees types in this compilation; " +
            "WithUpcasters registration is checked only at warmup.",
        helpLinkUri: HelpLink,
        customTags: WellKnownDiagnosticTags.CompilationEnd);
}
