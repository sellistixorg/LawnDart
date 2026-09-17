; https://github.com/dotnet/roslyn-analyzers/blob/main/src/Microsoft.CodeAnalysis.Analyzers/ReleaseTrackingAnalyzers.Help.md

## Release 0.4

### New Rules

Rule ID | Category | Severity | Notes
--------|----------|----------|--------------------
LDT001  | LawnDart.EventSchema | Error | Two current types for one family token
LDT002  | LawnDart.EventSchema | Error | Multi-type family with no current: true
LDT003  | LawnDart.EventSchema | Error | Incomplete upcaster chain to current
