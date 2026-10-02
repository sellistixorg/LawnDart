# LawnDart.Analyzers

Compile-time checks for **event schema versioning** only. Diagnostic prefix
`LDT`. Add the package to a project that declares `[EventTypeName]` types.

This package reports `LDT001` to `LDT003` only. Warmup
(`EventTypeCatalog.Materialize` / `WithUpcasters`) is still the runtime
authority. A green analyzer is not a substitute for warmup. The analyzer
sees types in the compilation; it cannot see whether `WithEventTypes` /
`WithUpcasters` registered them.

Missing `[EventTypeName]`, duplicate tokens, kebab-case, and handler/DI
mistakes stay warmup / `ContextStartupValidator`.

## Install

```bash
dotnet add package LawnDart.Analyzers --prerelease
```

## Diagnostics

| ID | When | Fix |
|---|---|---|
| `LDT001` | Two or more types in one family mark `current: true`. | Keep the domain name on the current type. Historical is `AuthorRegisteredV1`. Mark **exactly one** `current: true`. |
| `LDT002` | A family has two or more CLR types and no `current: true`. | Add `current: true` on the current type. One-arg `[EventTypeName("token")]` is not an error when it is the only type. |
| `LDT003` | A historical version cannot reach current through `IEventUpcaster<TTo, TFrom>` hops. | Add a direct hop or a chain (`v1 → v2`, `v2 → v3`). There is no downcast API. |

See [Event schema versioning](https://github.com/sellistixorg/LawnDart/blob/main/docs/EVENT_SCHEMA_VERSIONING.md).
