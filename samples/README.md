# Samples

`Library.Domain` is the canonical reference slice: commands, events, a `Book`
aggregate whose decision state (`BookState`) is not the catalog read model
(`BookCatalogView`), command handlers, and `LibraryProjector`.
`Library.Host` is the compiled host grammar (InMemory, SQL, HTTP, Lightweight).
`Library.Domain.Tests` holds the given / when / then specs, including `AndView`.

The Eventhesis-shaped input those projects implement is
[`build-kit/library-slice.json`](../build-kit/library-slice.json).

Academy (`demos/LawnDart.Demo.Academy`) is the runnable host, not the excerpt
source.

The raw-append comparison foil is [docs/LIBRARY_RAW_APPEND.md](../docs/LIBRARY_RAW_APPEND.md).
It is article material, not the recommended path, and is not part of this
slice.
