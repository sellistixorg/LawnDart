# Samples

`Library.Domain` is the canonical reference slice: commands, events, a `Book`
aggregate whose decision state (`BookState`) is not the catalog read model
(`BookCatalogView`), command handlers, `LibraryProjector`, and
`LoanNoticeReactor`.
`Library.Host` is the compiled host grammar (InMemory, SQL, HTTP, Lightweight,
in-memory messaging).
`Library.Domain.Tests` holds the given / when / then specs, including `AndView`.

`Library.Dcb.Domain` is the same library intents as a DCB entity (`BookLoan`)
that spans book and member. `Library.Dcb.Host` and `Library.Dcb.Domain.Tests`
are its host and `DcbSpec` proofs.

The Eventhesis-shaped inputs are
[`build-kit/library-slice.json`](../build-kit/library-slice.json) (aggregate)
and [`build-kit/library-dcb-slice.json`](../build-kit/library-dcb-slice.json)
(DCB).

Academy (`demos/LawnDart.Demo.Academy`) is the runnable host, not the excerpt
source.

The raw-append comparison foil is [docs/LIBRARY_RAW_APPEND.md](../docs/LIBRARY_RAW_APPEND.md).
It is article material, not the recommended path, and is not part of this
slice.
