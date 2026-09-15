# One-time regression proof (RDY-08)

Not a CI job. Recorded so the tripwire is known to work.

**When:** 2026-09-14  
**What:** Commented out `WithCommandHandlers<TMarker>()` in
`src/LawnDart.EventSourcing/Context/BoundedContextBuilderExtensions.cs`
(the `RDY-02` marker overload).  
**Result:** `dotnet build samples/Library.Host` failed with CS1061 on
`LibraryHost.cs` lines 24, 46, and 55 (`ctx.WithCommandHandlers<BorrowBookHandler>()`).  
**Restore:** The method was put back. `dotnet test samples/Library.Domain.Tests`
was green again (4 passed).

Do not automate this. Repeat only if the check itself is rewritten.
