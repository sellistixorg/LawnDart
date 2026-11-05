# DCB patterns

A Dynamic Consistency Boundary (DCB) decision takes a query. The same
query loads the entity and guards the append. Items combine with OR.
Inside one item, tags combine with AND and event types combine with OR.
Each new event is stored with only its own tags.

Use DCB when a rule spans more than one identity (book and member,
student and section). Use an aggregate when one stream owns the rule.

Academy `EnrollmentEntity` is the runnable host. Its decision is one
item, so that item requires both the student tag and the section tag.

The Library borrow decision is `BookLoan.ForLoan`. Add book is
`BookLoan.ForBook`. Source: `samples/Library.Dcb.Domain/BookLoan.cs`.

```csharp
public static Query ForBook(Guid bookId) =>
    Query.FromItems(QueryItem.ByTags($"book:{bookId}"));

public static Query ForLoan(Guid bookId, Guid memberId) => Query.FromItems(
    QueryItem.ByTags($"book:{bookId}"),
    QueryItem.ByTypeAndTags(
        ["book-borrowed", "book-returned"],
        [$"member:{memberId}"]));
```

`AddBookHandler` loads `ForBook`. `BorrowBookHandler` and
`ReturnBookHandler` load `ForLoan`. `BookAdded` is stored with the book
tag only. `BookBorrowed` and `BookReturned` are stored with the book
tag and the member tag. Each of those events matches at least one item
of the query that wrote it.

## Shape

Academy `EnrollmentEntity` declares closed `Handle(TCommand)`:

```csharp
public class EnrollmentEntity : DcbEntity<EnrollmentState>
{
    public static string[] GetTags(Guid studentId, Guid sectionId)
        => [$"student:{studentId}", $"section:{sectionId}"];

    public void Handle(EnrollStudentCommand cmd)
    {
        if (!State.StudentIsActive)
            throw new InvalidOperationException("Student must be registered before enrolling.");

        if (State.SeatsAvailable <= 0)
            throw new InvalidOperationException(
                $"No seats available in section {_sectionId}. Enrollment denied.");

        if (State.StudentEnrolled)
            throw new InvalidOperationException($"Student {_studentId} is already enrolled in this section.");

        if (State.IsCancelled)
            throw new InvalidOperationException("Enrollment has been cancelled.");

        Emit(new SeatReserved(Guid.NewGuid(), DateTime.UtcNow, _sectionId, _studentId),
            $"student:{_studentId}", $"section:{_sectionId}", $"course:{cmd.CourseId}");

        Emit(new StudentEnrolled(Guid.NewGuid(), DateTime.UtcNow, _studentId, cmd.CourseId),
            $"student:{_studentId}", $"section:{_sectionId}", $"course:{cmd.CourseId}");
    }
}
```

`Handle(SendConfirmationCommand)` and `Handle(CancelRegistrationCommand)`
are on the same type. Source:
`demos/LawnDart.Demo.Academy/Enrollment/EnrollmentEntity.cs`.

`EnrollmentEntity.GetTags` and each `Emit` in that file use the same
lowercase prefixes: `student:`, `section:`, and `course:`. Build the
load, `Emit`, and the provider with the context `ITagFormat`. A second
spelling is a different tag. See [TAGGING.md](TAGGING.md).

The repository:

1. Reads events for the query passed to `GetOrCreateEntityAsync`.
2. Rebuilds the entity and stores that query on it.
3. Runs the command.
4. Appends with `FailIfMatches` on that query, after the last sequence the entity has seen.

Each event the command emits should match at least one item of the query.
The fence position is the last appended sequence. A matching event that
lands at or before that position does not fail the append.

Academy loads one item:

```csharp
var query = Query.FromItems(QueryItem.ByTags(enrollmentTags));
var entity = await _dcbRepository.GetOrCreateEntityAsync<EnrollmentEntity>(query);
```

`enrollmentTags` is `student:{id}` and `section:{id}`. That one item
requires both tags.

## Versus choreography

Academy Showcase A (aggregate + in-memory broker) is eventually consistent.
Showcase B (DCB) appends `SeatReserved` and `StudentEnrolled` in one write.

DCB records pending events with `Emit` (tags), not aggregate `Apply`.
`HandleCommandAsync` is the repository; author `Handle(TCommand)` on the
entity. See [Intentional verb differences](GLOSSARY.md#intentional-verb-differences).

See [TAGGING.md](TAGGING.md).
