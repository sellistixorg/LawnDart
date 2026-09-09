# DCB patterns

Dynamic Consistency Boundary (DCB) loads events by **tags**, not by a single
aggregate stream, then appends new facts atomically with an `AppendCondition`.

Use DCB when a rule spans more than one identity (student + section, order +
inventory). Use an aggregate when one stream owns the rule.

## Shape

```csharp
public sealed class EnrollmentEntity : DcbEntity<EnrollmentState>
{
    public override Task HandleAsync<TCommand>(TCommand command, CancellationToken cancellationToken = default)
    {
        if (command is EnrollStudentCommand enroll)
        {
            if (State.SeatsRemaining <= 0)
                throw new DomainException("Section is full.");
            Emit(new SeatReserved(...), $"student:{enroll.StudentId}", $"section:{enroll.SectionId}");
            Emit(new StudentEnrolled(...), $"student:{enroll.StudentId}", $"section:{enroll.SectionId}");
        }
        return Task.CompletedTask;
    }
}
```

The repository:

1. Reads events for the tag set (`student:{id}`, `section:{id}`).
2. Rebuilds the entity.
3. Runs the command.
4. Appends emitted events with `FailIfMatches` after the last seen sequence.

## Versus choreography

Academy Showcase A (aggregate + in-memory broker) is eventually consistent.
Showcase B (DCB) appends `SeatReserved` and `StudentEnrolled` in one write.

DCB records pending events with `Emit` (tags), not aggregate `Apply`.
`HandleCommandAsync` is the repository; author `Handle(TCommand)` on the
entity. See [Glossary — Intentional verb differences](GLOSSARY.md#intentional-verb-differences).

See [TAGGING.md](TAGGING.md) and Academy `Enrollment/EnrollmentEntity.cs`.
