# DCB patterns

Dynamic Consistency Boundary (DCB) loads events by **tags**, not by a
single aggregate stream, then appends new facts atomically with an
`AppendCondition`.

Use DCB when a rule spans more than one identity (book + member,
student + section). Use an aggregate when one stream owns the rule.

Skill excerpts come from `samples/Library.Dcb.Domain` (`BookLoan`).
Academy `EnrollmentEntity` is the larger runnable host.

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
entity. See [Intentional verb differences](GLOSSARY.md#intentional-verb-differences).

See [TAGGING.md](TAGGING.md).
