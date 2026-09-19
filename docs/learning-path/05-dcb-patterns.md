# Step 5. DCB patterns

**Previous:** [Reading state](04-reading-state.md) · **Next:** [Reactions](06-reactions.md)

Use an aggregate when one stream owns the rule. Use a `DcbEntity` when the
rule spans identities (student and section, order and SKU) and must commit
in one append.

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

The repository loads events for the tag set, rebuilds the entity, runs
the command, and appends with `FailIfMatches` after the last seen sequence.
Excerpted from `demos/LawnDart.Demo.Academy/Showcases/ShowcaseB_DcbInProcess.cs`
(seed and projector lines omitted):

```csharp
var entity = await _dcbRepository.GetOrCreateEntityAsync<EnrollmentEntity>(enrollmentTags);
await _dcbRepository.HandleCommandAsync<EnrollmentEntity, EnrollStudentCommand>(
    entity,
    new EnrollStudentCommand(Guid.NewGuid(), studentId, courseId));
```

Bootstrap events must carry the same tag set the entity loads, or they
will not appear in state. Same InMemory host as
[step 2](02-first-aggregate.md).

Academy Showcase B appends `SeatReserved` and `StudentEnrolled` in one
write. Showcase A needs five hops and is eventually consistent.

DCB uses `Emit(event, tags)`, not aggregate `Apply`. `HandleCommandAsync`
is the repository. See [Intentional verb differences](../GLOSSARY.md#intentional-verb-differences).

Guides: [DCB_PATTERNS.md](../DCB_PATTERNS.md), [TAGGING.md](../TAGGING.md).
