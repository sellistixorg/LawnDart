# Step 5 — DCB patterns

**Previous:** [Reading state](04-reading-state.md) · **Next:** [Reactions](06-reactions.md)

Use an aggregate when one stream owns the rule. Use a `DcbEntity` when the
rule spans identities — student and section, order and SKU — and must commit
in one append.

The repository loads every event that carries **all** of the given tags,
rebuilds the entity, runs the command, and appends with
`FailIfEventsMatch` so a concurrent writer on the same tag set loses.

```csharp
public sealed record HoldSeatCommand(Guid Id, Guid StudentId, Guid SectionId) : ICommand;

[EventTypeName("section-opened")]
public sealed record SectionOpened(Guid Id, DateTime Timestamp, Guid SectionId, int Seats) : IEvent;

[EventTypeName("seat-held")]
public sealed record SeatHeld(Guid Id, DateTime Timestamp, Guid StudentId, Guid SectionId) : IEvent;

public sealed class SeatHoldState : IState
{
    public int SeatsRemaining { get; set; }
}

public sealed class SeatHold : DcbEntity<SeatHoldState>
{
    public override Task HandleAsync<TCommand>(TCommand command, CancellationToken cancellationToken = default)
    {
        if (command is HoldSeatCommand hold)
        {
            if (State.SeatsRemaining <= 0)
                throw new InvalidOperationException("Section is full.");

            Emit(
                new SeatHeld(Guid.NewGuid(), DateTime.UtcNow, hold.StudentId, hold.SectionId),
                $"student:{hold.StudentId}",
                $"section:{hold.SectionId}");
        }

        return Task.CompletedTask;
    }

    protected override void ApplyEventToState(IEvent @event)
    {
        if (@event is SectionOpened opened) State.SeatsRemaining = opened.Seats;
        if (@event is SeatHeld) State.SeatsRemaining--;
    }
}
```

Bootstrap events must carry the **same tag set** the entity loads, or they
will not appear in state. Same InMemory host as [step 2](02-first-aggregate.md):

```csharp
var store = sp.GetRequiredService<IEventStore>();
var dcb = sp.GetRequiredService<IDcbRepository>();
var tags = new[] { $"student:{studentId}", $"section:{sectionId}" };

await store.AppendAsync(
    $"dcb-seed:{sectionId}",
    [new SectionOpened(Guid.NewGuid(), DateTime.UtcNow, sectionId, Seats: 5)],
    tags: tags);

var entity = await dcb.GetOrCreateEntityAsync<SeatHold>(tags);
await dcb.HandleCommandAsync(entity, new HoldSeatCommand(Guid.NewGuid(), studentId, sectionId));
```

Academy Showcase B does this for enrollment: one read of `student:{id}` +
`section:{id}`, then one append of `SeatReserved` + `StudentEnrolled`.
Showcase A needs five hops and is eventually consistent.

DCB uses `Emit(event, tags)`, not aggregate `Apply`. `HandleCommandAsync`
is the repository. See [Intentional verb differences](../GLOSSARY.md#intentional-verb-differences).

Guides: [DCB_PATTERNS.md](../DCB_PATTERNS.md), [TAGGING.md](../TAGGING.md).
