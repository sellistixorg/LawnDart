# Step 5 — DCB patterns

**Previous:** [Reading state](04-reading-state.md) · **Next:** [Reactions](06-reactions.md)

When "student is active AND section has a seat" cannot live on one aggregate,
use a `DcbEntity` and tags.

Academy Showcase B:

1. Seed student + section events with combined tags.
2. `IDcbRepository` loads both identities in one read.
3. One append writes `SeatReserved` + `StudentEnrolled`.

Compare Showcase A (five hops, eventual) vs B (three hops, strong).

Guides: [DCB_PATTERNS.md](../DCB_PATTERNS.md), [TAGGING.md](../TAGGING.md).
