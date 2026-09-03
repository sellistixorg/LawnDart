# Step 7 — Testing EDA

**Previous:** [Reactions](06-reactions.md) · **Next:** [Production](08-production.md)

`LawnDart.Testing` messaging harnesses stay on InMemory transports. Assert:

- The reactor emitted the expected command type.
- Inbox / idempotency: the same event does not enroll twice.
- Task processors emit only for overdue rows.

Use `WaitForAsync.UntilAsync` when projections or reactors are asynchronous.

SQL Server integration tests are optional and tagged `Category=Integration`.
