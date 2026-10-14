# LawnDart.Messaging.InMemory

In-process messaging transport and inbox for local development and testing.

## What this project provides

- `AddInMemoryMessaging(...)` registration
- In-memory `IMessageTransport` and `IInboxStore`
- Retries, an in-memory dead-letter queue, and optional topology setup
