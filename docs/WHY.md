# Why LawnDart

LawnDart is the .NET runtime that event-modeled systems compile into.

You model slices — in Eventhesis, in prooph board, in eventmodelers.ai, or on a
whiteboard — and LawnDart is what they become: commands, events, state,
projections, with a production path that is a DI swap away.

That is a different product from the ones readers usually compare first.

## Not a document database

[Marten](https://martendb.io) is a document database that also does events.
LawnDart is not. It does not store documents, does not replace a general-purpose
persistence library, and does not compete with Marten on breadth.

## Not a kernel

[Cratis](https://cratis.io) is a kernel. LawnDart is not. It runs in-process in
your host, on your DI, on `net10.0`. There is no LawnDart server.

## Not a JVM platform

[Axon](https://axoniq.io) is a JVM platform. LawnDart is a .NET library. It is
not a cross-language runtime.

## Closed set

Supported today:

- **Stores:** InMemory and SQL Server. The log (`IEventLog`) is recorded events; `IEventStore` is the typed session. Third-party stores implement the log.
- **Messaging:** in-process.
- **Runtime:** in-process, `net10.0`, your host, your DI.
- **Modelling:** aggregates and DCB behind one store contract.

That set is the product, not a teaser for a larger one.

## A separate store on the same contract

[Boomerang](https://github.com/sellistix/Sellistix.Patterns) is a separate
product, not a LawnDart package or tier. It is a generic event store that
implements LawnDart's frozen `IEventStore` contract. Its own published numbers
(eight subscribers, two writers, thirty seconds, fsync on: p50 0.28 ms, p99
0.55 ms, ~50K events/sec) are Boomerang's, not LawnDart's. They show what that
contract can be driven by.
