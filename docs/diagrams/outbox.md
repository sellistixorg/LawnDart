# Outbox

```mermaid
flowchart LR
    Cmd["Command"] --> Tx["SQL transaction"]
    Tx --> Events["Event tables"]
    Tx --> Rows["Outbox rows"]
    Rows --> Pub["Outbox publisher"]
    Pub --> Bus["IMessageTransport"]
    Bus --> Reactor["IReactor"]
```

InMemory hosts skip the durable outbox and publish in-process.
