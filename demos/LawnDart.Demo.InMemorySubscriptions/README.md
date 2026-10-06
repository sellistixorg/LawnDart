# InMemory subscriptions

Catch-up, then live delivery on `IEventStoreSubscriptions.Subscribe`, with a file-backed client cursor.

```bash
dotnet run --project demos/LawnDart.Demo.InMemorySubscriptions
```

Requires the .NET 10 SDK. No Docker.

Run it a second time in the same output directory. The first run writes `subscribe-checkpoint.txt` next to the built assembly. InMemory starts empty each process. The demo seeds three events, then subscribes. A checkpoint ahead of the new store head is reset, and the run finishes again.

## What to look for

- The first run prints three `catch-up` lines (sequences 1, 2, 3) and one `live` line (sequence 4).
- The checkpoint file ends at `4`.
- The second run prints `resetting cursor`, then the same three catch-up lines and one live line.

The client owns the cursor. Reconnect with `lastApplied + 1`. The method table for `IEventStoreSubscriptions` is in [Backend selection](../../docs/BACKEND_SELECTION.md#portable-store-contract).
