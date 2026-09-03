# LawnDart.Projections.Lightweight

In-process projection host. Includes the store, checkpoint, SDK, and
partitioning types used with InMemory and SQL Server.

```csharp
services.AddInMemoryProjectionStores("default");
ctx.WithProjections([typeof(MyProjection).Assembly]);
app.MapProjectionQueries("default");
```
