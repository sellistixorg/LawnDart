# LawnDart.Projections.Lightweight

In-process projection host. Includes the view store, checkpoint store, SDK,
and partitioning types used with InMemory and SQL Server.

Take it when you need queryable read models. LawnDart ships this projection
engine only.

Register the view stores **before** `WithProjections`.

## Registration

```csharp
services.AddInMemoryProjectionStores("default");
ctx.WithProjections([typeof(MyProjection).Assembly]);
app.MapProjectionQueries("default");
```

For SQL Server, swap in `AddSqlProjectionStores("default", connectionString)`
and keep the same context name.

Academy WebApi maps GET `/api/views/students/{studentId}` this way.

## Related

- [Package map](README.md)
- [Lightweight projections](../LIGHTWEIGHT_PROJECTIONS.md)
- [Reading state](../learning-path/04-reading-state.md)
