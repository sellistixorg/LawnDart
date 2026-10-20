# LawnDart.Projections.Lightweight

In-process projection host. Includes the in-memory view store, checkpoint
store, SDK, and partitioning types.

SQL Server view and checkpoint stores are in
[`LawnDart.Projections.Lightweight.SqlServer`](projections-lightweight-sqlserver.md).

Take it when you need queryable read models. LawnDart ships this projection
engine only. Author `ProjectionBase<TView>` plus attributes. Multi-stream
views implement `IMultiStreamEntityResolver`.

Register the view stores **before** `WithProjections`.

## Registration

```csharp
services.AddInMemoryProjectionStores("default");
ctx.WithProjections([typeof(MyProjection).Assembly]);
app.MapProjectionQueries("default");
```

For SQL Server, add `LawnDart.Projections.Lightweight.SqlServer` and call
`AddSqlProjectionStores("default", connectionString)`. Add
`using LawnDart.Projections.Lightweight.SqlServer`. Keep the same context
name.

Academy WebApi maps GET `/api/views/students/{studentId}` this way.

## Related

- [Package map](README.md)
- [Lightweight projections](../LIGHTWEIGHT_PROJECTIONS.md)
- [Reading state](../learning-path/04-reading-state.md)
