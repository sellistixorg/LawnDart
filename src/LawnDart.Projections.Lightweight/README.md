# LawnDart.Projections.Lightweight

In-process projection host. View and checkpoint stores in this package are in-memory.

```csharp
ctx.UseInMemory();
ctx.WithProjections(...);
```

SQL Server view and checkpoint stores are in `LawnDart.Projections.Lightweight.SqlServer`.
