namespace LawnDart.Projections.Lightweight.Postgres;

internal sealed class PostgresProjectionNames
{
    internal PostgresProjectionNames(string schema)
    {
        Schema = PgIdentifier.RequireSchema(schema);
        Views = PgIdentifier.Qualify(Schema, "projection_views");
        Checkpoints = PgIdentifier.Qualify(Schema, "projection_checkpoints");
        Leases = PgIdentifier.Qualify(Schema, "projection_runner_leases");
        Flush = PgIdentifier.Qualify(Schema, "save_projection_flush");
    }

    internal string Schema { get; }

    internal string Views { get; }

    internal string Checkpoints { get; }

    internal string Leases { get; }

    internal string Flush { get; }

    internal string SearchPath => PgIdentifier.Quote(Schema) + ", pg_catalog";
}
