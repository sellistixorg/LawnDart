using Npgsql;

namespace LawnDart.EventSourcing.Postgres;

/// <summary>
/// Grants the application role use of a schema after the owner creates tables.
/// </summary>
internal static class PostgresRoleGrant
{
    internal static async Task GrantAppRoleAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        string schema,
        string? appConnectionString,
        string? ownerConnectionString,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(appConnectionString))
            return;

        var app = new NpgsqlConnectionStringBuilder(appConnectionString).Username;
        var ownerSource = string.IsNullOrWhiteSpace(ownerConnectionString)
            ? appConnectionString
            : ownerConnectionString;
        var owner = new NpgsqlConnectionStringBuilder(ownerSource).Username;
        if (string.IsNullOrWhiteSpace(app) || string.Equals(app, owner, StringComparison.Ordinal))
            return;

        var role = PgIdentifier.Quote(app);
        var quotedSchema = PgIdentifier.Quote(schema);
        var sql = $"""
            GRANT USAGE ON SCHEMA {quotedSchema} TO {role};
            GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA {quotedSchema} TO {role};
            GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA {quotedSchema} TO {role};
            GRANT EXECUTE ON ALL ROUTINES IN SCHEMA {quotedSchema} TO {role};
            """;
        using var command = new NpgsqlCommand(sql, connection, transaction);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }
}
