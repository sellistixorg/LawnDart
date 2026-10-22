using System.Globalization;
using LawnDart;
using Npgsql;

namespace LawnDart.EventSourcing.Postgres;

/// <summary>
/// Reads catalog rows. Callers open the application connection, not the migration connection.
/// </summary>
internal static class PostgresSchemaProbe
{
    internal readonly record struct Relation(string Label, string Qualified);

    internal readonly record struct PgType(string Label, string Name);

    /// <summary>
    /// A function this version writes.
    /// <paramref name="ExpectedBody"/> is the exact <c>prosrc</c> setup sends.
    /// When it is null, the check only requires the function to exist.
    /// </summary>
    internal readonly record struct Routine(
        string Name,
        string? MarkerPrefix,
        int MarkerVersion,
        string? ExpectedBody);

    internal readonly record struct Trigger(string Name, string TableQualified);

    /// <summary>The text between the first and last <paramref name="delimiter"/> in <paramref name="sql"/>.</summary>
    internal static string DollarBody(string sql, string delimiter)
    {
        var start = sql.IndexOf(delimiter, StringComparison.Ordinal);
        if (start < 0)
            throw new InvalidOperationException($"Function SQL has no {delimiter} body.");
        start += delimiter.Length;
        var end = sql.LastIndexOf(delimiter, StringComparison.Ordinal);
        if (end < start)
            throw new InvalidOperationException($"Function SQL has no closing {delimiter} body.");
        return sql[start..end];
    }

    internal static async Task<SchemaCheckResult> CheckAsync(
        NpgsqlConnection connection,
        string? expectedFormat,
        string schema,
        string? stampTable,
        string? stampComponent,
        IReadOnlyList<Relation> relations,
        IReadOnlyList<PgType> types,
        IReadOnlyList<Routine> functions,
        IReadOnlyList<Trigger> triggers,
        CancellationToken cancellationToken)
    {
        var missing = new List<string>();
        string? found = null;
        var stampTableExists = stampTable is not null && await RelationExistsAsync(connection, stampTable, cancellationToken).ConfigureAwait(false);
        if (stampComponent is not null && stampTableExists)
        {
            var format = await ReadFormatAsync(connection, stampTable!, stampComponent, cancellationToken).ConfigureAwait(false);
            found = format?.ToString(System.Globalization.CultureInfo.InvariantCulture);
        }

        foreach (var relation in relations)
        {
            if (!await RelationExistsAsync(connection, relation.Qualified, cancellationToken).ConfigureAwait(false))
                missing.Add(relation.Label);
        }

        foreach (var type in types)
        {
            if (!await TypeExistsAsync(connection, schema, type.Name, cancellationToken).ConfigureAwait(false))
                missing.Add(type.Label);
        }

        foreach (var function in functions)
            await AddFunctionAsync(connection, schema, function, missing, cancellationToken).ConfigureAwait(false);

        foreach (var trigger in triggers)
        {
            if (!await TriggerExistsAsync(connection, schema, trigger, cancellationToken).ConfigureAwait(false))
                missing.Add(trigger.Name);
        }

        return SchemaCheckResult.FromRead(expectedFormat, found, missing);
    }

    private static async Task<bool> RelationExistsAsync(
        NpgsqlConnection connection,
        string qualified,
        CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand("SELECT to_regclass(@name) IS NOT NULL", connection);
        command.Parameters.AddWithValue("name", qualified);
        var result = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return result is true;
    }

    private static async Task<int?> ReadFormatAsync(
        NpgsqlConnection connection,
        string stampTable,
        string component,
        CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(
            $"SELECT format FROM {stampTable} WHERE component = @component",
            connection);
        command.Parameters.AddWithValue("component", component);
        var result = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return result is null or DBNull ? null : Convert.ToInt32(result);
    }

    private static async Task<bool> TypeExistsAsync(
        NpgsqlConnection connection,
        string schema,
        string name,
        CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(
            """
            SELECT 1
            FROM pg_type t
            JOIN pg_namespace n ON n.oid = t.typnamespace
            WHERE n.nspname = @schema AND t.typname = @name
            """,
            connection);
        command.Parameters.AddWithValue("schema", schema);
        command.Parameters.AddWithValue("name", name);
        var result = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return result is not null and not DBNull;
    }

    private static async Task AddFunctionAsync(
        NpgsqlConnection connection,
        string schema,
        Routine function,
        List<string> missing,
        CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(
            """
            SELECT p.prosrc
            FROM pg_proc p
            JOIN pg_namespace n ON n.oid = p.pronamespace
            WHERE n.nspname = @schema AND p.proname = @name
            """,
            connection);
        command.Parameters.AddWithValue("schema", schema);
        command.Parameters.AddWithValue("name", function.Name);
        var bodies = new List<string>();
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
        {
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                bodies.Add(reader.GetString(0));
        }

        if (bodies.Count == 0)
        {
            missing.Add(function.Name);
            return;
        }

        if (function.ExpectedBody is null)
            return;

        var expectedHash = await Md5Async(connection, function.ExpectedBody, cancellationToken).ConfigureAwait(false);
        var matched = false;
        foreach (var body in bodies)
        {
            if (!MarkerVersionIs(body, function.MarkerPrefix, function.MarkerVersion))
                continue;
            var actualHash = await Md5Async(connection, body, cancellationToken).ConfigureAwait(false);
            if (string.Equals(actualHash, expectedHash, StringComparison.Ordinal))
            {
                matched = true;
                break;
            }
        }

        if (!matched)
            missing.Add(function.Name + " body");
    }

    /// <summary>
    /// True when <paramref name="body"/> contains <paramref name="prefix"/> followed by
    /// <paramref name="version"/> and the next character is not another digit.
    /// Version 1 does not match version 10.
    /// </summary>
    internal static bool MarkerVersionIs(string body, string? prefix, int version)
    {
        if (string.IsNullOrEmpty(prefix))
            return false;

        var expected = version.ToString(CultureInfo.InvariantCulture);
        var index = 0;
        while ((index = body.IndexOf(prefix, index, StringComparison.Ordinal)) >= 0)
        {
            var digits = index + prefix.Length;
            var end = digits;
            while (end < body.Length && char.IsDigit(body[end]))
                end++;
            if (end > digits && body[digits..end] == expected)
                return true;
            index += prefix.Length;
        }

        return false;
    }

    private static async Task<string> Md5Async(
        NpgsqlConnection connection,
        string body,
        CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand("SELECT md5(@body)", connection);
        command.Parameters.Add(new NpgsqlParameter("body", body) { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text });
        var result = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return Convert.ToString(result, CultureInfo.InvariantCulture) ?? "";
    }

    private static async Task<bool> TriggerExistsAsync(
        NpgsqlConnection connection,
        string schema,
        Trigger trigger,
        CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(
            """
            SELECT 1
            FROM pg_trigger t
            JOIN pg_class c ON c.oid = t.tgrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE NOT t.tgisinternal
              AND n.nspname = @schema
              AND t.tgname = @name
              AND c.oid = to_regclass(@table)
            """,
            connection);
        command.Parameters.AddWithValue("schema", schema);
        command.Parameters.AddWithValue("name", trigger.Name);
        command.Parameters.AddWithValue("table", trigger.TableQualified);
        var result = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return result is not null and not DBNull;
    }
}
