using LawnDart;
using Microsoft.Data.SqlClient;

namespace LawnDart.Sql;

/// <summary>
/// Reads catalog rows. The caller opens the application connection.
/// </summary>
internal static class SqlSchemaProbe
{
    /// <summary>
    /// Throws when the login cannot read the schema definition.
    /// A schema that is not visible throws because it does not exist, or the login cannot read it.
    /// </summary>
    internal static async Task RequireViewDefinitionAsync(
        SqlConnection connection,
        string schema,
        CancellationToken cancellationToken)
    {
        const string sql = """
            SELECT
                SCHEMA_ID(@schema),
                HAS_PERMS_BY_NAME(@schema, N'SCHEMA', N'VIEW DEFINITION')
            """;
        var command = new SqlCommand(sql, connection);
        await using var disposal = command.ConfigureAwait(false);
        command.Parameters.AddWithValue("@schema", schema);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            throw Permission(schema);

        var schemaVisible = !reader.IsDBNull(0);
        var schemaView = ReadFlag(reader, 1);
        if (schemaView == 1)
            return;
        if (!schemaVisible)
            throw MissingOrUnreadable(schema);

        throw Permission(schema);
    }

    private static int? ReadFlag(System.Data.Common.DbDataReader reader, int ordinal)
        => reader.IsDBNull(ordinal)
            ? null
            : Convert.ToInt32(reader.GetValue(ordinal), System.Globalization.CultureInfo.InvariantCulture);

    private static InvalidOperationException MissingOrUnreadable(string schema)
        => new(
            "Schema '" + schema + "' does not exist, or the login cannot read its definition. " +
            "Create a database user for the login and grant VIEW DEFINITION on schema '" +
            schema + "'.");

    private static InvalidOperationException Permission(string schema)
        => new(
            "The login lacks permission to read the schema definition. " +
            "Create a database user for the login and grant VIEW DEFINITION on schema '" +
            schema + "'.");

    internal static async Task<bool> ExistsAsync(
        SqlConnection connection,
        string schema,
        string name,
        string typeCode,
        CancellationToken cancellationToken)
    {
        var type = typeCode switch
        {
            "U" or "SO" or "TR" => typeCode,
            _ => throw new ArgumentOutOfRangeException(nameof(typeCode))
        };
        var sql = $"""
            SELECT 1
            FROM sys.objects o
            INNER JOIN sys.schemas s ON s.schema_id = o.schema_id
            WHERE s.name = @schema AND o.name = @name AND o.type = '{type}'
            """;
        var command = new SqlCommand(sql, connection);
        await using var disposal = command.ConfigureAwait(false);
        command.Parameters.AddWithValue("@schema", schema);
        command.Parameters.AddWithValue("@name", name);
        var result = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return result is not null and not DBNull;
    }

    internal static async Task<string?> ReadStampAsync(
        SqlConnection connection,
        string schema,
        string table,
        string property,
        CancellationToken cancellationToken)
    {
        const string sql = """
            SELECT CONVERT(nvarchar(128), ep.value)
            FROM sys.extended_properties ep
            INNER JOIN sys.objects o ON o.object_id = ep.major_id
            INNER JOIN sys.schemas s ON s.schema_id = o.schema_id
            WHERE s.name = @schema
              AND o.name = @table
              AND ep.name = @property
              AND ep.class = 1
              AND ep.minor_id = 0
            """;
        var command = new SqlCommand(sql, connection);
        await using var disposal = command.ConfigureAwait(false);
        command.Parameters.AddWithValue("@schema", schema);
        command.Parameters.AddWithValue("@table", table);
        command.Parameters.AddWithValue("@property", property);
        var result = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return result is null or DBNull ? null : Convert.ToString(result);
    }

    internal static async Task<bool> IndexExistsAsync(
        SqlConnection connection,
        string schema,
        string table,
        string index,
        CancellationToken cancellationToken)
    {
        const string sql = """
            SELECT 1
            FROM sys.indexes i
            INNER JOIN sys.objects o ON o.object_id = i.object_id
            INNER JOIN sys.schemas s ON s.schema_id = o.schema_id
            WHERE s.name = @schema AND o.name = @table AND i.name = @index
            """;
        var command = new SqlCommand(sql, connection);
        await using var disposal = command.ConfigureAwait(false);
        command.Parameters.AddWithValue("@schema", schema);
        command.Parameters.AddWithValue("@table", table);
        command.Parameters.AddWithValue("@index", index);
        var result = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return result is not null and not DBNull;
    }

    internal static async Task<bool> ColumnExistsAsync(
        SqlConnection connection,
        string qualifiedTable,
        string column,
        CancellationToken cancellationToken)
    {
        const string sql = "SELECT CASE WHEN COL_LENGTH(@table, @column) IS NULL THEN 0 ELSE 1 END";
        var command = new SqlCommand(sql, connection);
        await using var disposal = command.ConfigureAwait(false);
        command.Parameters.AddWithValue("@table", qualifiedTable);
        command.Parameters.AddWithValue("@column", column);
        var result = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return Convert.ToInt32(result, System.Globalization.CultureInfo.InvariantCulture) == 1;
    }

    internal static async Task AddMissingAsync(
        SqlConnection connection,
        string schema,
        string name,
        string typeCode,
        List<string> missing,
        CancellationToken cancellationToken)
    {
        if (!await ExistsAsync(connection, schema, name, typeCode, cancellationToken).ConfigureAwait(false))
            missing.Add(name);
    }

    internal static async Task AddMissingIndexAsync(
        SqlConnection connection,
        string schema,
        string table,
        string index,
        List<string> missing,
        CancellationToken cancellationToken)
    {
        if (!await IndexExistsAsync(connection, schema, table, index, cancellationToken).ConfigureAwait(false))
            missing.Add(index);
    }

    internal static async Task AddMissingColumnAsync(
        SqlConnection connection,
        string qualifiedTable,
        string column,
        List<string> missing,
        CancellationToken cancellationToken)
    {
        if (!await ColumnExistsAsync(connection, qualifiedTable, column, cancellationToken).ConfigureAwait(false))
            missing.Add(column);
    }

    internal static SchemaCheckResult Finish(string? expected, string? found, List<string> missing)
        => SchemaCheckResult.FromRead(expected, found, missing);
}
