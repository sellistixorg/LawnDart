using LawnDart;
using Microsoft.Data.SqlClient;

namespace LawnDart.Sql;

/// <summary>
/// Reads catalog rows. The caller opens the application connection.
/// </summary>
internal static class SqlSchemaProbe
{
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

    internal static SchemaCheckResult Finish(string? expected, string? found, List<string> missing)
        => SchemaCheckResult.FromRead(expected, found, missing);
}
