using System.Data;
using System.Text;
using System.Text.Json;
using Microsoft.Data.SqlClient;
using LawnDart.EventStore;
using LawnDart.Sql;

namespace LawnDart.EventSourcing.SqlServer.EventStore;

public partial class SqlServerEventStore
{
    /// <summary>
    /// Adds the binary stream id column, its indexes, and <c>StreamTags</c> when they are missing.
    /// Fills <c>StreamTags</c> from existing <c>Streams.Tags</c> values.
    /// The events format stamp stays at <see cref="CurrentSchemaFormat"/>.
    /// </summary>
    private async Task EnsureStreamListingSchemaAsync(SqlConnection connection, CancellationToken cancellationToken)
    {
        var transaction = connection.BeginTransaction();
        await using var transactionDisposal = transaction.ConfigureAwait(false);
        try
        {
            await ExecuteListingAsync(connection, transaction, ApplockSql(), cancellationToken).ConfigureAwait(false);
            await ExecuteListingAsync(connection, transaction, AddStreamIdBinSql(), cancellationToken).ConfigureAwait(false);
            await ExecuteListingAsync(connection, transaction, EnsureStreamIdBinIndexSql(), cancellationToken).ConfigureAwait(false);
            await ExecuteListingAsync(connection, transaction, EnsureAggregateIndexSql(), cancellationToken).ConfigureAwait(false);
            await ExecuteListingAsync(connection, transaction, EnsureStreamTagsTableSql(), cancellationToken).ConfigureAwait(false);
            await ExecuteListingAsync(connection, transaction, BackfillStreamTagsSql(), cancellationToken).ConfigureAwait(false);
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            if (transaction.Connection != null)
            {
                try
                {
                    await transaction.RollbackAsync(CancellationToken.None).ConfigureAwait(false);
                }
                catch (Exception)
                {
                    // The transaction is already finished.
                }
            }

            throw;
        }
    }

    private string ApplockSql()
    {
        var resource = "LawnDartStreamList:" + _schemaName + ":" + _registryTableName;
        if (resource.Length > 255)
            resource = resource[..255];

        return $"""
            SET XACT_ABORT ON;
            DECLARE @result int;
            EXEC @result = sp_getapplock
                @Resource = N'{SqlIdentifier.Literal(resource)}',
                @LockMode = N'Exclusive',
                @LockOwner = N'Transaction',
                @LockTimeout = 60000;
            IF @result < 0
                THROW 50001, N'Could not lock the stream registry to add listing objects.', 1;
            """;
    }

    private string AddStreamIdBinSql() => $"""
        IF NOT EXISTS (
            SELECT 1 FROM sys.columns
            WHERE object_id = OBJECT_ID(N'{SqlIdentifier.Literal(_qRegistry)}')
              AND name = N'StreamIdBin')
            ALTER TABLE {_qRegistry}
                ADD [StreamIdBin] AS ([StreamId] COLLATE {TagCollation}) PERSISTED;
        """;

    private string EnsureStreamIdBinIndexSql() => $"""
        IF NOT EXISTS (
            SELECT 1 FROM sys.indexes
            WHERE name = N'UX_StreamIdBin'
              AND object_id = OBJECT_ID(N'{SqlIdentifier.Literal(_qRegistry)}'))
            CREATE UNIQUE NONCLUSTERED INDEX [UX_StreamIdBin] ON {_qRegistry} ([StreamIdBin]);
        """;

    private string EnsureAggregateIndexSql() => $"""
        IF NOT EXISTS (
            SELECT 1
            FROM sys.indexes i
            INNER JOIN sys.index_columns ic
                ON ic.object_id = i.object_id AND ic.index_id = i.index_id AND ic.key_ordinal = 2
            INNER JOIN sys.columns c
                ON c.object_id = ic.object_id AND c.column_id = ic.column_id
            WHERE i.name = N'IX_AggregateType'
              AND i.object_id = OBJECT_ID(N'{SqlIdentifier.Literal(_qRegistry)}')
              AND c.name = N'StreamIdBin')
        BEGIN
            IF EXISTS (
                SELECT 1 FROM sys.indexes
                WHERE name = N'IX_AggregateType'
                  AND object_id = OBJECT_ID(N'{SqlIdentifier.Literal(_qRegistry)}'))
                DROP INDEX [IX_AggregateType] ON {_qRegistry};
            CREATE NONCLUSTERED INDEX [IX_AggregateType]
                ON {_qRegistry} ([AggregateType], [StreamIdBin]);
        END
        """;

    private string EnsureStreamTagsTableSql()
    {
        var pk = SqlIdentifier.Quote("PK_" + _streamTagsTableName);
        var clustered = SqlIdentifier.Quote("IX_" + _streamTagsTableName + "_StreamId");
        // Tag plus stream id is wider than a clustered primary key allows, so the key is nonclustered.
        return $"""
            IF OBJECT_ID(N'{SqlIdentifier.Literal(_qStreamTags)}', N'U') IS NULL
            BEGIN
                CREATE TABLE {_qStreamTags} (
                    [Tag] NVARCHAR(256) COLLATE {TagCollation} NOT NULL,
                    [StreamId] NVARCHAR(255) COLLATE {TagCollation} NOT NULL,
                    CONSTRAINT {pk} PRIMARY KEY NONCLUSTERED ([Tag], [StreamId])
                );
                CREATE CLUSTERED INDEX {clustered} ON {_qStreamTags} ([StreamId]);
            END
            """;
    }

    private string BackfillStreamTagsSql() => $"""
        INSERT INTO {_qStreamTags} ([Tag], [StreamId])
        SELECT DISTINCT
            CONVERT(NVARCHAR(256), j.[value]) COLLATE {TagCollation},
            CONVERT(NVARCHAR(255), s.[StreamId]) COLLATE {TagCollation}
        FROM {_qRegistry} AS s
        CROSS APPLY OPENJSON(CASE WHEN ISJSON(s.[Tags]) = 1 THEN s.[Tags] ELSE N'[]' END) AS j
        WHERE j.[value] IS NOT NULL
          AND j.[value] <> N''
          AND NOT EXISTS (
              SELECT 1
              FROM {_qStreamTags} AS t
              WHERE t.[Tag] = CONVERT(NVARCHAR(256), j.[value]) COLLATE {TagCollation}
                AND t.[StreamId] = CONVERT(NVARCHAR(255), s.[StreamId]) COLLATE {TagCollation}
          );
        """;

    private async Task SyncStreamTagsAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        string streamId,
        IReadOnlyList<string> tags,
        CancellationToken cancellationToken)
    {
        var tagsJson = JsonSerializer.Serialize(tags, _jsonOptions);
        var sql = $"""
            DELETE t
            FROM {_qStreamTags} AS t
            WHERE t.[StreamId] = @StreamId COLLATE {TagCollation}
              AND NOT EXISTS (
                  SELECT 1
                  FROM OPENJSON(@Tags) AS j
                  WHERE CONVERT(NVARCHAR(256), j.[value]) COLLATE {TagCollation} = t.[Tag]
              );

            INSERT INTO {_qStreamTags} ([Tag], [StreamId])
            SELECT DISTINCT
                CONVERT(NVARCHAR(256), j.[value]) COLLATE {TagCollation},
                @StreamId COLLATE {TagCollation}
            FROM OPENJSON(@Tags) AS j
            WHERE j.[value] IS NOT NULL
              AND j.[value] <> N''
              AND NOT EXISTS (
                  SELECT 1
                  FROM {_qStreamTags} AS t
                  WHERE t.[Tag] = CONVERT(NVARCHAR(256), j.[value]) COLLATE {TagCollation}
                    AND t.[StreamId] = @StreamId COLLATE {TagCollation}
              );
            """;
        var command = new SqlCommand(sql, connection, transaction);
        await using var commandDisposal = command.ConfigureAwait(false);
        command.Parameters.Add("@StreamId", SqlDbType.NVarChar, 255).Value = streamId;
        command.Parameters.Add("@Tags", SqlDbType.NVarChar, -1).Value = tagsJson;
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<StreamPage> ListStreamsAsync(
        StreamListFilter? filter = null,
        string? pageToken = null,
        int pageSize = StreamPage.DefaultPageSize,
        CancellationToken cancellationToken = default)
    {
        if (!_enableRegistry)
            throw new InvalidOperationException("Stream registry is not enabled");

        StreamPageToken.CheckPageSize(pageSize);
        var after = StreamPageToken.ReadAfter(pageToken, filter);
        var prefix = StreamPageToken.Normalize(filter?.Prefix);
        var tag = StreamPageToken.Normalize(filter?.Tag);
        var aggregateType = StreamPageToken.Normalize(filter?.AggregateType);

        var connection = new SqlConnection(_connectionString);
        await using var connectionDisposal = connection.ConfigureAwait(false);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

        var sql = new StringBuilder();
        sql.Append("SELECT TOP (@Take) ");
        sql.Append("s.StreamId, s.TenantId, s.AggregateType, s.AggregateId, s.CurrentVersion, s.LastSequencePosition, ");
        sql.Append("s.CreatedAt, s.LastEventAt, s.EventCount, s.Tags, s.Status ");
        if (tag is not null)
        {
            sql.Append("FROM ").Append(_qStreamTags).Append(" AS t ");
            sql.Append("INNER JOIN ").Append(_qRegistry).Append(" AS s ON s.StreamIdBin = t.StreamId ");
            sql.Append("WHERE t.Tag = @Tag COLLATE ").Append(TagCollation).Append(" AND s.Status = 'Active'");
        }
        else
        {
            sql.Append("FROM ").Append(_qRegistry).Append(" AS s WHERE s.Status = 'Active'");
        }

        if (after is not null)
        {
            var column = tag is not null ? "t.StreamId" : "s.StreamIdBin";
            sql.Append(" AND ").Append(column).Append(" > @After COLLATE ").Append(TagCollation);
        }

        if (prefix is not null)
            sql.Append(" AND ").Append(StreamIdPrefixSql.Predicate("s.StreamId"));
        if (aggregateType is not null)
            sql.Append(" AND s.AggregateType = @AggregateType");

        sql.Append(" ORDER BY ").Append(tag is not null ? "t.StreamId" : "s.StreamIdBin");

        var command = new SqlCommand(sql.ToString(), connection);
        await using var commandDisposal = command.ConfigureAwait(false);
        command.Parameters.Add("@Take", SqlDbType.Int).Value = pageSize + 1;
        if (after is not null)
            command.Parameters.Add("@After", SqlDbType.NVarChar, 255).Value = after;
        if (prefix is not null)
            command.Parameters.Add("@Prefix", SqlDbType.NVarChar, 512).Value = StreamIdPrefixSql.LikeParameter(prefix);
        if (tag is not null)
            command.Parameters.Add("@Tag", SqlDbType.NVarChar, 256).Value = tag;
        if (aggregateType is not null)
            command.Parameters.Add("@AggregateType", SqlDbType.NVarChar, 100).Value = aggregateType;

        var streams = new List<StreamMetadata>();
        var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        await using var readerDisposal = reader.ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            streams.Add(ReadStreamMetadata(reader));

        return StreamPageToken.ToPage(streams, pageSize, filter);
    }

    private static async Task ExecuteListingAsync(
        SqlConnection connection,
        SqlTransaction transaction,
        string sql,
        CancellationToken cancellationToken)
    {
        var command = new SqlCommand(sql, connection, transaction)
        {
            CommandTimeout = 120
        };
        await using var commandDisposal = command.ConfigureAwait(false);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }
}
