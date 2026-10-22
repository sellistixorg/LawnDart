using System.Data;
using System.Text;
using Microsoft.Data.SqlClient;
using LawnDart.EventStore;
using LawnDart.Sql;

namespace LawnDart.EventSourcing.SqlServer.EventStore;

public partial class SqlServerEventStore
{
    private const string StreamTagsFilledProperty = "LawnDart_StreamTagsFilled";
    private const int StreamTagBatchSize = 2000;

    /// <summary>
    /// Adds the binary stream id column, its indexes, <c>StreamTags</c>, and the trigger when they are missing.
    /// Copies existing tags once, after that lock is released. The events format stamp stays at <see cref="CurrentSchemaFormat"/>.
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
            await ExecuteListingAsync(connection, transaction, EnsureStreamTagTriggerSql(), cancellationToken).ConfigureAwait(false);
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

        await FillStreamTagsOnceAsync(connection, cancellationToken).ConfigureAwait(false);
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

    private string EnsureStreamTagTriggerSql()
    {
        var trigger = SqlIdentifier.Quote("TR_" + _registryTableName + "_StreamTags");
        // A different case spelling of the same stream id matches under the database collation,
        // so those tag rows are removed and written again for the stored spelling.
        return $"""
            CREATE OR ALTER TRIGGER {trigger} ON {_qRegistry}
            AFTER INSERT, UPDATE, DELETE
            AS
            BEGIN
                SET NOCOUNT ON;
                IF EXISTS (SELECT 1 FROM inserted) AND EXISTS (SELECT 1 FROM deleted)
                   AND NOT (UPDATE([Tags]) OR UPDATE([StreamId]))
                    RETURN;

                DELETE t
                FROM {_qStreamTags} AS t
                WHERE EXISTS (
                        SELECT 1 FROM deleted AS d
                        WHERE t.[StreamId] COLLATE DATABASE_DEFAULT = d.[StreamId])
                   OR EXISTS (
                        SELECT 1 FROM inserted AS i
                        WHERE t.[StreamId] COLLATE DATABASE_DEFAULT = i.[StreamId]);

                INSERT INTO {_qStreamTags} ([Tag], [StreamId])
                SELECT DISTINCT
                    CONVERT(NVARCHAR(256), j.[value]) COLLATE {TagCollation},
                    CONVERT(NVARCHAR(255), i.[StreamId]) COLLATE {TagCollation}
                FROM inserted AS i
                CROSS APPLY OPENJSON(CASE WHEN ISJSON(i.[Tags]) = 1 THEN i.[Tags] ELSE N'[]' END) AS j
                WHERE j.[value] IS NOT NULL AND j.[value] <> N'';
            END
            """;
    }

    private string FillStreamTagsBatchSql() => $"""
        SET NOCOUNT ON;
        DECLARE @ids TABLE ([StreamId] NVARCHAR(255) COLLATE {TagCollation} NOT NULL PRIMARY KEY);
        INSERT INTO @ids ([StreamId])
        SELECT TOP ({StreamTagBatchSize}) CONVERT(NVARCHAR(255), s.[StreamId]) COLLATE {TagCollation}
        FROM {_qRegistry} AS s
        WHERE (@After IS NULL OR s.[StreamIdBin] > @After COLLATE {TagCollation})
        ORDER BY s.[StreamIdBin];

        INSERT INTO {_qStreamTags} ([Tag], [StreamId])
        SELECT DISTINCT
            CONVERT(NVARCHAR(256), j.[value]) COLLATE {TagCollation},
            i.[StreamId]
        FROM @ids AS i
        INNER JOIN {_qRegistry} AS s ON s.[StreamIdBin] = i.[StreamId]
        CROSS APPLY OPENJSON(CASE WHEN ISJSON(s.[Tags]) = 1 THEN s.[Tags] ELSE N'[]' END) AS j
        WHERE j.[value] IS NOT NULL AND j.[value] <> N''
          AND NOT EXISTS (
              SELECT 1 FROM {_qStreamTags} AS t
              WHERE t.[Tag] = CONVERT(NVARCHAR(256), j.[value]) COLLATE {TagCollation}
                AND t.[StreamId] = i.[StreamId]);

        SELECT MAX([StreamId]) FROM @ids;
        """;

    private async Task FillStreamTagsOnceAsync(SqlConnection connection, CancellationToken cancellationToken)
    {
        if (await StreamTagsFilledAsync(connection, cancellationToken).ConfigureAwait(false))
            return;

        await ExecuteListingAsync(connection, transaction: null, FillLockSql(acquire: true), cancellationToken).ConfigureAwait(false);
        try
        {
            if (await StreamTagsFilledAsync(connection, cancellationToken).ConfigureAwait(false))
                return;

            string? after = null;
            while (true)
            {
                var command = new SqlCommand(FillStreamTagsBatchSql(), connection) { CommandTimeout = 120 };
                await using var commandDisposal = command.ConfigureAwait(false);
                command.Parameters.Add("@After", SqlDbType.NVarChar, 255).Value = (object?)after ?? DBNull.Value;
                var next = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
                if (next is null or DBNull)
                    break;
                after = Convert.ToString(next);
            }

            await ExecuteListingAsync(connection, transaction: null, MarkStreamTagsFilledSql(), cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            await ExecuteListingAsync(connection, transaction: null, FillLockSql(acquire: false), CancellationToken.None).ConfigureAwait(false);
        }
    }

    private async Task<bool> StreamTagsFilledAsync(SqlConnection connection, CancellationToken cancellationToken)
    {
        const string sql = """
            SELECT 1
            FROM sys.extended_properties
            WHERE major_id = OBJECT_ID(@Table)
              AND name = @Name
              AND minor_id = 0
            """;
        var command = new SqlCommand(sql, connection);
        await using var commandDisposal = command.ConfigureAwait(false);
        command.Parameters.Add("@Table", SqlDbType.NVarChar, 256).Value = _qStreamTags;
        command.Parameters.Add("@Name", SqlDbType.NVarChar, 128).Value = StreamTagsFilledProperty;
        var result = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return result is not null and not DBNull;
    }

    private string MarkStreamTagsFilledSql() => $"""
        IF NOT EXISTS (
            SELECT 1 FROM sys.extended_properties
            WHERE major_id = OBJECT_ID(N'{SqlIdentifier.Literal(_qStreamTags)}')
              AND name = N'{StreamTagsFilledProperty}'
              AND minor_id = 0)
            EXEC sys.sp_addextendedproperty
                @name = N'{StreamTagsFilledProperty}',
                @value = N'1',
                @level0type = N'SCHEMA', @level0name = N'{SqlIdentifier.Literal(_schemaName)}',
                @level1type = N'TABLE', @level1name = N'{SqlIdentifier.Literal(_streamTagsTableName)}';
        """;

    private string FillLockSql(bool acquire)
    {
        var resource = FillLockResource();
        if (acquire)
        {
            return $"""
                DECLARE @result int;
                EXEC @result = sp_getapplock
                    @Resource = N'{SqlIdentifier.Literal(resource)}',
                    @LockMode = N'Exclusive',
                    @LockOwner = N'Session',
                    @LockTimeout = -1;
                IF @result < 0
                    THROW 50001, N'Could not lock the stream tag copy.', 1;
                """;
        }

        return $"""
            EXEC sp_releaseapplock
                @Resource = N'{SqlIdentifier.Literal(resource)}',
                @LockOwner = N'Session';
            """;
    }

    private string FillLockResource()
    {
        var resource = "LawnDartStreamTagsFill:" + _schemaName + ":" + _registryTableName;
        return resource.Length <= 255 ? resource : resource[..255];
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
            var tagKey = SqlIdentifier.Quote("PK_" + _streamTagsTableName);
            sql.Append("FROM ").Append(_qStreamTags).Append(" AS t WITH (INDEX(").Append(tagKey).Append(")) ");
            sql.Append("INNER JOIN ").Append(_qRegistry).Append(" AS s ON s.StreamIdBin = t.StreamId ");
            sql.Append("WHERE t.Tag = @Tag COLLATE ").Append(TagCollation).Append(" AND s.Status = 'Active'");
        }
        else if (aggregateType is not null)
        {
            sql.Append("FROM ").Append(_qRegistry).Append(" AS s WITH (INDEX([IX_AggregateType])) WHERE s.Status = 'Active'");
        }
        else
        {
            sql.Append("FROM ").Append(_qRegistry).Append(" AS s WITH (INDEX([UX_StreamIdBin])) WHERE s.Status = 'Active'");
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
        SqlTransaction? transaction,
        string sql,
        CancellationToken cancellationToken)
    {
        var command = transaction is null ? new SqlCommand(sql, connection) : new SqlCommand(sql, connection, transaction);
        command.CommandTimeout = 120;
        await using var commandDisposal = command.ConfigureAwait(false);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }
}
