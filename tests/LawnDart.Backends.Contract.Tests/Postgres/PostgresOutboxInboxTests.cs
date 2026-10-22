using System.Collections.Concurrent;
using LawnDart.EventSourcing.Postgres.EventStore;
using LawnDart.EventSourcing.Postgres.Outbox;
using LawnDart.EventStore;
using LawnDart.Outbox;
using Npgsql;

namespace LawnDart.Backends.Contract.Tests;

/// <summary>
/// Outbox and inbox behavior that the shared contract suite does not cover.
/// </summary>
[Collection("PostgresContract")]
[Trait("Category", "Integration")]
public sealed class PostgresOutboxInboxTests
{
    private readonly PostgresContractFixture _fixture;

    public PostgresOutboxInboxTests(PostgresContractFixture fixture) => _fixture = fixture;

    [Fact]
    public async Task TwoProcessors_ClaimDisjointRows()
    {
        var writer = await _fixture.Backend.OpenOutboxAsync(ContractStores.Isolated());
        for (var i = 1; i <= 20; i++)
            await writer.WriteAsync(Message(i));

        var claimed = new ConcurrentBag<Guid>();
        var tasks = Enumerable.Range(0, 8).Select(async _ =>
        {
            foreach (var row in await writer.GetUnprocessedAsync(5))
                claimed.Add(row.Id);
        });
        await Task.WhenAll(tasks);

        var ids = claimed.ToArray();
        Assert.Equal(20, ids.Length);
        Assert.Equal(20, ids.Distinct().Count());
    }

    [Fact]
    public async Task ExpiredClaim_IsClaimedAgain()
    {
        var options = ContractStores.Isolated(outboxClaimLease: TimeSpan.FromHours(1));
        var writer = (PostgresOutboxWriter)await _fixture.Backend.OpenOutboxAsync(options);
        var message = Message(1);
        await writer.WriteAsync(message);
        Assert.Equal(message.Id, Assert.Single(await writer.GetUnprocessedAsync(10)).Id);
        Assert.Empty(await writer.GetUnprocessedAsync(10));

        await using var connection = new NpgsqlConnection(_fixture.ConnectionString);
        await connection.OpenAsync();
        await using var expire = new NpgsqlCommand(
            $"UPDATE {writer.QualifiedTableName} SET claimed_until = clock_timestamp() - interval '1 second' WHERE id = @id",
            connection);
        expire.Parameters.AddWithValue("id", message.Id);
        Assert.Equal(1, await expire.ExecuteNonQueryAsync());

        Assert.Equal(message.Id, Assert.Single(await writer.GetUnprocessedAsync(10)).Id);
    }

    [Fact]
    public async Task RolledBackAppend_LeavesNoOutboxRow()
    {
        var opened = await _fixture.Backend.OpenEventStoreAsync(ContractStores.Isolated(enableOutbox: true));
        var store = (PostgresEventStore)opened.Store;
        var streamId = "acme:Order:" + Guid.NewGuid().ToString("N");
        await store.AppendAndRollbackAsync(streamId, [
            new AppendEvent("contract.outbox.rollback", """{"n":1}"""u8.ToArray(), metadata: "{}"u8.ToArray())
        ]);

        Assert.Empty(await store.ReadStreamAsync(streamId));
        Assert.Empty(await opened.Outbox.GetUnprocessedAsync(10));
    }

    [Fact]
    public async Task ParallelMarks_OfOneId_LeaveOneRow()
    {
        var inbox = await _fixture.Backend.OpenInboxAsync(ContractStores.Isolated());
        var processedAt = DateTimeOffset.UtcNow;
        var tasks = Enumerable.Range(0, 20).Select(_ => inbox.Store.MarkProcessedAsync("same-id", processedAt));
        await Task.WhenAll(tasks);
        Assert.True(await inbox.Store.IsProcessedAsync("same-id"));
        Assert.Equal(1, await inbox.CountRowsAsync!(CancellationToken.None));
    }

    [Fact]
    public async Task InitializeSchemaAsync_AddsOutboxOnAnAlpha1Schema_AndTheOldAppendFunctionStillWorks()
    {
        var opened = await _fixture.Backend.OpenEventStoreAsync(ContractStores.Isolated());
        var store = (PostgresEventStore)opened.Store;
        var schema = store.SchemaName;
        var quoted = Quote(schema);

        await using (var connection = new NpgsqlConnection(_fixture.AdminConnectionString))
        {
            await connection.OpenAsync();
            var escaped = schema.Replace("'", "''", StringComparison.Ordinal);
            await using var drop = new NpgsqlCommand(
                $"""
                DROP TABLE IF EXISTS {quoted}."outbox";
                DELETE FROM {quoted}.schema_info WHERE component = 'outbox';
                DO $drop$
                DECLARE sig regprocedure;
                BEGIN
                    FOR sig IN
                        SELECT p.oid::regprocedure
                        FROM pg_proc p
                        JOIN pg_namespace n ON n.oid = p.pronamespace
                        WHERE n.nspname = '{escaped}'
                          AND p.proname = 'append_to_stream'
                          AND p.pronargs = 37
                    LOOP
                        EXECUTE 'DROP FUNCTION ' || sig::text;
                    END LOOP;
                END
                $drop$;
                """,
                connection);
            await drop.ExecuteNonQueryAsync();
        }

        var first = "acme:Order:" + Guid.NewGuid().ToString("N");
        await store.AppendAsync(first, [ContractEvent.Tick()]);
        Assert.Single(await store.ReadStreamAsync(first));

        await store.InitializeSchemaAsync();

        await using (var connection = new NpgsqlConnection(_fixture.AdminConnectionString))
        {
            await connection.OpenAsync();
            await using var format = new NpgsqlCommand(
                $"SELECT format FROM {quoted}.schema_info WHERE component = 'events'",
                connection);
            Assert.Equal(1, Convert.ToInt32(await format.ExecuteScalarAsync()));

            await using var args = new NpgsqlCommand(
                """
                SELECT p.pronargs
                FROM pg_proc p
                JOIN pg_namespace n ON n.oid = p.pronamespace
                WHERE n.nspname = @schema AND p.proname = 'append_to_stream'
                ORDER BY p.pronargs
                """,
                connection);
            args.Parameters.AddWithValue("schema", schema);
            var counts = new List<int>();
            await using (var reader = await args.ExecuteReaderAsync())
            {
                while (await reader.ReadAsync())
                    counts.Add(reader.GetInt32(0));
            }

            Assert.Equal(new[] { 34, 37 }, counts);

            await using var options = new NpgsqlCommand(
                """
                SELECT reloptions
                FROM pg_class
                WHERE oid = to_regclass(@table)
                """,
                connection);
            options.Parameters.AddWithValue("table", quoted + ".\"outbox\"");
            var reloptions = (string[])(await options.ExecuteScalarAsync())!;
            Assert.Contains("autovacuum_vacuum_scale_factor=0.01", reloptions);
            Assert.Contains("autovacuum_vacuum_threshold=1000", reloptions);
        }

        var second = "acme:Order:" + Guid.NewGuid().ToString("N");
        await store.AppendAsync(second, [ContractEvent.Tick()]);
        Assert.Empty(await opened.Outbox.GetUnprocessedAsync(10));

        var enabled = await _fixture.Backend.OpenEventStoreAsync(
            new ContractStoreOptions
            {
                SchemaName = schema,
                RequireTenantId = false,
                EnableOutbox = true,
                OutboxClaimLease = TimeSpan.Zero
            });
        var third = "acme:Order:" + Guid.NewGuid().ToString("N");
        await enabled.Store.AppendAsync(third, [ContractEvent.Tick()]);
        Assert.Equal(third, Assert.Single(await enabled.Outbox.GetUnprocessedAsync(10)).StreamId);
    }

    private static OutboxMessage Message(long sequence)
        => new()
        {
            Id = Guid.NewGuid(),
            EventType = "contract.outbox",
            SchemaVersion = 1,
            CodecId = 1,
            Payload = "{}"u8.ToArray(),
            Metadata = "{}",
            CreatedAt = DateTime.UtcNow,
            Attempts = 0,
            StreamId = "acme:Order:outbox",
            SequencePosition = sequence
        };

    private static string Quote(string name) => "\"" + name.Replace("\"", "\"\"", StringComparison.Ordinal) + "\"";
}
