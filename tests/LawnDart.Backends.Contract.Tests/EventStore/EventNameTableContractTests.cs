using LawnDart.EventSourcing.Postgres.EventStore;
using LawnDart.EventSourcing.SqlServer.EventStore;
using LawnDart.EventStore;
using Microsoft.Data.SqlClient;
using Npgsql;

namespace LawnDart.Backends.Contract.Tests;

/// <summary>
/// Event-name table checks for the SQL Server and Postgres stores. These read the
/// table directly, so they stay with LawnDart's own backends and are not in the package.
/// </summary>
public abstract class EventNameTableContractTests : BackendContractSuite
{
    protected EventNameTableContractTests(IFullContractBackend backend)
        : base(backend)
    {
    }

    [ContractFact]
    public async Task SchemaInit_RecordsCatalogEventNames_AndASecondCallIsHarmless()
    {
        SkipUnless(
            Cap.UnknownEventNameMarksQueryUnresolved,
            "This store has no event-name table.");

        var storedName = EventTypeCatalog.TryGetDeclaredName(typeof(ContractTestEvent))!;
        var otherName = EventTypeCatalog.TryGetDeclaredName(typeof(ContractAnotherEvent))!;
        var opened = await Backend.OpenEventStoreAsync(ContractStores.Isolated(
            eventTypes: [typeof(ContractTestEvent), typeof(ContractAnotherEvent)]));

        Assert.Equal(1, await CountEventNamesAsync(opened, storedName));
        Assert.Equal(1, await CountEventNamesAsync(opened, otherName));

        await opened.Store.AppendAsync(Stream(), [ContractEvent.Tick()]);
        var query = Query.FromItems(QueryItem.ByType(storedName, otherName));
        var read = await opened.Store.ReadByQueryAsync(query);
        var sequenced = Assert.Single(read.Events);
        Assert.IsType<ContractTestEvent>(sequenced.Event);
        Assert.True(read.TypesResolved);

        await InitializeSchemaAsync(opened);
        await Task.WhenAll(InitializeSchemaAsync(opened), InitializeSchemaAsync(opened));

        Assert.Equal(1, await CountEventNamesAsync(opened, storedName));
        Assert.Equal(1, await CountEventNamesAsync(opened, otherName));
        var again = await opened.Store.ReadByQueryAsync(query);
        Assert.Single(again.Events);
        Assert.True(again.TypesResolved);
    }

    [ContractFact]
    public async Task ReadByQuery_NameStoredDuringTheRead_DoesNotSkipTheEarlierEvent()
    {
        SkipUnless(
            Cap.UnknownEventNameMarksQueryUnresolved,
            "This store has no event-name table.");

        var schema = "c" + Guid.NewGuid().ToString("N");
        var options = new ContractStoreOptions { SchemaName = schema, RequireTenantId = false };
        var writer = await Backend.OpenEventStoreAsync(options);
        var reader = await Backend.OpenEventStoreAsync(options);
        var reloadGate = new ReloadGate();
        if (reader.Store is PostgresEventStore postgres)
        {
            // A pooled connection reopens too quickly for the writer to land
            // between the name reload and the read limit. Wait here, after the
            // reload has released its lock, until this round's writes commit.
            postgres.AfterEventNameReload = _ => reloadGate.Current?.Task ?? Task.CompletedTask;
        }

        const string known = "race.known";
        await writer.Log.AppendAsync(Stream(), [new AppendEvent(known, "{}"u8.ToArray())]);
        var primed = await reader.Log.ReadByQueryAsync(Query.FromItems(QueryItem.ByType(known)));
        var cursor = Assert.Single(primed.Events).SequencePosition;

        // A few hundred rounds is enough to catch a read that returns the later
        // event and skips the earlier one. SQL Server needs a longer loop.
        const int rounds = 1000;
        for (var round = 0; round < rounds; round++)
        {
            var fresh = "race.new." + round.ToString("D4") + "." + Guid.NewGuid().ToString("N");
            var query = Query.FromItems(QueryItem.ByType(known, fresh));
            var readsStarted = 0;
            var writesFinished = 0;
            long freshPosition = 0;
            long knownPosition = 0;
            var seen = new Dictionary<long, string>();
            var roundWrites = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            reloadGate.Current = roundWrites;

            var reading = Task.Run(async () =>
            {
                var local = cursor;
                var deadline = 0L;
                while (deadline == 0 || Environment.TickCount64 < deadline)
                {
                    Interlocked.Increment(ref readsStarted);
                    var read = await reader.Log.ReadByQueryAsync(
                        query,
                        fromSequencePosition: local + 1);
                    foreach (var recorded in read.Events)
                    {
                        seen[recorded.SequencePosition] = recorded.EventType;
                        if (recorded.SequencePosition > local)
                            local = recorded.SequencePosition;
                    }

                    var wantFresh = Volatile.Read(ref freshPosition);
                    var wantKnown = Volatile.Read(ref knownPosition);
                    if (wantFresh != 0 && seen.ContainsKey(wantFresh) && seen.ContainsKey(wantKnown))
                        break;
                    if (Volatile.Read(ref writesFinished) == 1 && deadline == 0)
                        deadline = Environment.TickCount64 + 5000;
                }

                return local;
            });

            while (Volatile.Read(ref readsStarted) == 0)
                await Task.Yield();

            var freshAppend = await writer.Log.AppendAsync(Stream(), [new AppendEvent(fresh, "{}"u8.ToArray())]);
            var knownAppend = await writer.Log.AppendAsync(Stream(), [new AppendEvent(known, "{}"u8.ToArray())]);
            Volatile.Write(ref freshPosition, freshAppend.SequencePositions[0]);
            Volatile.Write(ref knownPosition, knownAppend.SequencePositions[0]);
            Volatile.Write(ref writesFinished, 1);
            roundWrites.TrySetResult();

            cursor = await reading;
            Assert.True(
                seen.ContainsKey(freshPosition),
                $"Round {round} returned {known} at {knownPosition} and skipped {fresh} at {freshPosition}.");
            Assert.True(seen.ContainsKey(knownPosition), $"Round {round} did not return {known} at {knownPosition}.");
            Assert.True(freshPosition < knownPosition);
        }
    }

    private static string Stream() => "test-tenant:TestAggregate:" + Guid.NewGuid().ToString("N");

    private sealed class ReloadGate
    {
        public volatile TaskCompletionSource? Current;
    }

    private static Task InitializeSchemaAsync(ContractEventStore opened)
    {
        if (opened.Store is SqlServerEventStore sql)
            return sql.InitializeSchemaAsync();
        return ((PostgresEventStore)opened.Store).InitializeSchemaAsync();
    }

    private static async Task<int> CountEventNamesAsync(ContractEventStore opened, string name)
    {
        if (opened.Store is SqlServerEventStore sql)
        {
            var hooks = (SqlServerContractHooks)opened.Hooks;
            await using var connection = new SqlConnection(hooks.ConnectionString);
            await connection.OpenAsync();
            await using var command = new SqlCommand(
                $"SELECT COUNT(*) FROM {sql.QualifiedTypesTable} WHERE Token = @Token",
                connection);
            command.Parameters.AddWithValue("@Token", name);
            return (int)(await command.ExecuteScalarAsync())!;
        }

        var postgres = (PostgresEventStore)opened.Store;
        await using var pg = new NpgsqlConnection(postgres.ConnectionString);
        await pg.OpenAsync();
        await using var query = new NpgsqlCommand(
            $"SELECT COUNT(*) FROM {postgres.QualifiedTypesTable} WHERE token = @token",
            pg);
        query.Parameters.AddWithValue("token", name);
        return Convert.ToInt32(await query.ExecuteScalarAsync());
    }
}
