using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using LawnDart.Aggregates;
using LawnDart.Dcb;
using LawnDart.EventSourcing.Aggregates;
using LawnDart.EventSourcing.Dcb;
using LawnDart.EventSourcing.SqlServer.EventStore;
using LawnDart.EventStore;
using LawnDart.Metadata;
using LawnDart.Tagging;
using Testcontainers.MsSql;

namespace LawnDart.EventSourcing.SqlServer.Tests.Tagging;

/// <summary>
/// Fallback tags and unknown-tag warnings against the SQL Server store.
/// The warning must not change which rows match.
/// </summary>
[Trait("Category", "Integration")]
public class TagFormatSqlServerTests : IAsyncLifetime
{
    private MsSqlContainer? _container;
    private SqlServerEventStore? _store;

    public async Task InitializeAsync()
    {
        _container = await MsSqlContainerStartup.StartAsync(() =>
            new MsSqlBuilder(MsSqlTestImage.Server2025).Build());
        _store = new SqlServerEventStore(
            _container.GetConnectionString(),
            options: new SqlServerEventStoreOptions { RequireTenantId = false });
        await _store.InitializeSchemaAsync();
    }

    public async Task DisposeAsync()
    {
        if (_container != null)
            await _container.DisposeAsync();
    }

    [Fact]
    public async Task FallbackFlag_Off_StoresClrTypeName()
    {
        var id = Guid.NewGuid();
        await SaveAsync(id, formatFallback: false);

        var tags = (await _store!.ReadStreamAsync(Stream(id)))[0].Tags;
        var clr = $"ProbeAggregate:{id:D}";
        var formatted = LowercaseTagFormat.Instance.Tag("ProbeAggregate", id);

        Assert.Contains(clr, tags);
        Assert.DoesNotContain(formatted, tags);
        Assert.Single((await _store.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(clr)))).Events);
        Assert.Empty((await _store.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(formatted)))).Events);
    }

    [Fact]
    public async Task FallbackFlag_On_StoresFormattedTag_AndLeavesOldRowsUnderTheClrTag()
    {
        var oldId = Guid.NewGuid();
        var newId = Guid.NewGuid();
        await SaveAsync(oldId, formatFallback: false);
        await SaveAsync(newId, formatFallback: true);

        var oldClr = $"ProbeAggregate:{oldId:D}";
        var formatted = LowercaseTagFormat.Instance.Tag("ProbeAggregate", newId);
        var newTags = (await _store!.ReadStreamAsync(Stream(newId)))[0].Tags;

        Assert.Contains(formatted, newTags);
        Assert.DoesNotContain($"ProbeAggregate:{newId:D}", newTags);
        Assert.Single((await _store.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(oldClr)))).Events);
        Assert.Empty((await _store.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(
            LowercaseTagFormat.Instance.Tag("ProbeAggregate", oldId))))).Events);
        Assert.Single((await _store.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(formatted)))).Events);
    }

    [Fact]
    public async Task Warning_FiresOnce_DoesNotFireWhenDisabled_AndDoesNotChangeRows()
    {
        var logger = new ListLogger();
        var id = Guid.NewGuid();
        var bad = $"Student:{id:D}";
        var good = LowercaseTagFormat.Instance.Tag("student", id);
        var loud = Dcb(logger, warn: true);

        await loud.AppendWithContextAsync([new ProbeEvent(Guid.NewGuid(), DateTime.UtcNow)], [bad]);
        var first = await loud.GetStateAsync<CountingState>([bad]);
        var second = await loud.GetStateAsync<CountingState>([bad]);
        var formatted = await loud.GetStateAsync<CountingState>([good]);

        Assert.Equal(1, first.Count);
        Assert.Equal(1, second.Count);
        Assert.Equal(0, formatted.Count);
        Assert.Equal(1, logger.WarningsFor(bad));
        Assert.Equal(0, logger.WarningsFor(good));

        var quietTag = $"Section:{Guid.NewGuid():D}";
        var quiet = Dcb(logger, warn: false);
        await quiet.GetStateAsync<CountingState>([quietTag]);
        await quiet.AppendWithContextAsync(
            [new ProbeEvent(Guid.NewGuid(), DateTime.UtcNow)],
            [LowercaseTagFormat.Instance.Tag("section", Guid.NewGuid())],
            AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags(quietTag))));

        Assert.Equal(0, logger.WarningsFor(quietTag));
    }

    [Fact]
    public async Task Warning_SkipsTagsTheFormatWouldProduce()
    {
        var logger = new ListLogger();
        var repo = Dcb(logger, warn: true);
        var student = LowercaseTagFormat.Instance.Tag("student", Guid.NewGuid());
        var tenant = LowercaseTagFormat.Instance.Tag("tenant", "academy-tenant");

        await repo.AppendWithContextAsync([new ProbeEvent(Guid.NewGuid(), DateTime.UtcNow)], [student, tenant]);
        var state = await repo.GetStateAsync<CountingState>([student, tenant]);

        Assert.Equal(1, state.Count);
        Assert.Empty(logger.Warnings);
    }

    private DcbRepository Dcb(ListLogger logger, bool warn)
        => new(
            _store!,
            new DefaultMetadataProvider(),
            new NullTenant(),
            Options.Create(new LawnDartOptions { RequireTenantId = false }),
            tagProvider: null,
            authorizationService: null,
            logger,
            Options.Create(new EventSourcingOptions { WarnOnUnknownTag = warn }),
            dcbSnapshotStore: null,
            strategyResolver: null,
            snapshotWriteQueue: null,
            tagFormat: LowercaseTagFormat.Instance);

    private async Task SaveAsync(Guid id, bool formatFallback)
    {
        var repository = new AggregateRepository(
            _store!,
            new DefaultMetadataProvider(),
            new NullTenant(),
            Options.Create(new LawnDartOptions { RequireTenantId = false }),
            tagProvider: null,
            authorizationService: null,
            logger: null,
            snapshotStore: null,
            strategyResolver: null,
            snapshotWriteQueue: null,
            tagFormat: LowercaseTagFormat.Instance,
            eventSourcingOptions: Options.Create(new EventSourcingOptions
            {
                FormatAggregateFallbackTag = formatFallback
            }));
        var aggregate = await repository.CreateAsync<ProbeAggregate>(Stream(id));
        aggregate.Raise(new ProbeEvent(Guid.NewGuid(), DateTime.UtcNow));
        await repository.SaveAsync(aggregate, new CommandMetadata());
    }

    private static string Stream(Guid id) => $"acct:ProbeAggregate:{id:D}";

    private sealed class NullTenant : ITenantContextProvider
    {
        public string? GetTenantId() => null;
        public string GetTenantIdRequired() => throw new InvalidOperationException();
    }

    private sealed class ProbeState : IState;

    private sealed class ProbeAggregate : AggregateRoot<ProbeState>
    {
        public void Raise(IEvent @event) => Apply(@event);
        protected override void ApplyEventToState(IEvent @event) { }
    }

    [EventTypeName("tag-format-sql.probe")]
    private sealed record ProbeEvent(Guid Id, DateTime Timestamp) : IEvent;

    private sealed class CountingState : IState
    {
        public int Count { get; private set; }
        public void Apply(ProbeEvent @event) => Count++;
    }

    private sealed class ListLogger : ILogger<DcbRepository>
    {
        public List<string> Warnings { get; } = [];
        public int WarningsFor(string tag) => Warnings.Count(message => message.Contains(tag, StringComparison.Ordinal));
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => true;
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (logLevel == LogLevel.Warning)
                Warnings.Add(formatter(state, exception));
        }
    }
}
