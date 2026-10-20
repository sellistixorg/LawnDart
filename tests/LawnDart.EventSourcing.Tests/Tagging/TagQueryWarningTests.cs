using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using LawnDart.Dcb;
using LawnDart.EventSourcing.Dcb;
using LawnDart.EventSourcing.EventStore;
using LawnDart.EventStore;
using LawnDart.Metadata;
using LawnDart.Tagging;

namespace LawnDart.EventSourcing.Tests.Tagging;

public class TagQueryWarningTests
{
    [Fact]
    public async Task Warning_FiresOnce_AndDoesNotChangeResults()
    {
        var store = new InMemoryEventStore(options: new InMemoryEventStoreOptions { RequireTenantId = false });
        var logger = new ListLogger();
        var repo = Repository(store, logger, warn: true);
        var id = Guid.NewGuid();
        var bad = $"Student:{id:D}";
        var good = LowercaseTagFormat.Instance.Tag("student", id);

        await repo.AppendWithContextAsync([Event()], [bad]);

        var first = await repo.GetStateAsync<CountingState>([bad]);
        var second = await repo.GetStateAsync<CountingState>([bad]);
        var formatted = await repo.GetStateAsync<CountingState>([good]);

        Assert.Equal(1, first.Count);
        Assert.Equal(1, second.Count);
        Assert.Equal(0, formatted.Count);
        Assert.Equal(1, logger.WarningsFor(bad));
        Assert.Equal(0, logger.WarningsFor(good));
    }

    [Fact]
    public async Task Warning_SameBadType_LogsOnceAcrossIds()
    {
        var store = new InMemoryEventStore(options: new InMemoryEventStoreOptions { RequireTenantId = false });
        var logger = new ListLogger();
        var repo = Repository(store, logger, warn: true);
        var first = $"Student:{Guid.NewGuid():D}";
        var second = $"Student:{Guid.NewGuid():D}";

        await repo.GetStateAsync<CountingState>([first]);
        await repo.GetStateAsync<CountingState>([second]);

        var other = Repository(new InMemoryEventStore(options: new InMemoryEventStoreOptions { RequireTenantId = false }), logger, warn: true);
        await other.GetStateAsync<CountingState>([$"Student:{Guid.NewGuid():D}"]);

        Assert.Equal(1, logger.WarningsFor(first));
        Assert.Equal(0, logger.WarningsFor(second));
        Assert.Equal(2, logger.Warnings.Count(message => message.Contains("Student:", StringComparison.Ordinal)));
    }

    [Fact]
    public async Task Warning_Cap_SuppressesFurtherTypes()
    {
        var store = new InMemoryEventStore(options: new InMemoryEventStoreOptions { RequireTenantId = false });
        var logger = new ListLogger();
        var repo = Repository(store, logger, warn: true, warnings: new UnknownTagWarningLog(maxTypes: 2));

        await repo.GetStateAsync<CountingState>([$"TypeA:{Guid.NewGuid():D}"]);
        await repo.GetStateAsync<CountingState>([$"TypeB:{Guid.NewGuid():D}"]);
        await repo.GetStateAsync<CountingState>([$"TypeC:{Guid.NewGuid():D}"]);
        await repo.GetStateAsync<CountingState>([$"TypeD:{Guid.NewGuid():D}"]);
        await repo.GetStateAsync<CountingState>([$"TypeA:{Guid.NewGuid():D}"]);

        Assert.Equal(1, logger.WarningsFor("TypeA:"));
        Assert.Equal(1, logger.WarningsFor("TypeB:"));
        Assert.Equal(0, logger.WarningsFor("TypeC:"));
        Assert.Equal(0, logger.WarningsFor("TypeD:"));
        Assert.Equal(1, logger.Warnings.Count(message => message.Contains("suppressed", StringComparison.Ordinal)));
    }

    [Fact]
    public async Task Warning_DoesNotFireWhenDisabled()
    {
        var store = new InMemoryEventStore(options: new InMemoryEventStoreOptions { RequireTenantId = false });
        var logger = new ListLogger();
        var bad = $"Student:{Guid.NewGuid():D}";
        var quiet = Repository(store, logger, warn: false);

        await quiet.GetStateAsync<CountingState>([bad]);
        await quiet.GetStateAsync<CountingState>([bad]);

        Assert.Empty(logger.Warnings);

        var loud = Repository(store, logger, warn: true);
        await loud.GetStateAsync<CountingState>([bad]);
        await loud.GetStateAsync<CountingState>([bad]);

        Assert.Equal(1, logger.WarningsFor(bad));
    }

    [Fact]
    public async Task Warning_SkipsTagsTheFormatWouldProduce()
    {
        var store = new InMemoryEventStore(options: new InMemoryEventStoreOptions { RequireTenantId = false });
        var logger = new ListLogger();
        var repo = Repository(store, logger, warn: true);
        var format = LowercaseTagFormat.Instance;
        var student = format.Tag("student", Guid.NewGuid());
        var tenant = format.Tag("tenant", "academy-tenant");

        await repo.AppendWithContextAsync([Event()], [student, tenant]);
        var state = await repo.GetStateAsync<CountingState>([student, tenant]);

        Assert.Equal(1, state.Count);
        Assert.Empty(logger.Warnings);
    }

    [Fact]
    public async Task Warning_FiresForGuidFormatN_AndForFailIfMatches()
    {
        var store = new InMemoryEventStore(options: new InMemoryEventStoreOptions { RequireTenantId = false });
        var logger = new ListLogger();
        var repo = Repository(store, logger, warn: true);
        var compact = $"student:{Guid.NewGuid():N}";
        var fence = $"Section:{Guid.NewGuid():D}";
        var condition = AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags(fence)));

        await repo.GetLastSequencePositionAsync([compact]);
        await repo.AppendWithContextAsync(
            [Event()],
            [LowercaseTagFormat.Instance.Tag("section", Guid.NewGuid())],
            condition);
        await repo.AppendWithContextAsync(
            [Event()],
            [LowercaseTagFormat.Instance.Tag("section", Guid.NewGuid())],
            condition);

        Assert.Equal(1, logger.WarningsFor(compact));
        Assert.Equal(1, logger.WarningsFor(fence));
    }

    [Fact]
    public async Task Warning_WhenFormatThrows_DoesNotChangeTheQuery()
    {
        var store = new InMemoryEventStore(options: new InMemoryEventStoreOptions { RequireTenantId = false });
        var logger = new ListLogger();
        var repo = Repository(store, logger, warn: true, format: new ThrowingFormat());
        var tag = $"student:{Guid.NewGuid():D}";

        await repo.AppendWithContextAsync([Event()], [tag]);
        var state = await repo.GetStateAsync<CountingState>([tag]);

        Assert.Equal(1, state.Count);
        Assert.Equal(1, logger.WarningsFor(tag));
    }

    [Fact]
    public async Task ProjectorAndReactor_WarnOnce_AndKeepTheirResults()
    {
        var store = new InMemoryEventStore(options: new InMemoryEventStoreOptions { RequireTenantId = false });
        var logger = new ListLogger();
        var projectionTag = $"Order:{Guid.NewGuid():D}";
        var reactionTag = $"Payment:{Guid.NewGuid():D}";
        await store.AppendAsync(
            $"dcb:{Guid.NewGuid():N}",
            [new NotedEvent(Guid.NewGuid(), DateTime.UtcNow, "seen")],
            tags: [projectionTag]);

        var projector = new NotedProjector(projectionTag);
        projector.UseTagFormat(LowercaseTagFormat.Instance, logger);
        await projector.InitializeFromHistoryAsync(store);
        await projector.InitializeFromHistoryAsync(store);

        var reactor = new NotedReactor(reactionTag);
        reactor.UseTagFormat(LowercaseTagFormat.Instance, logger);
        var first = await reactor.ReactAsync(new NotedEvent(Guid.NewGuid(), DateTime.UtcNow, "go"), new EventMetadata());
        var second = await reactor.ReactAsync(new NotedEvent(Guid.NewGuid(), DateTime.UtcNow, "go"), new EventMetadata());

        Assert.Equal(["seen", "seen"], projector.Seen);
        Assert.Equal("go", Assert.IsType<NotedCommand>(Assert.Single(first)).Value);
        Assert.Equal("go", Assert.IsType<NotedCommand>(Assert.Single(second)).Value);
        Assert.Equal(1, logger.WarningsFor(projectionTag));
        Assert.Equal(1, logger.WarningsFor(reactionTag));
    }

    [Fact]
    public async Task Projector_WarnOnUnknownTagFalse_DoesNotLog()
    {
        var logger = new ListLogger();
        var bad = $"Order:{Guid.NewGuid():D}";
        var projector = new NotedProjector(bad);
        projector.UseTagFormat(LowercaseTagFormat.Instance, logger, warnOnUnknownTag: false);

        await projector.InitializeFromHistoryAsync(new InMemoryEventStore(options: new InMemoryEventStoreOptions { RequireTenantId = false }));

        Assert.Empty(logger.Warnings);
    }

    private static DcbRepository Repository(
        InMemoryEventStore store,
        ListLogger logger,
        bool warn,
        ITagFormat? format = null,
        UnknownTagWarningLog? warnings = null)
        => new(
            store,
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
            tagFormat: format ?? LowercaseTagFormat.Instance,
            unknownTagWarnings: warnings);

    private static NotedEvent Event() => new(Guid.NewGuid(), DateTime.UtcNow, "n");

    private sealed class NullTenant : ITenantContextProvider
    {
        public string? GetTenantId() => null;
        public string GetTenantIdRequired() => throw new InvalidOperationException();
    }

    private sealed class CountingState : IState
    {
        public int Count { get; private set; }
        public void Apply(NotedEvent @event) => Count++;
    }

    [EventTypeName("tag-query-warning.noted")]
    private sealed record NotedEvent(Guid Id, DateTime Timestamp, string Value) : IEvent;

    private sealed class NotedCommand : ICommand
    {
        public Guid Id { get; } = Guid.NewGuid();
        public string Value { get; init; } = "";
    }

    private sealed class NotedProjector : DcbProjector<CountingState>
    {
        private readonly string _tag;
        public List<string> Seen { get; } = [];
        public NotedProjector(string tag) => _tag = tag;
        public override string[] GetTagsForProjection() => [_tag];
        public override Task ProjectEventAsync(IEvent @event, EventMetadata metadata, CancellationToken cancellationToken = default)
        {
            if (@event is NotedEvent noted)
                Seen.Add(noted.Value);
            return Task.CompletedTask;
        }
    }

    private sealed class NotedReactor : DcbReactor<NotedEvent>
    {
        private readonly string _tag;
        public NotedReactor(string tag) => _tag = tag;
        public override string[] GetTagsForReaction() => [_tag];
        protected override Task<IEnumerable<ICommand>> ReactToEventAsync(NotedEvent @event, EventMetadata metadata, CancellationToken cancellationToken = default)
            => Task.FromResult<IEnumerable<ICommand>>([new NotedCommand { Value = @event.Value }]);
    }

    private sealed class ThrowingFormat : ITagFormat
    {
        public string Tag(string type, Guid id) => throw new InvalidOperationException("format failed");
        public string Tag(string type, string id) => throw new InvalidOperationException("format failed");
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
