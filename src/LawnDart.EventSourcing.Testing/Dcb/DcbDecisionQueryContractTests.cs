using LawnDart;
using LawnDart.Dcb;
using LawnDart.EventSourcing;
using LawnDart.EventSourcing.Dcb;
using LawnDart.EventStore;
using LawnDart.Metadata;
using Microsoft.Extensions.Options;

namespace LawnDart.EventSourcing.Testing;

/// <summary>
/// A decision query is OR across items. The same query loads the entity and guards the append.
/// An event that matches two items is applied once. Each new event is stored with only the tags
/// passed to Emit.
/// </summary>
public abstract class DcbDecisionQueryContractTests : ContractSuite
{
    private const string Borrowed = "backend-contract.decision-borrowed";
    private const int Rounds = 4;

    private static readonly Type[] ProbeTypes =
    [
        typeof(ContractDecisionAdded),
        typeof(ContractDecisionBorrowed),
        typeof(ContractDecisionNoted),
        typeof(ContractDecisionFiller)
    ];

    protected DcbDecisionQueryContractTests(IContractBackend backend)
        : base(backend)
    {
    }

    [Fact]
    public async Task Load_SeesEventThatMatchesOnlyTheFirstItem()
    {
        var (_, repository) = await OpenAsync();
        var book = Tag("book");
        var member = Tag("member");
        await repository.AppendWithContextAsync(
            [new TaggedEvent(new ContractDecisionAdded(Guid.NewGuid()), [book])]);

        var entity = await repository.GetOrCreateEntityAsync<DecisionProbe>(LoanQuery(book, member));

        Assert.Equal(["added"], entity.State.Kinds);
    }

    [Fact]
    public async Task Load_SeesEventThatMatchesOnlyTheSecondItem_AndSkipsADifferentType()
    {
        var (_, repository) = await OpenAsync();
        var book = Tag("book");
        var member = Tag("member");
        var otherBook = Tag("book");
        await repository.AppendWithContextAsync(
            [new TaggedEvent(new ContractDecisionBorrowed(Guid.NewGuid()), [member, otherBook])]);
        await repository.AppendWithContextAsync(
            [new TaggedEvent(new ContractDecisionNoted(Guid.NewGuid()), [member])]);

        var entity = await repository.GetOrCreateEntityAsync<DecisionProbe>(LoanQuery(book, member));

        Assert.Equal(["borrowed"], entity.State.Kinds);
    }

    [Fact]
    public async Task Load_AppliesEventThatMatchesBothItemsOnce()
    {
        var (_, repository) = await OpenAsync();
        var book = Tag("book");
        var member = Tag("member");
        await repository.AppendWithContextAsync(
            [new TaggedEvent(new ContractDecisionBorrowed(Guid.NewGuid()), [book, member])]);

        var entity = await repository.GetOrCreateEntityAsync<DecisionProbe>(LoanQuery(book, member));

        Assert.Equal(1, entity.State.Applied);
        Assert.Equal(["borrowed"], entity.State.Kinds);
    }

    [Fact]
    public async Task LastSequencePosition_IsTheGreatestSequenceInTheUnion()
    {
        var (_, repository) = await OpenAsync();
        var book = Tag("book");
        var member = Tag("member");
        var query = LoanQuery(book, member);
        var first = (await repository.AppendWithContextAsync(
            [new TaggedEvent(new ContractDecisionAdded(Guid.NewGuid()), [book])]))[0];
        await repository.AppendWithContextAsync(
            [new TaggedEvent(new ContractDecisionFiller(Guid.NewGuid()), [Tag("other")])]);
        var second = (await repository.AppendWithContextAsync(
            [new TaggedEvent(new ContractDecisionBorrowed(Guid.NewGuid()), [member, Tag("book")])]))[0];
        await repository.AppendWithContextAsync(
            [new TaggedEvent(new ContractDecisionFiller(Guid.NewGuid()), [Tag("other")])]);

        var entity = await repository.GetOrCreateEntityAsync<DecisionProbe>(query);

        Assert.True(second > first);
        Assert.Equal(second, entity.LastSequencePosition);
        Assert.Equal(2, entity.State.Applied);
        Assert.Equal(second, await repository.GetLastSequencePositionAsync(query));
    }

    [Fact]
    public async Task HandleCommandAsync_WritesOnlyTheTagsPassedToEmit()
    {
        var (store, repository) = await OpenAsync();
        var book = Tag("book");
        var member = Tag("member");
        var replayed = Tag("replayed");
        var writtenTag = Tag("written");
        await repository.AppendWithContextAsync(
            [new TaggedEvent(new ContractDecisionAdded(Guid.NewGuid()), [book, replayed])]);

        var entity = await repository.GetOrCreateEntityAsync<DecisionProbe>(LoanQuery(book, member));
        await repository.HandleCommandAsync(
            entity,
            new EmitProbe(new ContractDecisionNoted(Guid.NewGuid()), [writtenTag]));

        var written = Assert.Single((await store.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(writtenTag)))).Events);
        Assert.Equal([writtenTag], written.Tags.Order(StringComparer.Ordinal).ToArray());
        Assert.DoesNotContain(book, written.Tags);
        Assert.DoesNotContain(member, written.Tags);
        Assert.DoesNotContain(replayed, written.Tags);
    }

    [Fact]
    public async Task SecondLoad_SeesTheNewEvent_AQueryForTheOtherTagDoesNot()
    {
        var (store, repository) = await OpenAsync();
        var book = Tag("book");
        var member = Tag("member");
        var query = LoanQuery(book, member);
        var entity = await repository.GetOrCreateEntityAsync<DecisionProbe>(query);
        await repository.HandleCommandAsync(
            entity,
            new EmitProbe(new ContractDecisionAdded(Guid.NewGuid()), [book]));

        var again = await repository.GetOrCreateEntityAsync<DecisionProbe>(query);
        Assert.Equal(["added"], again.State.Kinds);
        Assert.Empty((await store.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(member)))).Events);
    }

    [Fact]
    public async Task AppendCondition_EventMatchingOnlyTheFirstItem_Throws()
    {
        var (_, repository) = await OpenAsync();
        var book = Tag("book");
        var member = Tag("member");
        await repository.AppendWithContextAsync(
            [new TaggedEvent(new ContractDecisionAdded(Guid.NewGuid()), [book])]);
        var entity = await repository.GetOrCreateEntityAsync<DecisionProbe>(LoanQuery(book, member));
        await repository.AppendWithContextAsync(
            [new TaggedEvent(new ContractDecisionAdded(Guid.NewGuid()), [book])]);

        await Assert.ThrowsAsync<ConcurrencyException>(() => repository.HandleCommandAsync(
            entity,
            new EmitProbe(new ContractDecisionNoted(Guid.NewGuid()), [book])));
    }

    [Fact]
    public async Task AppendCondition_EventMatchingOnlyTheSecondItem_Throws()
    {
        var (_, repository) = await OpenAsync();
        var book = Tag("book");
        var member = Tag("member");
        await repository.AppendWithContextAsync(
            [new TaggedEvent(new ContractDecisionAdded(Guid.NewGuid()), [book])]);
        var entity = await repository.GetOrCreateEntityAsync<DecisionProbe>(LoanQuery(book, member));
        await repository.AppendWithContextAsync(
            [new TaggedEvent(new ContractDecisionBorrowed(Guid.NewGuid()), [member, Tag("book")])]);

        await Assert.ThrowsAsync<ConcurrencyException>(() => repository.HandleCommandAsync(
            entity,
            new EmitProbe(new ContractDecisionNoted(Guid.NewGuid()), [book])));
    }

    [Fact]
    public async Task AppendCondition_EventMatchingNeitherItem_DoesNotThrow()
    {
        var (store, repository) = await OpenAsync();
        var book = Tag("book");
        var member = Tag("member");
        await repository.AppendWithContextAsync(
            [new TaggedEvent(new ContractDecisionAdded(Guid.NewGuid()), [book])]);
        var entity = await repository.GetOrCreateEntityAsync<DecisionProbe>(LoanQuery(book, member));
        await repository.AppendWithContextAsync(
            [new TaggedEvent(new ContractDecisionFiller(Guid.NewGuid()), [Tag("other")])]);

        await repository.HandleCommandAsync(
            entity,
            new EmitProbe(new ContractDecisionNoted(Guid.NewGuid()), [book]));

        Assert.Equal(2, (await store.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(book)))).Events.Count);
    }

    [Fact]
    public async Task CreateEntityAsync_SetsDecisionQuery_EmitAndReplayLeaveIt()
    {
        var (_, repository) = await OpenAsync();
        var query = LoanQuery(Tag("book"), Tag("member"));
        var entity = await repository.CreateEntityAsync<DecisionProbe>(query);
        Assert.Same(query, entity.DecisionQuery);

        entity.ReplayEvents(
        [
            new SequencedEvent(
                new ContractDecisionAdded(Guid.NewGuid()),
                sequencePosition: 3,
                streamId: "probe",
                version: 0,
                metadata: new EventMetadata(),
                tags: ["book"])
        ]);
        entity.Handle(new EmitProbe(new ContractDecisionNoted(Guid.NewGuid()), ["noted"]));

        Assert.Same(query, entity.DecisionQuery);
        Assert.Equal(3, entity.LastSequencePosition);
        Assert.Equal(2, entity.State.Applied);
    }

    [Fact]
    public async Task TenantIsolation_AddsTenantTagToEveryItem_AndToTheWrittenEvent()
    {
        const string tenant = "tenant-a";
        var (store, repository) = await OpenAsync(isolate: true, tenant: tenant);
        var book = Tag("book");
        var caller = Query.FromItems(
            QueryItem.ByTags(book),
            QueryItem.ByType(Borrowed));
        var callerBookTags = caller.Items[0].Tags!.ToArray();

        await repository.AppendWithContextAsync(
            [new TaggedEvent(new ContractDecisionAdded(Guid.NewGuid()), [book, "tenant:other"])]);
        var missed = await repository.GetOrCreateEntityAsync<DecisionProbe>(caller);

        Assert.Equal(0, missed.State.Applied);
        Assert.NotSame(caller, missed.DecisionQuery);
        Assert.All(missed.DecisionQuery!.Items, item => Assert.Contains($"tenant:{tenant}", item.Tags!));
        Assert.Equal(callerBookTags, caller.Items[0].Tags);
        Assert.Null(caller.Items[1].Tags);

        await repository.AppendWithContextAsync(
            [new TaggedEvent(new ContractDecisionAdded(Guid.NewGuid()), [book, $"tenant:{tenant}"])]);
        var seen = await repository.GetOrCreateEntityAsync<DecisionProbe>(caller);
        Assert.Equal(["added"], seen.State.Kinds);

        await repository.HandleCommandAsync(
            seen,
            new EmitProbe(new ContractDecisionNoted(Guid.NewGuid()), [book]));
        var written = Assert.Single(
            (await store.ReadByQueryAsync(Query.FromItems(QueryItem.ByType("backend-contract.decision-noted")))).Events);
        Assert.Contains($"tenant:{tenant}", written.Tags);
        Assert.Contains(book, written.Tags);
    }

    [Fact]
    public async Task Rejects_Null_All_EmptyItem_AndPartitionFilter()
    {
        var (_, repository) = await OpenAsync();

        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            repository.GetStateAsync<DecisionProbeState>(null!));
        await Assert.ThrowsAsync<ArgumentException>(() =>
            repository.GetStateAsync<DecisionProbeState>(Query.All()));
        await Assert.ThrowsAsync<ArgumentException>(() =>
            repository.GetStateAtAsync<DecisionProbeState>(Query.All(), asOfTimestamp: DateTime.UtcNow));
        await Assert.ThrowsAsync<ArgumentException>(() =>
            repository.GetLastSequencePositionAsync(Query.All()));
        await Assert.ThrowsAsync<ArgumentException>(() =>
            repository.CreateEntityAsync<DecisionProbe>(Query.All()));
        await Assert.ThrowsAsync<ArgumentException>(() =>
            repository.GetOrCreateEntityAsync<DecisionProbe>(Query.All()));
        await Assert.ThrowsAsync<ArgumentException>(() =>
            repository.GetOrCreateEntityAsync<DecisionProbe>(Query.FromItems(new QueryItem())));
        await Assert.ThrowsAsync<ArgumentException>(() =>
            repository.GetOrCreateEntityAsync<DecisionProbe>(Query.FromItems(new QueryItem
            {
                Tags = ["book"],
                PartitionFilter = new PartitionFilter(0, 2)
            })));
    }

    [Fact(Timeout = 120_000)]
    public async Task Race_OverlapOnBookItem_ExactlyOneWins()
    {
        var (_, repository) = await OpenAsync();
        for (var round = 0; round < Rounds; round++)
        {
            var book = Tag("book");
            var member = Tag("member");
            await repository.AppendWithContextAsync(
                [new TaggedEvent(new ContractDecisionAdded(Guid.NewGuid()), [book])]);
            await repository.AppendWithContextAsync(
                [new TaggedEvent(new ContractDecisionFiller(Guid.NewGuid()), [Tag("filler")])]);

            var results = await RaceLoadsAsync(
                repository,
                LoanQuery(book, member),
                new EmitProbe(new ContractDecisionBorrowed(Guid.NewGuid()), [book, member]),
                Query.FromItems(QueryItem.ByTags(book)),
                new EmitProbe(new ContractDecisionAdded(Guid.NewGuid()), [book]));

            AssertOneWinner(results);
        }
    }

    [Fact(Timeout = 120_000)]
    public async Task Race_OverlapOnMemberItem_ExactlyOneWins()
    {
        var (_, repository) = await OpenAsync();
        for (var round = 0; round < Rounds; round++)
        {
            var book = Tag("book");
            var otherBook = Tag("book");
            var member = Tag("member");
            await repository.AppendWithContextAsync(
                [new TaggedEvent(new ContractDecisionBorrowed(Guid.NewGuid()), [book, member])]);
            await repository.AppendWithContextAsync(
                [new TaggedEvent(new ContractDecisionFiller(Guid.NewGuid()), [Tag("filler")])]);

            var results = await RaceLoadsAsync(
                repository,
                LoanQuery(book, member),
                new EmitProbe(new ContractDecisionBorrowed(Guid.NewGuid()), [book, member]),
                Query.FromItems(QueryItem.ByTypeAndTags([Borrowed], [member])),
                new EmitProbe(new ContractDecisionBorrowed(Guid.NewGuid()), [otherBook, member]));

            AssertOneWinner(results);
        }
    }

    private async Task<(IEventStore Store, DcbRepository Repository)> OpenAsync(
        bool isolate = false,
        string? tenant = "test-tenant")
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated(eventTypes: ProbeTypes))).Store;
        return (store, Repo(store, isolate, tenant));
    }

    private static DcbRepository Repo(IEventStore store, bool isolate, string? tenant)
        => new(
            store,
            new ContractMetadataProvider(),
            new ContractTenantContextProvider(tenant),
            Options.Create(new LawnDartOptions()),
            tagProvider: null,
            authorizationService: null,
            logger: null,
            eventSourcingOptions: Options.Create(new EventSourcingOptions { EnforceDcbTenantIsolation = isolate }));

    private static Query LoanQuery(string book, string member) => Query.FromItems(
        QueryItem.ByTags(book),
        QueryItem.ByTypeAndTags([Borrowed], [member]));

    private static async Task<Exception?[]> RaceLoadsAsync(
        DcbRepository repository,
        Query leftQuery,
        EmitProbe leftCommand,
        Query rightQuery,
        EmitProbe rightCommand)
    {
        var specs = new[] { (leftQuery, leftCommand), (rightQuery, rightCommand) };
        var loaded = 0;
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var tasks = specs.Select(spec => Task.Run(async () =>
        {
            DecisionProbe? entity = null;
            Exception? loadError = null;
            try
            {
                entity = await repository.GetOrCreateEntityAsync<DecisionProbe>(spec.Item1).ConfigureAwait(false);
                if (entity.LastSequencePosition <= 0)
                    loadError = new InvalidOperationException("The load saw no prior event, so After would be unset.");
            }
            catch (Exception ex)
            {
                loadError = ex;
            }
            finally
            {
                if (Interlocked.Increment(ref loaded) == specs.Length)
                    gate.TrySetResult();
            }

            await gate.Task.WaitAsync(TimeSpan.FromSeconds(30)).ConfigureAwait(false);
            if (loadError is not null)
                return loadError;

            try
            {
                await repository.HandleCommandAsync(entity!, spec.Item2).ConfigureAwait(false);
                return (Exception?)null;
            }
            catch (Exception ex)
            {
                return ex;
            }
        })).ToArray();

        return await Task.WhenAll(tasks).ConfigureAwait(false);
    }

    private static void AssertOneWinner(Exception?[] results)
    {
        Assert.All(results, e => Assert.True(e is null or ConcurrencyException, e?.ToString()));
        Assert.Equal(1, results.Count(e => e is null));
        Assert.Equal(1, results.Count(e => e is ConcurrencyException));
    }

    private static string Tag(string kind) => kind + ":" + Guid.NewGuid().ToString("N");
}

[EventTypeName("backend-contract.decision-added")]
public sealed record ContractDecisionAdded(Guid Id) : IEvent
{
    public DateTime Timestamp { get; init; } = DateTime.UtcNow;
}

[EventTypeName("backend-contract.decision-borrowed")]
public sealed record ContractDecisionBorrowed(Guid Id) : IEvent
{
    public DateTime Timestamp { get; init; } = DateTime.UtcNow;
}

[EventTypeName("backend-contract.decision-noted")]
public sealed record ContractDecisionNoted(Guid Id) : IEvent
{
    public DateTime Timestamp { get; init; } = DateTime.UtcNow;
}

[EventTypeName("backend-contract.decision-filler")]
public sealed record ContractDecisionFiller(Guid Id) : IEvent
{
    public DateTime Timestamp { get; init; } = DateTime.UtcNow;
}

public sealed record EmitProbe(IEvent Event, string[] Tags) : ICommand
{
    public Guid Id { get; } = Guid.NewGuid();
}

public sealed class DecisionProbeState : IState
{
    public int Applied { get; private set; }

    public List<string> Kinds { get; } = [];

    public void Note(string kind)
    {
        Applied++;
        Kinds.Add(kind);
    }
}

public sealed class DecisionProbe : DcbEntity<DecisionProbeState>
{
    public void Handle(EmitProbe command)
        => Emit(command.Event, command.Tags);

    protected override void ApplyEventToState(IEvent @event)
    {
        State.Note(@event switch
        {
            ContractDecisionAdded => "added",
            ContractDecisionBorrowed => "borrowed",
            ContractDecisionNoted => "noted",
            ContractDecisionFiller => "filler",
            _ => "other"
        });
    }
}
