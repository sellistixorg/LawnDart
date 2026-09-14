using LawnDart;
using LawnDart.Aggregates;
using LawnDart.EventStore;
using LawnDart.Metadata;
using LawnDart.Testing.Bdd;

namespace LawnDart.Testing.Tests.Bdd;

public sealed class AggregateSpecAssertionTests
{
    [Fact]
    public async Task spec_internal_failure_throws_bdd_spec_assertion_exception()
    {
        await using var ctx = BddTestContext.CreateInMemory();
        var id = Guid.NewGuid();

        var ex = await Assert.ThrowsAsync<BddSpecAssertionException>(() =>
            AggregateSpec
                .For<Book>(ctx, id)
                .When(new RegisterBook(Guid.NewGuid(), id))
                .ThenEmittedTypes(typeof(BookLoaned))
                .RunAsync());

        Assert.IsNotType<InvalidOperationException>(ex);
        Assert.False(typeof(InvalidOperationException).IsAssignableFrom(ex.GetType()));
    }

    [Fact]
    public async Task wrapping_run_async_in_invalid_operation_throws_fails_when_domain_did_not_throw()
    {
        await using var ctx = BddTestContext.CreateInMemory();
        var id = Guid.NewGuid();

        await Assert.ThrowsAsync<Xunit.Sdk.ThrowsException>(() =>
            Assert.ThrowsAsync<InvalidOperationException>(() =>
                AggregateSpec
                    .For<Book>(ctx, id)
                    .When(new RegisterBook(Guid.NewGuid(), id))
                    .ThenEmittedTypes(typeof(BookLoaned))
                    .RunAsync()));
    }

    [Fact]
    public async Task then_throws_matches_derived_domain_exception()
    {
        await using var ctx = BddTestContext.CreateInMemory();
        var id = Guid.NewGuid();

        var result = await AggregateSpec
            .For<Book>(ctx, id)
            .When(new LoanBook(Guid.NewGuid(), id))
            .ThenThrows<InvalidOperationException>()
            .RunAsync();

        Assert.IsType<DomainException>(result.Exception);
    }

    [Fact]
    public async Task then_throws_wrong_type_names_expected_and_actual()
    {
        await using var ctx = BddTestContext.CreateInMemory();
        var id = Guid.NewGuid();

        var ex = await Assert.ThrowsAsync<BddSpecAssertionException>(() =>
            AggregateSpec
                .For<Book>(ctx, id)
                .When(new LoanBook(Guid.NewGuid(), id))
                .ThenThrows<IOException>()
                .RunAsync());

        Assert.Contains(nameof(IOException), ex.Message, StringComparison.Ordinal);
        Assert.Contains(nameof(DomainException), ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task missing_sequence_capability_keeps_minus_one_fallback()
    {
        var store = new ProbeEventStore { SequenceException = new NotSupportedException("no global sequence") };
        await using var ctx = BddTestContext.Create(store);
        var id = Guid.NewGuid();

        var result = await AggregateSpec
            .For<Book>(ctx, id)
            .When(new RegisterBook(Guid.NewGuid(), id))
            .ThenEmittedEvent<BookRegistered>(e => e.BookId == id)
            .ThenAppendedOnlyTo($"Book:{id}")
            .RunAsync();

        Assert.Single(result.EmittedEvents);
    }

    [Fact]
    public async Task missing_query_capability_keeps_empty_set_fallback()
    {
        var store = new ProbeEventStore { QueryException = new NotSupportedException("no query") };
        await using var ctx = BddTestContext.Create(store);
        var id = Guid.NewGuid();

        var result = await AggregateSpec
            .For<Book>(ctx, id)
            .When(new RegisterBook(Guid.NewGuid(), id))
            .ThenEmittedEvent<BookRegistered>(e => e.BookId == id)
            .RunAsync();

        Assert.Empty(result.AppendedStreamIds);

        var assertEx = await Assert.ThrowsAsync<BddSpecAssertionException>(() =>
            AggregateSpec
                .For<Book>(ctx, Guid.NewGuid())
                .When(new RegisterBook(Guid.NewGuid(), Guid.NewGuid()))
                .ThenAppendedOnlyTo("Book:unused")
                .RunAsync());

        Assert.Contains("Expected appends to exactly one stream", assertEx.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("Unexpected failure probing", assertEx.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task unexpected_sequence_probe_failure_surfaces()
    {
        var store = new ProbeEventStore { SequenceException = new IOException("store down") };
        await using var ctx = BddTestContext.Create(store);

        var ex = await Assert.ThrowsAsync<BddSpecAssertionException>(() =>
            AggregateSpec
                .For<Book>(ctx, Guid.NewGuid())
                .When(new RegisterBook(Guid.NewGuid(), Guid.NewGuid()))
                .RunAsync());

        Assert.Contains(nameof(IEventStore.GetCurrentSequenceAsync), ex.Message, StringComparison.Ordinal);
        Assert.IsType<IOException>(ex.InnerException);
    }

    [Fact]
    public async Task unexpected_query_probe_failure_surfaces()
    {
        var store = new ProbeEventStore { QueryException = new IOException("query pipe broken") };
        await using var ctx = BddTestContext.Create(store);

        var ex = await Assert.ThrowsAsync<BddSpecAssertionException>(() =>
            AggregateSpec
                .For<Book>(ctx, Guid.NewGuid())
                .When(new RegisterBook(Guid.NewGuid(), Guid.NewGuid()))
                .RunAsync());

        Assert.Contains(nameof(IEventStore.ReadByQueryAsync), ex.Message, StringComparison.Ordinal);
        Assert.IsType<IOException>(ex.InnerException);
    }

    public sealed class BookState : IState
    {
        public bool Exists { get; set; }
    }

    public sealed class Book : AggregateRoot<BookState>
    {
        public void Handle(RegisterBook command) =>
            Apply(new BookRegistered(Guid.NewGuid(), DateTime.UtcNow, command.BookId));

        public void Handle(LoanBook _) =>
            throw new DomainException("Book does not exist.");

        protected override void ApplyEventToState(IEvent @event)
        {
            if (@event is BookRegistered)
                State.Exists = true;
        }
    }

    public sealed record RegisterBook(Guid Id, Guid BookId) : ICommand;

    public sealed record LoanBook(Guid Id, Guid BookId) : ICommand;

    [EventTypeName("aggregate-spec-assertion-tests.book-registered")]
    public sealed record BookRegistered(Guid Id, DateTime Timestamp, Guid BookId) : IEvent;

    [EventTypeName("aggregate-spec-assertion-tests.book-loaned")]
    public sealed record BookLoaned(Guid Id, DateTime Timestamp, Guid BookId) : IEvent;

    /// <summary>
    /// Delegates to <see cref="EventSourcing.EventStore.InMemoryEventStore"/> except
    /// for the two capability probes <see cref="AggregateSpec"/> uses.
    /// </summary>
    private sealed class ProbeEventStore : IEventStore
    {
        private readonly EventSourcing.EventStore.InMemoryEventStore _inner = new();

        public Exception? SequenceException { get; init; }
        public Exception? QueryException { get; init; }

        public Task<IReadOnlyList<SequencedEvent>> ReadStreamAsync(
            string streamId,
            long fromVersion = 0,
            long? toVersion = null,
            DateTime? toTimestamp = null,
            CancellationToken cancellationToken = default)
            => _inner.ReadStreamAsync(streamId, fromVersion, toVersion, toTimestamp, cancellationToken);

        public IAsyncEnumerable<SequencedEvent> ReadStreamEnumerableAsync(
            string streamId,
            long fromVersion = 0,
            long? toVersion = null,
            DateTime? toTimestamp = null,
            CancellationToken cancellationToken = default)
            => _inner.ReadStreamEnumerableAsync(streamId, fromVersion, toVersion, toTimestamp, cancellationToken);

        public Task<QueryResult> ReadByQueryAsync(
            Query query,
            long? fromSequencePosition = null,
            int? limit = null,
            long? toSequencePosition = null,
            DateTime? toTimestamp = null,
            CancellationToken cancellationToken = default)
        {
            if (QueryException is not null)
                throw QueryException;
            return _inner.ReadByQueryAsync(query, fromSequencePosition, limit, toSequencePosition, toTimestamp, cancellationToken);
        }

        public IAsyncEnumerable<SequencedEvent> ReadByQueryStreamAsync(
            Query query,
            long? fromSequencePosition = null,
            long? toSequencePosition = null,
            DateTime? toTimestamp = null,
            CancellationToken cancellationToken = default)
            => _inner.ReadByQueryStreamAsync(query, fromSequencePosition, toSequencePosition, toTimestamp, cancellationToken);

        public Task<AppendResult> AppendAsync(
            string streamId,
            IEnumerable<IEvent> events,
            long? expectedVersion = null,
            EventMetadata? metadata = null,
            IEnumerable<string>? tags = null,
            CancellationToken cancellationToken = default)
            => _inner.AppendAsync(streamId, events, expectedVersion, metadata, tags, cancellationToken);

        public Task<AppendResult> AppendAsync(
            IEnumerable<IEvent> events,
            AppendCondition condition,
            EventMetadata? metadata = null,
            IEnumerable<string>? tags = null,
            CancellationToken cancellationToken = default)
            => _inner.AppendAsync(events, condition, metadata, tags, cancellationToken);

        public Task<long> GetCurrentSequenceAsync(CancellationToken cancellationToken = default)
        {
            if (SequenceException is not null)
                throw SequenceException;
            return _inner.GetCurrentSequenceAsync(cancellationToken);
        }

        public Task<long> GetMaxSequencePositionAsync(
            Query query,
            long? fromSequencePosition = null,
            long? toSequencePosition = null,
            DateTime? toTimestamp = null,
            CancellationToken cancellationToken = default)
            => _inner.GetMaxSequencePositionAsync(query, fromSequencePosition, toSequencePosition, toTimestamp, cancellationToken);

        public Task<StreamMetadata?> GetStreamAsync(string streamId, CancellationToken cancellationToken = default)
            => _inner.GetStreamAsync(streamId, cancellationToken);

        public Task<IReadOnlyList<StreamMetadata>> GetStreamsByAggregateTypeAsync(
            string aggregateType,
            CancellationToken cancellationToken = default)
            => _inner.GetStreamsByAggregateTypeAsync(aggregateType, cancellationToken);

        public Task<IReadOnlyList<StreamMetadata>> GetStreamsByTagAsync(
            string tag,
            CancellationToken cancellationToken = default)
            => _inner.GetStreamsByTagAsync(tag, cancellationToken);

        public Task<IReadOnlyList<string>> EnumerateStreamIdsAsync(
            string? prefix = null,
            CancellationToken cancellationToken = default)
            => _inner.EnumerateStreamIdsAsync(prefix, cancellationToken);

        public Task<IReadOnlyList<StreamMetadata>> GetStreamsUpdatedAfterAsync(
            long afterSequencePosition,
            int? limit = null,
            CancellationToken cancellationToken = default)
            => _inner.GetStreamsUpdatedAfterAsync(afterSequencePosition, limit, cancellationToken);

        public Task<long> GetStreamCountAsync(string? prefix = null, CancellationToken cancellationToken = default)
            => _inner.GetStreamCountAsync(prefix, cancellationToken);
    }
}
