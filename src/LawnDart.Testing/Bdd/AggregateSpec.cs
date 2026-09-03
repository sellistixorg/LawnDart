using System.Text.Json;
using LawnDart;
using LawnDart.Aggregates;
using LawnDart.EventStore;

namespace LawnDart.Testing.Bdd;

/// <summary>
/// Entry point for aggregate-style Given/When/Then specifications.
/// </summary>
public static class AggregateSpec
{
    public static AggregateSpecBuilder<TAggregate> For<TAggregate>(
        BddTestContext context,
        Guid aggregateId)
        where TAggregate : AggregateRoot, new()
        => new(context, aggregateId);
}

public sealed class AggregateSpecBuilder<TAggregate>
    where TAggregate : AggregateRoot, new()
{
    private static readonly JsonSerializerOptions JsonOptions = new() { WriteIndented = false };

    private readonly BddTestContext _context;
    private readonly Guid _aggregateId;
    private readonly List<IEvent> _givenEvents = [];
    private readonly List<Action<AggregateSpecResult<TAggregate>>> _assertions = [];

    private Func<IAggregateRepository, TAggregate, Task>? _when;
    private Type? _expectedExceptionType;
    private long? _expectedVersion;
    private Func<BddProjectionRunner>? _projectionRunnerFactory;

    internal AggregateSpecBuilder(BddTestContext context, Guid aggregateId)
    {
        _context = context;
        _aggregateId = aggregateId;
    }

    public AggregateSpecBuilder<TAggregate> Given(params IEvent[] pastEvents)
    {
        _givenEvents.AddRange(pastEvents);
        return this;
    }

    public AggregateSpecBuilder<TAggregate> When<TCommand>(TCommand command)
        where TCommand : ICommand
    {
        _when = (repo, aggregate) => repo.HandleCommandAsync(aggregate, command);
        return this;
    }

    public AggregateSpecBuilder<TAggregate> When(Func<IAggregateRepository, TAggregate, Task> handler)
    {
        _when = handler ?? throw new ArgumentNullException(nameof(handler));
        return this;
    }

    public AggregateSpecBuilder<TAggregate> ThenThrows<TException>()
        where TException : Exception
    {
        _expectedExceptionType = typeof(TException);
        return this;
    }

    public AggregateSpecBuilder<TAggregate> ThenEmittedTypes(params Type[] expectedTypes)
    {
        _assertions.Add(result =>
        {
            var actualTypes = result.EmittedEvents.Select(e => e.GetType()).ToArray();
            if (!actualTypes.SequenceEqual(expectedTypes))
            {
                throw new InvalidOperationException(
                    $"Emitted event types mismatch.{Environment.NewLine}" +
                    $"Expected: [{string.Join(", ", expectedTypes.Select(t => t.Name))}]{Environment.NewLine}" +
                    $"Actual:   [{string.Join(", ", actualTypes.Select(t => t.Name))}]");
            }
        });
        return this;
    }

    public AggregateSpecBuilder<TAggregate> ThenEmitted(params IEvent[] expectedEvents)
    {
        _assertions.Add(result =>
        {
            if (result.EmittedEvents.Count != expectedEvents.Length)
            {
                throw new InvalidOperationException(
                    $"Emitted event count mismatch.{Environment.NewLine}" +
                    $"Expected: {expectedEvents.Length}{Environment.NewLine}" +
                    $"Actual:   {result.EmittedEvents.Count}{Environment.NewLine}" +
                    $"Actual payload: {Serialize(result.EmittedEvents)}");
            }

            for (var i = 0; i < expectedEvents.Length; i++)
            {
                var expected = expectedEvents[i];
                var actual = result.EmittedEvents[i];
                if (expected.GetType() != actual.GetType())
                {
                    throw new InvalidOperationException(
                        $"Event type mismatch at index {i}.{Environment.NewLine}" +
                        $"Expected: {expected.GetType().Name}{Environment.NewLine}" +
                        $"Actual:   {actual.GetType().Name}");
                }

                var expectedJson = JsonSerializer.Serialize(expected, JsonOptions);
                var actualJson = JsonSerializer.Serialize(actual, JsonOptions);
                if (!string.Equals(expectedJson, actualJson, StringComparison.Ordinal))
                {
                    throw new InvalidOperationException(
                        $"Event payload mismatch at index {i} ({actual.GetType().Name}).{Environment.NewLine}" +
                        $"Expected: {expectedJson}{Environment.NewLine}" +
                        $"Actual:   {actualJson}");
                }
            }
        });
        return this;
    }

    /// <summary>
    /// Alias for <see cref="ThenEmitted"/> to make intent explicit.
    /// </summary>
    public AggregateSpecBuilder<TAggregate> ThenEmittedEvents(params IEvent[] expectedEvents)
        => ThenEmitted(expectedEvents);

    /// <summary>
    /// Asserts that exactly one emitted event of <typeparamref name="TEvent"/> matches <paramref name="predicate"/>.
    /// Useful for asserting business fields while ignoring volatile IDs/timestamps.
    /// </summary>
    public AggregateSpecBuilder<TAggregate> ThenEmittedEvent<TEvent>(
        Func<TEvent, bool> predicate,
        string? description = null)
        where TEvent : class, IEvent
    {
        ArgumentNullException.ThrowIfNull(predicate);

        _assertions.Add(result =>
        {
            var matches = result.EmittedEvents
                .Where(e => e is TEvent typed && predicate(typed))
                .ToArray();

            if (matches.Length != 1)
            {
                var reason = string.IsNullOrWhiteSpace(description)
                    ? "Predicate did not uniquely match a single emitted event."
                    : description!;
                throw new InvalidOperationException(
                    $"Expected exactly one matching {typeof(TEvent).Name} event. {reason}{Environment.NewLine}" +
                    $"Matches: {matches.Length}{Environment.NewLine}" +
                    $"Actual:  {Serialize(result.EmittedEvents)}");
            }
        });
        return this;
    }

    /// <summary>
    /// Asserts that during <c>When</c> exactly one stream was written to, and it is
    /// <paramref name="expectedStreamId"/>.
    /// </summary>
    public AggregateSpecBuilder<TAggregate> ThenAppendedOnlyTo(string expectedStreamId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(expectedStreamId);

        _assertions.Add(result =>
        {
            var actual = result.AppendedStreamIds;
            if (actual.Count != 1 || !actual.Contains(expectedStreamId))
            {
                throw new InvalidOperationException(
                    $"Expected appends to exactly one stream: '{expectedStreamId}'.{Environment.NewLine}" +
                    $"Actual streams written ({actual.Count}): " +
                    $"[{string.Join(", ", actual.OrderBy(s => s, StringComparer.Ordinal))}]");
            }
        });
        return this;
    }

    /// <summary>
    /// Asserts that during <c>When</c> exactly the streams in <paramref name="expectedStreamIds"/>
    /// were written to (exact set, order-insensitive).
    /// </summary>
    public AggregateSpecBuilder<TAggregate> ThenAppendedTo(params string[] expectedStreamIds)
    {
        if (expectedStreamIds is null || expectedStreamIds.Length == 0)
            throw new ArgumentException("At least one expected stream ID is required.", nameof(expectedStreamIds));

        _assertions.Add(result =>
        {
            var expected = new HashSet<string>(expectedStreamIds, StringComparer.Ordinal);
            var actual   = result.AppendedStreamIds;
            if (!expected.SetEquals(actual))
            {
                throw new InvalidOperationException(
                    $"Expected appends to streams: [{string.Join(", ", expected.OrderBy(s => s, StringComparer.Ordinal))}]{Environment.NewLine}" +
                    $"Actual streams written:      [{string.Join(", ", actual.OrderBy(s => s, StringComparer.Ordinal))}]");
            }
        });
        return this;
    }

    public AggregateSpecBuilder<TAggregate> AndExpectedVersion(long expectedVersion)
    {
        _expectedVersion = expectedVersion;
        return this;
    }

    public AggregateSpecBuilder<TAggregate> AndView(Action<BddProjectionRunner> configure)
    {
        _projectionRunnerFactory = () =>
        {
            var runner = new BddProjectionRunner();
            configure(runner);
            return runner;
        };
        return this;
    }

    public AggregateSpecBuilder<TAggregate> AndAssert(Action<AggregateSpecResult<TAggregate>> assertion)
    {
        _assertions.Add(assertion);
        return this;
    }

    public async Task<AggregateSpecResult<TAggregate>> RunAsync(CancellationToken cancellationToken = default)
    {
        if (_when is null)
        {
            throw new InvalidOperationException("When(...) must be configured before RunAsync().");
        }

        var streamId = $"{typeof(TAggregate).Name}:{_aggregateId}";
        if (_givenEvents.Count > 0)
        {
            await _context.EventStore.AppendAsync(streamId, _givenEvents, cancellationToken: cancellationToken)
                .ConfigureAwait(false);
        }

        var aggregate = await _context.AggregateRepository.GetOrCreateAsync<TAggregate>(_aggregateId, cancellationToken)
            .ConfigureAwait(false);
        var deltaReadFromVersion = aggregate.CommittedVersion >= 0 ? aggregate.CommittedVersion + 1 : 0;

        // Capture the current global sequence before When so we can detect all stream writes.
        long baselineSeq;
        try { baselineSeq = await _context.EventStore.GetCurrentSequenceAsync(cancellationToken).ConfigureAwait(false); }
        catch { baselineSeq = -1; }

        Exception? thrown = null;
        try
        {
            await _when(_context.AggregateRepository, aggregate).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            thrown = ex;
        }

        // Collect all stream IDs that had events appended during When.
        IReadOnlySet<string> appendedStreamIds;
        try
        {
            var allNew = await _context.EventStore.ReadByQueryAsync(
                Query.All(),
                fromSequencePosition: baselineSeq + 1,
                cancellationToken: cancellationToken).ConfigureAwait(false);
            appendedStreamIds = allNew.Events
                .Select(e => e.StreamId)
                .ToHashSet(StringComparer.Ordinal);
        }
        catch { appendedStreamIds = new HashSet<string>(); }

        if (_expectedExceptionType is not null)
        {
            if (thrown is null || thrown.GetType() != _expectedExceptionType)
            {
                throw new InvalidOperationException(
                    $"Expected exception {_expectedExceptionType.Name} but got {(thrown?.GetType().Name ?? "none")}.");
            }
        }
        else if (thrown is not null)
        {
            throw new InvalidOperationException($"Unexpected exception: {thrown}");
        }

        var emitted = await _context.EventStore.ReadStreamAsync(
                streamId,
                fromVersion: deltaReadFromVersion,
                cancellationToken: cancellationToken)
            .ConfigureAwait(false);

        var finalAggregate = await _context.AggregateRepository.GetOrCreateAsync<TAggregate>(_aggregateId, cancellationToken)
            .ConfigureAwait(false);
        if (_expectedVersion.HasValue && finalAggregate.Version != _expectedVersion.Value)
        {
            throw new InvalidOperationException(
                $"Expected version {_expectedVersion.Value} but was {finalAggregate.Version}.");
        }

        if (_projectionRunnerFactory is not null)
        {
            var runner = _projectionRunnerFactory();
            await runner.ProjectStreamAsync(_context.EventStore, streamId, cancellationToken).ConfigureAwait(false);
        }

        var result = new AggregateSpecResult<TAggregate>(
            streamId,
            finalAggregate,
            emitted.Select(e => e.Event).ToArray(),
            emitted.ToArray(),
            thrown,
            appendedStreamIds);

        foreach (var assertion in _assertions)
        {
            assertion(result);
        }

        return result;
    }

    private static string Serialize(IReadOnlyList<IEvent> events)
        => JsonSerializer.Serialize(events, JsonOptions);
}

public sealed record AggregateSpecResult<TAggregate>(
    string StreamId,
    TAggregate Aggregate,
    IReadOnlyList<IEvent> EmittedEvents,
    IReadOnlyList<SequencedEvent> EmittedSequencedEvents,
    Exception? Exception,
    IReadOnlySet<string> AppendedStreamIds)
    where TAggregate : AggregateRoot;
