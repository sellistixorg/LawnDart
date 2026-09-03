using System.Text.Json;
using LawnDart;
using LawnDart.Dcb;
using LawnDart.EventStore;

namespace LawnDart.Testing.Bdd;

/// <summary>
/// Entry point for DCB-style Given/When/Then specifications.
/// </summary>
public static class DcbSpec
{
    public static DcbSpecBuilder<TEntity, TCommand> For<TEntity, TCommand>(
        BddTestContext context,
        string[] boundaryTags,
        TCommand command)
        where TEntity : DcbEntity, new()
        where TCommand : ICommand
        => new(context, boundaryTags, command);
}

public sealed class DcbSpecBuilder<TEntity, TCommand>
    where TEntity : DcbEntity, new()
    where TCommand : ICommand
{
    private static readonly JsonSerializerOptions JsonOptions = new() { WriteIndented = false };

    private readonly BddTestContext _context;
    private readonly List<(IEvent Event, string[] Tags)> _givenEvents = [];
    private readonly string[] _boundaryTags;
    private readonly TCommand _command;
    private readonly List<Action<DcbSpecResult<TEntity>>> _assertions = [];

    private Func<IDcbRepository, TEntity, TCommand, Task>? _when;
    private Func<BddProjectionRunner>? _projectionRunnerFactory;
    private Type? _expectedExceptionType;

    internal DcbSpecBuilder(BddTestContext context, string[] boundaryTags, TCommand command)
    {
        _context = context;
        _boundaryTags = boundaryTags;
        _command = command;
    }

    public DcbSpecBuilder<TEntity, TCommand> Given(params (IEvent Event, string[] Tags)[] events)
    {
        _givenEvents.AddRange(events);
        return this;
    }

    public DcbSpecBuilder<TEntity, TCommand> Given(IEvent @event, params string[] tags)
    {
        _givenEvents.Add((@event, tags));
        return this;
    }

    public DcbSpecBuilder<TEntity, TCommand> When(Func<IDcbRepository, TEntity, TCommand, Task> handler)
    {
        _when = handler ?? throw new ArgumentNullException(nameof(handler));
        return this;
    }

    public DcbSpecBuilder<TEntity, TCommand> ThenThrows<TException>()
        where TException : Exception
    {
        _expectedExceptionType = typeof(TException);
        return this;
    }

    public DcbSpecBuilder<TEntity, TCommand> ThenEmittedTypes(params Type[] expectedTypes)
    {
        _assertions.Add(result =>
        {
            var actualTypes = result.EmittedEvents.Select(e => e.Event.GetType()).ToArray();
            if (!actualTypes.SequenceEqual(expectedTypes))
            {
                throw new InvalidOperationException(
                    $"Emitted event types mismatch.{Environment.NewLine}" +
                    $"Expected: [{string.Join(", ", expectedTypes.Select(t => t.Name))}]{Environment.NewLine}" +
                    $"Actual:   [{string.Join(", ", actualTypes.Select(t => t.Name))}]{Environment.NewLine}" +
                    $"Emitted:  {string.Join("; ", result.EmittedEvents.Select(e => e.ToString()))}");
            }
        });
        return this;
    }

    /// <summary>
    /// Asserts the exact emitted event payloads in order.
    /// Use <see cref="ThenEmittedEvent{TEvent}"/> for partial/predicate-based checks.
    /// </summary>
    public DcbSpecBuilder<TEntity, TCommand> ThenEmittedEvents(params IEvent[] expectedEvents)
    {
        _assertions.Add(result =>
        {
            if (result.EmittedEvents.Count != expectedEvents.Length)
            {
                throw new InvalidOperationException(
                    $"Emitted event count mismatch.{Environment.NewLine}" +
                    $"Expected: {expectedEvents.Length}{Environment.NewLine}" +
                    $"Actual:   {result.EmittedEvents.Count}{Environment.NewLine}" +
                    $"Emitted:  {string.Join("; ", result.EmittedEvents.Select(e => e.ToString()))}");
            }

            for (var i = 0; i < expectedEvents.Length; i++)
            {
                var expected = expectedEvents[i];
                var actual = result.EmittedEvents[i].Event;

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
                        $"Actual:   {actualJson}{Environment.NewLine}" +
                        $"Tags:     [{string.Join(", ", result.EmittedEvents[i].Tags)}]");
                }
            }
        });
        return this;
    }

    /// <summary>
    /// Asserts that exactly one emitted event of <typeparamref name="TEvent"/> matches <paramref name="predicate"/>.
    /// Use this for business-field assertions while ignoring volatile IDs/timestamps.
    /// </summary>
    public DcbSpecBuilder<TEntity, TCommand> ThenEmittedEvent<TEvent>(
        Func<TEvent, bool> predicate,
        string? description = null)
        where TEvent : class, IEvent
    {
        ArgumentNullException.ThrowIfNull(predicate);

        _assertions.Add(result =>
        {
            var matches = result.EmittedEvents
                .Where(e => e.Event is TEvent typed && predicate(typed))
                .ToArray();

            if (matches.Length != 1)
            {
                var reason = string.IsNullOrWhiteSpace(description)
                    ? "Predicate did not uniquely match a single emitted event."
                    : description!;
                throw new InvalidOperationException(
                    $"Expected exactly one matching {typeof(TEvent).Name} event. {reason}{Environment.NewLine}" +
                    $"Matches: {matches.Length}{Environment.NewLine}" +
                    $"Emitted: {string.Join("; ", result.EmittedEvents.Select(e => e.ToString()))}");
            }
        });
        return this;
    }

    public DcbSpecBuilder<TEntity, TCommand> AndAllEmittedEventsContainTags(params string[] requiredTags)
    {
        _assertions.Add(result =>
        {
            foreach (var emitted in result.EmittedEvents)
            {
                var missing = requiredTags.Where(t => !emitted.Tags.Contains(t, StringComparer.Ordinal)).ToArray();
                if (missing.Length > 0)
                {
                    throw new InvalidOperationException(
                        $"Emitted event {emitted.Event.GetType().Name} missing required tags: {string.Join(", ", missing)}. " +
                        $"Actual tags: [{string.Join(", ", emitted.Tags)}]");
                }
            }
        });
        return this;
    }

    public DcbSpecBuilder<TEntity, TCommand> AndNoConflict()
    {
        _assertions.Add(result =>
        {
            if (result.Exception is ConcurrencyException)
            {
                throw new InvalidOperationException(
                    $"Expected no conflict but received concurrency exception: {result.Exception.Message}");
            }
        });
        return this;
    }

    public DcbSpecBuilder<TEntity, TCommand> AndView(Action<BddProjectionRunner> configure)
    {
        _projectionRunnerFactory = () =>
        {
            var runner = new BddProjectionRunner();
            configure(runner);
            return runner;
        };
        return this;
    }

    public DcbSpecBuilder<TEntity, TCommand> AndAssert(Action<DcbSpecResult<TEntity>> assertion)
    {
        _assertions.Add(assertion);
        return this;
    }

    public async Task<DcbSpecResult<TEntity>> RunAsync(CancellationToken cancellationToken = default)
    {
        foreach (var item in _givenEvents)
        {
            var seedStream = $"bdd-seed:{Guid.NewGuid():N}";
            await _context.EventStore.AppendAsync(
                    seedStream,
                    [item.Event],
                    tags: item.Tags,
                    cancellationToken: cancellationToken)
                .ConfigureAwait(false);
        }

        // Capture a checkpoint after Given() so emitted events only include When() output.
        var afterGiven = await _context.EventStore.ReadByQueryAsync(Query.All(), cancellationToken: cancellationToken)
            .ConfigureAwait(false);
        var beforeWhenSequence = afterGiven.Events.LastOrDefault()?.SequencePosition ?? 0;

        var entity = await _context.DcbRepository.GetOrCreateEntityAsync<TEntity>(_boundaryTags, cancellationToken)
            .ConfigureAwait(false);

        Exception? thrown = null;
        try
        {
            if (_when is null)
            {
                await _context.DcbRepository.HandleCommandAsync(entity, _command, cancellationToken: cancellationToken)
                    .ConfigureAwait(false);
            }
            else
            {
                await _when(_context.DcbRepository, entity, _command).ConfigureAwait(false);
            }
        }
        catch (Exception ex)
        {
            thrown = ex;
        }

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

        var emitted = await _context.EventStore.ReadByQueryAsync(
                Query.All(),
                fromSequencePosition: beforeWhenSequence + 1,
                cancellationToken: cancellationToken)
            .ConfigureAwait(false);

        if (_projectionRunnerFactory is not null)
        {
            var runner = _projectionRunnerFactory();
            await runner.ProjectQuerySinceAsync(
                    _context.EventStore,
                    Query.All(),
                    beforeWhenSequence,
                    cancellationToken)
                .ConfigureAwait(false);
        }

        var taggedEvents = emitted.Events
            .Select(e => new DcbEmittedEvent(e.Event, e.Tags))
            .ToArray();

        var result = new DcbSpecResult<TEntity>(
            entity,
            taggedEvents,
            thrown,
            beforeWhenSequence,
            entity.LastSequencePosition,
            entity.ConsistencyMarker);

        foreach (var assertion in _assertions)
        {
            assertion(result);
        }

        return result;
    }

    public static string Serialize(IEvent @event) => JsonSerializer.Serialize(@event, JsonOptions);
}

public sealed record DcbEmittedEvent(IEvent Event, IReadOnlyList<string> Tags)
{
    public override string ToString()
        => $"{Event.GetType().Name} tags=[{string.Join(", ", Tags)}]";
}

public sealed record DcbSpecResult<TEntity>(
    TEntity Entity,
    IReadOnlyList<DcbEmittedEvent> EmittedEvents,
    Exception? Exception,
    long BeforeSequencePosition,
    long LastEntitySequencePosition,
    byte[]? ConsistencyMarker)
    where TEntity : DcbEntity;
