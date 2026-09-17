using System.Text.Json;
using LawnDart;
using LawnDart.EventStore;
using LawnDart.Serialization;
using Microsoft.Extensions.DependencyInjection;

namespace LawnDart.Tests.EventStore;

public class EventTypeCatalogTests
{
    [Fact]
    public void Materialize_SingleLegacyAttribute_IsVersion1AndCurrent()
    {
        var catalog = EventTypeCatalog.Materialize([typeof(LegacyRegistered)]);

        Assert.Equal("author-registered", catalog.GetName(typeof(LegacyRegistered)));
        Assert.True(catalog.TryResolveType("author-registered", out var current));
        Assert.Equal(typeof(LegacyRegistered), current);
        Assert.True(catalog.TryResolveType("author-registered", 1, out var v1));
        Assert.Equal(typeof(LegacyRegistered), v1);
        Assert.False(catalog.TryResolveType("author-registered", 2, out _));
    }

    [Fact]
    public void Materialize_GetName_ReturnsFamilyToken_NotVersionSuffix()
    {
        var catalog = EventTypeCatalog.Materialize(
            [typeof(AuthorRegisteredV1), typeof(AuthorRegistered)]);

        Assert.Equal("author-registered", catalog.GetName(typeof(AuthorRegistered)));
        Assert.Equal("author-registered", catalog.GetName(typeof(AuthorRegisteredV1)));
        Assert.DoesNotContain(".v2", catalog.GetName(typeof(AuthorRegistered)));
        Assert.DoesNotContain("v2", catalog.GetName(typeof(AuthorRegistered)));
    }

    [Fact]
    public void Materialize_ExplicitCurrent_ResolvesEachVersion()
    {
        var catalog = EventTypeCatalog.Materialize(
            [typeof(AuthorRegisteredV1), typeof(AuthorRegistered)]);

        Assert.True(catalog.TryResolveType("author-registered", 1, out var v1));
        Assert.Equal(typeof(AuthorRegisteredV1), v1);
        Assert.True(catalog.TryResolveType("author-registered", 2, out var v2));
        Assert.Equal(typeof(AuthorRegistered), v2);
        Assert.True(catalog.TryResolveType("author-registered", out var current));
        Assert.Equal(typeof(AuthorRegistered), current);
    }

    [Fact]
    public void Materialize_DoesNotInferHighestVersionAsCurrent()
    {
        var catalog = EventTypeCatalog.Materialize(
            [typeof(KeptCurrentV1), typeof(NotCurrentV2)]);

        Assert.True(catalog.TryResolveType("kept-current", out var current));
        Assert.Equal(typeof(KeptCurrentV1), current);
        Assert.True(catalog.TryResolveType("kept-current", 2, out var v2));
        Assert.Equal(typeof(NotCurrentV2), v2);
    }

    [Fact]
    public void Materialize_TwoCurrents_Throws()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            EventTypeCatalog.Materialize([typeof(TwoCurrentV1), typeof(TwoCurrentV2)]));
        Assert.Contains("author-two-current", ex.Message);
    }

    [Fact]
    public void Materialize_MultiTypeFamilyWithoutCurrent_Throws()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            EventTypeCatalog.Materialize([typeof(NoCurrentV1), typeof(NoCurrentV2)]));
        Assert.Contains("author-no-current", ex.Message);
        Assert.Contains("current: true", ex.Message);
    }

    [Fact]
    public void Materialize_DuplicateTokenAndVersion_Throws()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            EventTypeCatalog.Materialize([typeof(DupA), typeof(DupB)]));
        Assert.Contains("dup-token", ex.Message);
    }

    [Fact]
    public void Materialize_ResolvesFullNameSimpleNameAndAssemblyQualifiedNameAliases()
    {
        var catalog = EventTypeCatalog.Materialize([typeof(LegacyRegistered)]);

        Assert.True(catalog.TryResolveType(typeof(LegacyRegistered).FullName!, 1, out var byFullName));
        Assert.Equal(typeof(LegacyRegistered), byFullName);
        Assert.True(catalog.TryResolveType(nameof(LegacyRegistered), 1, out var byName));
        Assert.Equal(typeof(LegacyRegistered), byName);
        Assert.True(catalog.TryResolveType(typeof(LegacyRegistered).AssemblyQualifiedName!, 1, out var byAqn));
        Assert.Equal(typeof(LegacyRegistered), byAqn);
    }

    [Fact]
    public void Materialize_TwoCatalogs_DoNotShareMaps()
    {
        var authors = EventTypeCatalog.Materialize([typeof(LegacyRegistered)]);
        var books = EventTypeCatalog.Materialize([typeof(BookRegistered)]);

        Assert.True(authors.TryResolveType("author-registered", 1, out _));
        Assert.False(authors.TryResolveType("book-registered", 1, out _));
        Assert.True(books.TryResolveType("book-registered", 1, out _));
        Assert.False(books.TryResolveType("author-registered", 1, out _));

        EventTypeNameResolver.Warmup([typeof(BookRegistered)]);
        Assert.True(EventTypeNameResolver.TryResolveType("book-registered", 1, out _));
        Assert.False(authors.TryResolveType("book-registered", 1, out _));
    }

    [Fact]
    public void WithEventTypes_TwoContexts_DoNotShareCatalog()
    {
        var services = new ServiceCollection();
        services.AddBoundedContext("authors").WithEventTypes(typeof(LegacyRegistered));
        services.AddBoundedContext("books").WithEventTypes(typeof(BookRegistered));

        using var sp = services.BuildServiceProvider();
        var authors = sp.GetRequiredKeyedService<IEventTypeCatalog>("authors");
        var books = sp.GetRequiredKeyedService<IEventTypeCatalog>("books");

        Assert.NotSame(authors, books);
        Assert.NotSame(authors, EventTypeCatalog.Shared);
        Assert.True(authors.TryResolveType("author-registered", 1, out var authorType));
        Assert.Equal(typeof(LegacyRegistered), authorType);
        Assert.False(authors.TryResolveType("book-registered", 1, out _));
        Assert.False(books.TryResolveType("author-registered", 1, out _));

        EventTypeNameResolver.Warmup([typeof(BookRegistered)]);
        Assert.False(authors.TryResolveType("book-registered", 1, out _));
    }

    [Fact]
    public void Hydrate_KnownFamilyNewerThanProcess_FailsClosed()
    {
        var catalog = EventTypeCatalog.Materialize([typeof(LegacyRegistered)]);
        var session = new EventSession(new BytesEventSerializer(), catalog);
        var recorded = new RecordedEvent(
            "author-registered",
            ReadOnlyMemory<byte>.Empty,
            "authors-1",
            streamVersion: 1,
            sequencePosition: 1,
            commitTimestamp: DateTime.UtcNow,
            schemaVersion: 2);

        var ex = Assert.Throws<EventSchemaTooNewException>(() => session.Hydrate(recorded));
        Assert.Equal("author-registered", ex.FamilyToken);
        Assert.Equal(2, ex.SchemaVersion);
        Assert.Equal(1, ex.ProcessCurrentVersion);
        Assert.Contains("author-registered", ex.Message);
        Assert.Contains("SchemaVersion 2", ex.Message);
    }

    [EventTypeName("author-registered")]
    private sealed record LegacyRegistered(Guid Id, DateTime Timestamp) : IEvent;

    [EventTypeName("author-registered", version: 1)]
    private sealed record AuthorRegisteredV1(Guid Id, DateTime Timestamp, string Name) : IEvent;

    [EventTypeName("author-registered", version: 2, current: true)]
    private sealed record AuthorRegistered(Guid Id, DateTime Timestamp, string Name, string Bio) : IEvent;

    [EventTypeName("kept-current", version: 1, current: true)]
    private sealed record KeptCurrentV1(Guid Id, DateTime Timestamp) : IEvent;

    [EventTypeName("kept-current", version: 2)]
    private sealed record NotCurrentV2(Guid Id, DateTime Timestamp) : IEvent;

    [EventTypeName("author-two-current", version: 1, current: true)]
    private sealed record TwoCurrentV1(Guid Id, DateTime Timestamp) : IEvent;

    [EventTypeName("author-two-current", version: 2, current: true)]
    private sealed record TwoCurrentV2(Guid Id, DateTime Timestamp) : IEvent;

    [EventTypeName("author-no-current", version: 1)]
    private sealed record NoCurrentV1(Guid Id, DateTime Timestamp) : IEvent;

    [EventTypeName("author-no-current", version: 2)]
    private sealed record NoCurrentV2(Guid Id, DateTime Timestamp) : IEvent;

    [EventTypeName("dup-token")]
    private sealed record DupA(Guid Id, DateTime Timestamp) : IEvent;

    [EventTypeName("dup-token")]
    private sealed record DupB(Guid Id, DateTime Timestamp) : IEvent;

    [EventTypeName("book-registered")]
    private sealed record BookRegistered(Guid Id, DateTime Timestamp, string Title) : IEvent;

    private sealed class BytesEventSerializer : IEventSerializer
    {
        public string ContentType => "application/json";

        public ReadOnlyMemory<byte> Serialize(object obj, Type type)
            => JsonSerializer.SerializeToUtf8Bytes(obj, type);

        public object Deserialize(ReadOnlyMemory<byte> data, Type type)
            => JsonSerializer.Deserialize(data.Span, type)
               ?? throw new InvalidOperationException($"Failed to deserialize {type.Name}");
    }
}
