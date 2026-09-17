using LawnDart;
using LawnDart.EventStore;
using Microsoft.Extensions.DependencyInjection;

namespace LawnDart.Tests.EventStore;

public class EventUpcastPipelineTests
{
    [Fact]
    public void Materialize_TwoStepChain_UpcastsV1ToCurrent()
    {
        var catalog = EventTypeCatalog.Materialize(
            [typeof(AuthorV1), typeof(AuthorV2), typeof(AuthorCurrent)]);
        var pipeline = EventUpcastPipeline.Materialize(
            catalog,
            [typeof(AuthorV1ToV2), typeof(AuthorV2ToCurrent)]);

        var stored = new AuthorV1(Guid.Parse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"), new DateTime(2026, 1, 2, 3, 4, 5, DateTimeKind.Utc), "Ada");
        var current = Assert.IsType<AuthorCurrent>(pipeline.UpcastToCurrent(stored));

        Assert.Equal(stored.Id, current.Id);
        Assert.Equal(stored.Timestamp, current.Timestamp);
        Assert.Equal("Ada", current.Name);
        Assert.Equal("", current.Bio);
        Assert.Equal("unknown", current.Country);
    }

    [Fact]
    public void Materialize_TwoStepChain_UpcastsV2ToCurrent()
    {
        var catalog = EventTypeCatalog.Materialize(
            [typeof(AuthorV1), typeof(AuthorV2), typeof(AuthorCurrent)]);
        var pipeline = EventUpcastPipeline.Materialize(
            catalog,
            [typeof(AuthorV1ToV2), typeof(AuthorV2ToCurrent)]);

        var stored = new AuthorV2(Guid.NewGuid(), DateTime.UtcNow, "Ada", "bio");
        var current = Assert.IsType<AuthorCurrent>(pipeline.UpcastToCurrent(stored));
        Assert.Equal("bio", current.Bio);
        Assert.Equal("unknown", current.Country);
    }

    [Fact]
    public void Materialize_CurrentInstance_ReturnsSameReference()
    {
        var catalog = EventTypeCatalog.Materialize(
            [typeof(AuthorV1), typeof(AuthorV2), typeof(AuthorCurrent)]);
        var pipeline = EventUpcastPipeline.Materialize(
            catalog,
            [typeof(AuthorV1ToV2), typeof(AuthorV2ToCurrent)]);
        var current = new AuthorCurrent(Guid.NewGuid(), DateTime.UtcNow, "Ada", "bio", "UK");

        Assert.Same(current, pipeline.UpcastToCurrent(current));
    }

    [Fact]
    public void Materialize_HistoricalFamilyWithNoUpcasters_FailsWarmup()
    {
        var catalog = EventTypeCatalog.Materialize([typeof(AuthorV1), typeof(AuthorCurrent)]);

        var ex = Assert.Throws<InvalidOperationException>(() =>
            EventUpcastPipeline.Materialize(catalog, []));

        Assert.Contains("author-evolved", ex.Message);
        Assert.Contains("SchemaVersion 1", ex.Message);
        Assert.Contains("WithUpcasters", ex.Message);
    }

    [Fact]
    public void Materialize_MissingHop_FailsWarmup()
    {
        var catalog = EventTypeCatalog.Materialize(
            [typeof(AuthorV1), typeof(AuthorV2), typeof(AuthorCurrent)]);

        var ex = Assert.Throws<InvalidOperationException>(() =>
            EventUpcastPipeline.Materialize(catalog, [typeof(AuthorV2ToCurrent)]));

        Assert.Contains("author-evolved", ex.Message);
        Assert.Contains("SchemaVersion 1", ex.Message);
        Assert.Contains("3", ex.Message);
        Assert.Contains("WithUpcasters", ex.Message);
    }

    [Fact]
    public void Materialize_DirectHopsToCurrent_AreComplete()
    {
        var catalog = EventTypeCatalog.Materialize(
            [typeof(AuthorV1), typeof(AuthorV2), typeof(AuthorCurrent)]);
        var pipeline = EventUpcastPipeline.Materialize(
            catalog,
            [typeof(AuthorV1ToCurrent), typeof(AuthorV2ToCurrent)]);

        var v1 = new AuthorV1(Guid.NewGuid(), DateTime.UtcNow, "Ada");
        var current = Assert.IsType<AuthorCurrent>(pipeline.UpcastToCurrent(v1));
        Assert.Equal("Ada", current.Name);
    }

    [Fact]
    public void Materialize_SingleTypeFamily_NeedsNoUpcasters()
    {
        var catalog = EventTypeCatalog.Materialize([typeof(SoloRegistered)]);
        var pipeline = EventUpcastPipeline.Materialize(catalog, []);
        var stored = new SoloRegistered(Guid.NewGuid(), DateTime.UtcNow);
        Assert.Same(stored, pipeline.UpcastToCurrent(stored));
    }

    [Fact]
    public void Materialize_Downcast_Throws()
    {
        var catalog = EventTypeCatalog.Materialize(
            [typeof(AuthorV1), typeof(AuthorCurrent)]);

        var ex = Assert.Throws<InvalidOperationException>(() =>
            EventUpcastPipeline.Materialize(catalog, [typeof(AuthorCurrentToV1)]));

        Assert.Contains("downcast", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Materialize_CrossFamily_Throws()
    {
        var catalog = EventTypeCatalog.Materialize(
            [typeof(AuthorV1), typeof(AuthorCurrent), typeof(BookRegistered)]);

        var ex = Assert.Throws<InvalidOperationException>(() =>
            EventUpcastPipeline.Materialize(catalog, [typeof(AuthorToBook)]));

        Assert.Contains("families", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Materialize_DuplicateFromType_Throws()
    {
        var catalog = EventTypeCatalog.Materialize(
            [typeof(AuthorV1), typeof(AuthorCurrent)]);

        var ex = Assert.Throws<InvalidOperationException>(() =>
            EventUpcastPipeline.Materialize(
                catalog,
                [typeof(AuthorV1ToCurrent), typeof(AuthorV1ToCurrentAlt)]));

        Assert.Contains("Duplicate", ex.Message);
    }

    [Fact]
    public void WithUpcasters_BeforeEventTypes_Throws()
    {
        var services = new ServiceCollection();
        var builder = services.AddBoundedContext("authors");

        var ex = Assert.Throws<InvalidOperationException>(() =>
            builder.WithUpcasters(typeof(AuthorV1ToCurrent)));

        Assert.Contains("WithEventTypes", ex.Message);
    }

    [Fact]
    public void WithUpcasters_RegistersPipelineForContext()
    {
        var services = new ServiceCollection();
        services.AddBoundedContext("authors")
            .WithEventTypes(typeof(AuthorV1), typeof(AuthorCurrent))
            .WithUpcasters(typeof(AuthorV1ToCurrent));

        using var sp = services.BuildServiceProvider();
        var pipeline = sp.GetRequiredKeyedService<EventUpcastPipeline>("authors");
        var stored = new AuthorV1(Guid.NewGuid(), DateTime.UtcNow, "Ada");
        Assert.IsType<AuthorCurrent>(pipeline.UpcastToCurrent(stored));
    }

    [Fact]
    public void WithUpcasters_ScannedAssemblyWithNoHops_FailsWhenFamilyHasHistory()
    {
        var services = new ServiceCollection();
        var ex = Assert.Throws<InvalidOperationException>(() =>
            services.AddBoundedContext("authors")
                .WithEventTypes(typeof(AuthorV1), typeof(AuthorCurrent))
                .WithUpcasters(typeof(EventTypeCatalog).Assembly));

        Assert.Contains("SchemaVersion 1", ex.Message);
        Assert.Contains("author-evolved", ex.Message);
    }

    [Fact]
    public void WithUpcasters_EmptyTypeList_Throws()
    {
        var services = new ServiceCollection();
        var builder = services.AddBoundedContext("authors")
            .WithEventTypes(typeof(SoloRegistered));

        var ex = Assert.Throws<InvalidOperationException>(() =>
            builder.WithUpcasters(Array.Empty<Type>()));

        Assert.Contains("no upcaster types", ex.Message);
    }

    [Fact]
    public void WithUpcasters_CalledTwice_Throws()
    {
        var services = new ServiceCollection();
        var builder = services.AddBoundedContext("authors")
            .WithEventTypes(typeof(AuthorV1), typeof(AuthorCurrent))
            .WithUpcasters(typeof(AuthorV1ToCurrent));

        var ex = Assert.Throws<InvalidOperationException>(() =>
            builder.WithUpcasters(typeof(AuthorV1ToCurrent)));

        Assert.Contains("already been called", ex.Message);
    }

    [Fact]
    public void Interface_HasNoDowncastMember()
    {
        var methods = typeof(IEventUpcaster<,>).GetMethods()
            .Select(m => m.Name)
            .ToArray();
        Assert.Equal(["Upcast"], methods);
        Assert.DoesNotContain(methods, n => n.Contains("Down", StringComparison.OrdinalIgnoreCase));
    }

    [EventTypeName("author-evolved", version: 1)]
    public sealed record AuthorV1(Guid Id, DateTime Timestamp, string Name) : IEvent;

    [EventTypeName("author-evolved", version: 2)]
    public sealed record AuthorV2(Guid Id, DateTime Timestamp, string Name, string Bio) : IEvent;

    [EventTypeName("author-evolved", version: 3, current: true)]
    public sealed record AuthorCurrent(Guid Id, DateTime Timestamp, string Name, string Bio, string Country) : IEvent;

    [EventTypeName("solo-registered")]
    public sealed record SoloRegistered(Guid Id, DateTime Timestamp) : IEvent;

    [EventTypeName("book-registered")]
    public sealed record BookRegistered(Guid Id, DateTime Timestamp, string Title) : IEvent;

    public sealed class AuthorV1ToV2 : IEventUpcaster<AuthorV2, AuthorV1>
    {
        public AuthorV2 Upcast(AuthorV1 source) => new(source.Id, source.Timestamp, source.Name, "");
    }

    public sealed class AuthorV2ToCurrent : IEventUpcaster<AuthorCurrent, AuthorV2>
    {
        public AuthorCurrent Upcast(AuthorV2 source)
            => new(source.Id, source.Timestamp, source.Name, source.Bio, "unknown");
    }

    public sealed class AuthorV1ToCurrent : IEventUpcaster<AuthorCurrent, AuthorV1>
    {
        public AuthorCurrent Upcast(AuthorV1 source)
            => new(source.Id, source.Timestamp, source.Name, "", "unknown");
    }

    public sealed class AuthorV1ToCurrentAlt : IEventUpcaster<AuthorCurrent, AuthorV1>
    {
        public AuthorCurrent Upcast(AuthorV1 source)
            => new(source.Id, source.Timestamp, source.Name, "alt", "unknown");
    }

    public sealed class AuthorCurrentToV1 : IEventUpcaster<AuthorV1, AuthorCurrent>
    {
        public AuthorV1 Upcast(AuthorCurrent source) => new(source.Id, source.Timestamp, source.Name);
    }

    public sealed class AuthorToBook : IEventUpcaster<BookRegistered, AuthorV1>
    {
        public BookRegistered Upcast(AuthorV1 source) => new(source.Id, source.Timestamp, source.Name);
    }
}
