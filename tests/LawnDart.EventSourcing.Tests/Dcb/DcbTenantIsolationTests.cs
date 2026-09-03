using Microsoft.Extensions.Options;
using LawnDart;
using LawnDart.Dcb;
using LawnDart.EventSourcing.Dcb;
using LawnDart.EventSourcing.EventStore;
using LawnDart.EventStore;
using LawnDart.Metadata;
using LawnDart.TestUtilities;
using Xunit;

namespace LawnDart.EventSourcing.Tests.Dcb;

/// <summary>
/// Tests for DCB tenant isolation enforcement.
/// </summary>
public class DcbTenantIsolationTests
{
    private static IOptions<EventSourcingOptions> EsOptions(bool enforce) =>
        Options.Create(new EventSourcingOptions { EnforceDcbTenantIsolation = enforce });

    private static IOptions<LawnDartOptions> DefaultLawnDartOptions() =>
        Options.Create(new LawnDartOptions());

    [Fact]
    public async Task GetStateAsync_WithEnforcementEnabled_AutoInjectsTenantTag()
    {
        // Arrange
        var eventStore = new InMemoryEventStore();
        var metadataProvider = new TestMetadataProvider();
        var tenantProvider = new TestTenantContextProvider("tenant-123");

        var repository = new DcbRepository(
            eventStore,
            metadataProvider,
            tenantProvider,
            DefaultLawnDartOptions(),
            null,
            null,
            null,
            EsOptions(true));

        // Add events with tenant tag
        await repository.AppendWithContextAsync(
            new[] { new TestEvent { Id = Guid.NewGuid(), Timestamp = DateTime.UtcNow } },
            new[] { "tenant:tenant-123", "order:order-1" });

        // Act - Query without explicit tenant tag (should auto-inject)
        var state = await repository.GetStateAsync<TestState>(new[] { "order:order-1" });

        // Assert
        Assert.NotNull(state);
        Assert.Equal(1, state.EventCount);
    }

    [Fact]
    public async Task GetStateAsync_WithEnforcementEnabled_WithoutTenantProvider_ThrowsException()
    {
        // Arrange
        var eventStore = new InMemoryEventStore();
        var metadataProvider = new TestMetadataProvider();
        var tenantProvider = new TestTenantContextProvider(null); // No tenant

        var repository = new DcbRepository(
            eventStore,
            metadataProvider,
            tenantProvider,
            DefaultLawnDartOptions(),
            null,
            null,
            null,
            EsOptions(true));

        // Act & Assert
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await repository.GetStateAsync<TestState>(new[] { "order:order-1" }));
    }

    [Fact]
    public async Task AppendWithContextAsync_WithEnforcementEnabled_AutoInjectsTenantTag()
    {
        // Arrange
        var eventStore = new InMemoryEventStore();
        var metadataProvider = new TestMetadataProvider();
        var tenantProvider = new TestTenantContextProvider("tenant-456");

        var repository = new DcbRepository(
            eventStore,
            metadataProvider,
            tenantProvider,
            DefaultLawnDartOptions(),
            null,
            null,
            null,
            EsOptions(true));

        // Act - Append without explicit tenant tag (should auto-inject)
        await repository.AppendWithContextAsync(
            new[] { new TestEvent { Id = Guid.NewGuid(), Timestamp = DateTime.UtcNow } },
            new[] { "order:order-1" });

        // Query with explicit tenant tag
        var state = await repository.GetStateAsync<TestState>(new[] { "tenant:tenant-456", "order:order-1" });

        // Assert
        Assert.NotNull(state);
        Assert.Equal(1, state.EventCount);
    }

    [Fact]
    public async Task DcbOperations_WithEnforcementDisabled_AllowsAnyTags()
    {
        // Arrange
        var eventStore = new InMemoryEventStore();
        var metadataProvider = new TestMetadataProvider();
        var tenantProvider = new TestTenantContextProvider(null); // No tenant

        var repository = new DcbRepository(
            eventStore,
            metadataProvider,
            tenantProvider,
            DefaultLawnDartOptions(),
            null,
            null,
            null,
            EsOptions(false));

        // Act - Should work without tenant
        await repository.AppendWithContextAsync(
            new[] { new TestEvent { Id = Guid.NewGuid(), Timestamp = DateTime.UtcNow } },
            new[] { "order:order-1" });

        var state = await repository.GetStateAsync<TestState>(new[] { "order:order-1" });

        // Assert
        Assert.NotNull(state);
        Assert.Equal(1, state.EventCount);
    }

    [Fact]
    public async Task GetStateAsync_WithEnforcementEnabled_WithExistingTenantTag_DoesNotDuplicate()
    {
        // Arrange
        var eventStore = new InMemoryEventStore();
        var metadataProvider = new TestMetadataProvider();
        var tenantProvider = new TestTenantContextProvider("tenant-789");

        var repository = new DcbRepository(
            eventStore,
            metadataProvider,
            tenantProvider,
            DefaultLawnDartOptions(),
            null,
            null,
            null,
            EsOptions(true));

        // Add events
        await repository.AppendWithContextAsync(
            new[] { new TestEvent { Id = Guid.NewGuid(), Timestamp = DateTime.UtcNow } },
            new[] { "tenant:tenant-789", "order:order-1" });

        // Act - Query with explicit tenant tag
        var state = await repository.GetStateAsync<TestState>(new[] { "tenant:tenant-789", "order:order-1" });

        // Assert
        Assert.NotNull(state);
        Assert.Equal(1, state.EventCount);
    }

    [Fact]
    public async Task DcbOperations_PreventsCrossTenantQueries()
    {
        // Arrange
        var eventStore = new InMemoryEventStore();
        var metadataProvider = new TestMetadataProvider();
        var esOptions = EsOptions(true);

        // Setup for tenant-1
        var tenantProvider1 = new TestTenantContextProvider("tenant-1");
        var repository1 = new DcbRepository(eventStore, metadataProvider, tenantProvider1, DefaultLawnDartOptions(), null, null, null, esOptions);

        // Setup for tenant-2
        var tenantProvider2 = new TestTenantContextProvider("tenant-2");
        var repository2 = new DcbRepository(eventStore, metadataProvider, tenantProvider2, DefaultLawnDartOptions(), null, null, null, esOptions);

        // Act - Tenant 1 appends data
        await repository1.AppendWithContextAsync(
            new[] { new TestEvent { Id = Guid.NewGuid(), Timestamp = DateTime.UtcNow } },
            new[] { "order:shared-order" });

        // Tenant 2 appends data with same order tag
        await repository2.AppendWithContextAsync(
            new[] { new TestEvent { Id = Guid.NewGuid(), Timestamp = DateTime.UtcNow } },
            new[] { "order:shared-order" });

        // Query as tenant 1
        var stateTenant1 = await repository1.GetStateAsync<TestState>(new[] { "order:shared-order" });

        // Query as tenant 2
        var stateTenant2 = await repository2.GetStateAsync<TestState>(new[] { "order:shared-order" });

        // Assert - Each tenant should only see their own event
        Assert.Equal(1, stateTenant1.EventCount);
        Assert.Equal(1, stateTenant2.EventCount);
    }

    // Test types
    private record TestEvent : IEvent
    {
        public Guid Id { get; init; }
        public DateTime Timestamp { get; init; }
    }

    private class TestState : IState
    {
        public int EventCount { get; private set; }

        public void Apply(TestEvent @event)
        {
            EventCount++;
        }
    }

    private class TestTenantContextProvider : ITenantContextProvider
    {
        private readonly string? _tenantId;

        public TestTenantContextProvider(string? tenantId)
        {
            _tenantId = tenantId;
        }

        public string? GetTenantId() => _tenantId;

        public string GetTenantIdRequired() =>
            _tenantId ?? throw new InvalidOperationException("Tenant ID is required");
    }
}
