using Microsoft.Extensions.Options;
using LawnDart;
using LawnDart.Aggregates;
using LawnDart.Authorization;
using LawnDart.EventSourcing.Aggregates;
using LawnDart.EventSourcing.EventStore;
using LawnDart.EventStore;
using LawnDart.Metadata;
using LawnDart.TestUtilities;
using Xunit;

namespace LawnDart.EventSourcing.Tests.Authorization;

/// <summary>
/// End-to-end integration tests for authorization in the repository pipeline.
/// </summary>
public class AuthorizationIntegrationTests
{
    [Fact]
    public async Task HandleCommandAsync_WithAuthorization_EnforcesPermissions()
    {
        // Arrange
        var eventStore = new InMemoryEventStore();
        var metadataProvider = new TestMetadataProvider();
        var tenantProvider = new TestTenantContextProvider("test-tenant");
        var options = Options.Create(new LawnDartOptions
        {
            EnableAuthorization = true,
            RequireTenantId = false
        });

        var authContext = new AuthorizationContext
        {
            UserClaims = new List<string> { "permission:orders.create" },
            AccountEntitlements = new List<string>()
        };
        var authProvider = new TestAuthorizationProvider(authContext);
        var authContextProvider = new TestAuthorizationContextProvider(authContext);
        var authService = new AuthorizationService(authContextProvider, authProvider);

        var repository = new AggregateRepository(
            eventStore,
            metadataProvider,
            tenantProvider,
            options,
            null,
            authService,
            null);

        var aggregate = new TestAggregate();
        aggregate.SetStreamId("test-tenant:TestAggregate:test-id");

        // Act - Should succeed with correct permission
        var command = new CreateOrderCommand(Guid.NewGuid(), "order-1");
        await repository.HandleCommandAsync(aggregate, command);

        // Assert - events should be flushed to store
        Assert.Empty(aggregate.PendingEvents); // Already flushed
        var events = await eventStore.ReadStreamAsync(aggregate.StreamId);
        Assert.Single(events);
    }

    [Fact]
    public async Task HandleCommandAsync_WithoutPermission_ThrowsUnauthorizedException()
    {
        // Arrange
        var eventStore = new InMemoryEventStore();
        var metadataProvider = new TestMetadataProvider();
        var tenantProvider = new TestTenantContextProvider("test-tenant");
        var options = Options.Create(new LawnDartOptions
        {
            EnableAuthorization = true,
            RequireTenantId = false
        });

        var authContext = new AuthorizationContext
        {
            UserClaims = new List<string>(), // No permissions
            AccountEntitlements = new List<string>()
        };
        var authProvider = new TestAuthorizationProvider(authContext);
        var authContextProvider = new TestAuthorizationContextProvider(authContext);
        var authService = new AuthorizationService(authContextProvider, authProvider);

        var repository = new AggregateRepository(
            eventStore,
            metadataProvider,
            tenantProvider,
            options,
            null,
            authService,
            null);

        var aggregate = new TestAggregate();
        aggregate.SetStreamId("test-tenant:TestAggregate:test-id");

        // Act & Assert
        var command = new CreateOrderCommand(Guid.NewGuid(), "order-1");
        await Assert.ThrowsAsync<UnauthorizedAccessException>(async () =>
            await repository.HandleCommandAsync(aggregate, command));
    }

    [Fact]
    public async Task HandleCommandAsync_WithAuthorizationDisabled_AllowsAllCommands()
    {
        // Arrange
        var eventStore = new InMemoryEventStore();
        var metadataProvider = new TestMetadataProvider();
        var tenantProvider = new TestTenantContextProvider("test-tenant");
        var options = Options.Create(new LawnDartOptions
        {
            EnableAuthorization = false, // Disabled
            RequireTenantId = false
        });

        // Auth service registered but disabled via options
        var authContext = new AuthorizationContext
        {
            UserClaims = new List<string>(), // No permissions
            AccountEntitlements = new List<string>()
        };
        var authProvider = new TestAuthorizationProvider(authContext);
        var authContextProvider = new TestAuthorizationContextProvider(authContext);
        var authService = new AuthorizationService(authContextProvider, authProvider);

        var repository = new AggregateRepository(
            eventStore,
            metadataProvider,
            tenantProvider,
            options,
            null,
            authService,
            null);

        var aggregate = new TestAggregate();
        aggregate.SetStreamId("test-tenant:TestAggregate:test-id");

        // Act - Should succeed even without permission because authorization is disabled
        var command = new CreateOrderCommand(Guid.NewGuid(), "order-1");
        await repository.HandleCommandAsync(aggregate, command);

        // Assert - events should be flushed to store
        Assert.Empty(aggregate.PendingEvents); // Already flushed
        var events = await eventStore.ReadStreamAsync(aggregate.StreamId);
        Assert.Single(events);
    }

    [Fact]
    public async Task HandleCommandAsync_WithEntitlementCheck_EnforcesAccountLevel()
    {
        // Arrange
        var eventStore = new InMemoryEventStore();
        var metadataProvider = new TestMetadataProvider();
        var tenantProvider = new TestTenantContextProvider("test-tenant");
        var options = Options.Create(new LawnDartOptions
        {
            EnableAuthorization = true,
            RequireTenantId = false
        });

        var authContext = new AuthorizationContext
        {
            UserClaims = new List<string> { "permission:orders.premium" },
            AccountEntitlements = new List<string> { "PremiumFeatures" }
        };
        var authProvider = new TestAuthorizationProvider(authContext);
        var authContextProvider = new TestAuthorizationContextProvider(authContext);
        var authService = new AuthorizationService(authContextProvider, authProvider);

        var repository = new AggregateRepository(
            eventStore,
            metadataProvider,
            tenantProvider,
            options,
            null,
            authService,
            null);

        var aggregate = new TestAggregate();
        aggregate.SetStreamId("test-tenant:TestAggregate:test-id");

        // Act - Should succeed with correct permission and entitlement
        var command = new PremiumOrderCommand(Guid.NewGuid(), "premium-order");
        await repository.HandleCommandAsync(aggregate, command);

        // Assert - events should be flushed to store
        Assert.Empty(aggregate.PendingEvents); // Already flushed
        var events = await eventStore.ReadStreamAsync(aggregate.StreamId);
        Assert.Single(events);
    }

    [Fact]
    public async Task HandleCommandAsync_WithoutEntitlement_ThrowsUnauthorizedException()
    {
        // Arrange
        var eventStore = new InMemoryEventStore();
        var metadataProvider = new TestMetadataProvider();
        var tenantProvider = new TestTenantContextProvider("test-tenant");
        var options = Options.Create(new LawnDartOptions
        {
            EnableAuthorization = true,
            RequireTenantId = false
        });

        var authContext = new AuthorizationContext
        {
            UserClaims = new List<string> { "permission:orders.premium" },
            AccountEntitlements = new List<string>() // No entitlement
        };
        var authProvider = new TestAuthorizationProvider(authContext);
        var authContextProvider = new TestAuthorizationContextProvider(authContext);
        var authService = new AuthorizationService(authContextProvider, authProvider);

        var repository = new AggregateRepository(
            eventStore,
            metadataProvider,
            tenantProvider,
            options,
            null,
            authService,
            null);

        var aggregate = new TestAggregate();
        aggregate.SetStreamId("test-tenant:TestAggregate:test-id");

        // Act & Assert
        var command = new PremiumOrderCommand(Guid.NewGuid(), "premium-order");
        await Assert.ThrowsAsync<UnauthorizedAccessException>(async () =>
            await repository.HandleCommandAsync(aggregate, command));
    }

    [Fact]
    public async Task HandleCommandAsync_CapturesAuthorizationMetadata()
    {
        // Arrange
        var eventStore = new InMemoryEventStore();
        var metadataProvider = new TestMetadataProvider();
        var tenantProvider = new TestTenantContextProvider("test-tenant");
        var options = Options.Create(new LawnDartOptions
        {
            EnableAuthorization = true,
            RequireTenantId = false
        });

        var authContext = new AuthorizationContext
        {
            UserClaims = new List<string> { "permission:orders.create" },
            AccountEntitlements = new List<string>()
        };
        var authProvider = new TestAuthorizationProvider(authContext);
        var authContextProvider = new TestAuthorizationContextProvider(authContext);
        var authService = new AuthorizationService(authContextProvider, authProvider);

        var repository = new AggregateRepository(
            eventStore,
            metadataProvider,
            tenantProvider,
            options,
            null,
            authService,
            null);

        var aggregate = new TestAggregate();
        aggregate.SetStreamId("test-tenant:TestAggregate:test-id");

        // Act
        var command = new CreateOrderCommand(Guid.NewGuid(), "order-1");
        var metadata = new CommandMetadata { UserId = "user-123" };
        await repository.HandleCommandAsync(aggregate, command, metadata);

        // Assert - Metadata should be captured and events flushed
        Assert.Empty(aggregate.PendingEvents); // Already flushed
        var events = await eventStore.ReadStreamAsync(aggregate.StreamId);
        Assert.Single(events);
        Assert.Equal("user-123", events[0].Metadata.UserId);
    }

    // Test types
    [RequiresPermission("orders.create")]
    private record CreateOrderCommand(Guid Id, string OrderNumber) : ICommand;

    [RequiresPermission("orders.premium")]
    [RequiresEntitlement("PremiumFeatures")]
    private record PremiumOrderCommand(Guid Id, string OrderNumber) : ICommand;
[EventTypeName("authorization-integration-tests.order-created-event")]

    private record OrderCreatedEvent(Guid OrderId, string OrderNumber) : IEvent
    {
        public Guid Id { get; init; } = Guid.NewGuid();
        public DateTime Timestamp { get; init; } = DateTime.UtcNow;
    }

    private class TestState : IState
    {
        public int CommandCount { get; private set; }

        public void Apply(OrderCreatedEvent @event)
        {
            CommandCount++;
        }
    }

    private class TestAggregate : AggregateRoot<TestState>
    {
        public void Handle(CreateOrderCommand command) =>
            Apply(new OrderCreatedEvent(command.Id, command.OrderNumber));

        public void Handle(PremiumOrderCommand command) =>
            Apply(new OrderCreatedEvent(command.Id, command.OrderNumber));

        protected override void ApplyEventToState(IEvent @event)
        {
            if (@event is OrderCreatedEvent orderCreated)
            {
                State.Apply(orderCreated);
            }
        }
    }

    private class TestAuthorizationProvider : IAuthorizationProvider
    {
        private readonly AuthorizationContext _context;

        public TestAuthorizationProvider(AuthorizationContext context)
        {
            _context = context;
        }

        public Task<AuthorizationResult> CheckPermissionAsync(
            AuthorizationContext context,
            string permissionName,
            CancellationToken cancellationToken = default)
        {
            var hasPermission = context.UserClaims.Contains($"permission:{permissionName}") ||
                              context.UserClaims.Contains($"permissions:{permissionName}");
            return Task.FromResult(hasPermission
                ? AuthorizationResult.Success()
                : AuthorizationResult.Failure($"Missing permission: {permissionName}"));
        }

        public Task<AuthorizationResult> CheckEntitlementAsync(
            AuthorizationContext context,
            string entitlementName,
            CancellationToken cancellationToken = default)
        {
            var hasEntitlement = context.AccountEntitlements.Contains(entitlementName);
            return Task.FromResult(hasEntitlement
                ? AuthorizationResult.Success()
                : AuthorizationResult.Failure($"Missing entitlement: {entitlementName}"));
        }

        public Task<AuthorizationResult> CheckPolicyAsync(
            AuthorizationContext context,
            string policyName,
            CancellationToken cancellationToken = default)
        {
            return Task.FromResult(AuthorizationResult.Failure("Policy checks not implemented in test"));
        }
    }

    private class TestAuthorizationContextProvider : IAuthorizationContextProvider
    {
        private readonly AuthorizationContext _context;

        public TestAuthorizationContextProvider(AuthorizationContext context)
        {
            _context = context;
        }

        public bool CanProvideContext() => true;

        public Task<AuthorizationContext?> GetAuthorizationContextAsync(CancellationToken cancellationToken = default)
        {
            return Task.FromResult<AuthorizationContext?>(_context);
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
