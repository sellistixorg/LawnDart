using System.Net;
using System.Net.Http.Json;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using LawnDart.Authorization;
using Xunit;

namespace LawnDart.AspNetCore.Tests.Integration;

/// <summary>
/// Integration tests for auto-registered command endpoints using an in-process test server.
/// </summary>
public class CommandEndpointIntegrationTests : IDisposable
{
    private readonly TestAppFactory _factory;
    private bool _disposed;

    public CommandEndpointIntegrationTests()
    {
        _factory = new TestAppFactory();
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            _factory.Dispose();
            _disposed = true;
        }
    }

    // ── Authorized ───────────────────────────────────────────────────────────

    [Fact]
    public async Task PostCommand_WhenAuthorized_Returns202Accepted()
    {
        var client = _factory.CreateClientForMode(AuthMode.Authorized);

        var payload = new { Id = Guid.NewGuid(), ProductCode = "ABC", Quantity = 5 };
        var response = await client.PostAsJsonAsync("/api/tests/create-order", payload);

        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
    }

    // ── Forbidden ────────────────────────────────────────────────────────────

    [Fact]
    public async Task PostCommand_WhenPermissionDenied_Returns403()
    {
        var client = _factory.CreateClientForMode(AuthMode.Forbidden);

        var payload = new { Id = Guid.NewGuid(), ProductCode = "ABC", Quantity = 5 };
        var response = await client.PostAsJsonAsync("/api/tests/create-order", payload);

        Assert.Equal(HttpStatusCode.Forbidden, response.StatusCode);
    }

    // ── No auth attributes → always passes ───────────────────────────────────

    [Fact]
    public async Task PostCommand_WithNoAuthAttributes_Returns202Regardless()
    {
        // ShipOrderCommand has no [RequiresPermission] attributes.
        // The system skips the authorization check entirely for unannotated commands.
        var client = _factory.CreateClientForMode(AuthMode.Forbidden);

        var payload = new { Id = Guid.NewGuid(), TrackingNumber = "TRACK-001" };
        var response = await client.PostAsJsonAsync("/api/tests/ship-order", payload);

        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
    }

    [Fact]
    public async Task PostCommand_WhenAuthorizedHandlerThrowsDomainException_Returns422()
    {
        var client = _factory.CreateClientForMode(AuthMode.Authorized);

        var payload = new { Id = Guid.NewGuid() };
        var response = await client.PostAsJsonAsync("/api/tests/reject-authorized-order", payload);

        await AssertProblemDetailsAsync(
            response,
            HttpStatusCode.UnprocessableEntity,
            "Domain rule violated",
            "violates a business rule");
    }

    [Fact]
    public async Task PostCommand_WhenNoAuthHandlerThrowsDomainException_Returns422()
    {
        var client = _factory.CreateClientForMode(AuthMode.Forbidden);

        var payload = new { Id = Guid.NewGuid() };
        var response = await client.PostAsJsonAsync("/api/tests/reject-shipment", payload);

        await AssertProblemDetailsAsync(
            response,
            HttpStatusCode.UnprocessableEntity,
            "Domain rule violated",
            "violates a business rule");
    }

    [Fact]
    public async Task PostCommand_WhenAuthorizedHandlerThrowsConcurrencyException_Returns409()
    {
        var client = _factory.CreateClientForMode(AuthMode.Authorized);

        var payload = new { Id = Guid.NewGuid() };
        var response = await client.PostAsJsonAsync("/api/tests/concurrent-authorized-order", payload);

        await AssertProblemDetailsAsync(
            response,
            HttpStatusCode.Conflict,
            "Conflict",
            "updated by another request");
    }

    [Fact]
    public async Task PostCommand_WhenNoAuthHandlerThrowsConcurrencyException_Returns409()
    {
        var client = _factory.CreateClientForMode(AuthMode.Forbidden);

        var payload = new { Id = Guid.NewGuid() };
        var response = await client.PostAsJsonAsync("/api/tests/concurrent-shipment", payload);

        await AssertProblemDetailsAsync(
            response,
            HttpStatusCode.Conflict,
            "Conflict",
            "updated by another request");
    }

    // ── Multiple routes are independently registered ──────────────────────────

    [Fact]
    public async Task TwoDistinctCommands_AreRegisteredAtSeparateRoutes()
    {
        var client = _factory.CreateClientForMode(AuthMode.Authorized);

        var orderPayload = new { Id = Guid.NewGuid(), ProductCode = "ABC", Quantity = 1 };
        var shipPayload = new { Id = Guid.NewGuid(), TrackingNumber = "T-123" };

        var orderResponse = await client.PostAsJsonAsync("/api/tests/create-order", orderPayload);
        var shipResponse = await client.PostAsJsonAsync("/api/tests/ship-order", shipPayload);

        Assert.Equal(HttpStatusCode.Accepted, orderResponse.StatusCode);
        Assert.Equal(HttpStatusCode.Accepted, shipResponse.StatusCode);
    }

    // ── OpenAPI document contains both command routes ─────────────────────────

    [Fact]
    public async Task OpenApiDocument_ContainsBothCommandRoutes()
    {
        var client = _factory.CreateClientForMode(AuthMode.Authorized);
        var response = await client.GetAsync("/openapi/v1.json");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var json = await response.Content.ReadAsStringAsync();

        Assert.Contains("/api/tests/create-order", json);
        Assert.Contains("/api/tests/ship-order", json);
    }

    // ── Test application factory ─────────────────────────────────────────────

    public class TestAppFactory : IDisposable
    {
        private readonly Dictionary<AuthMode, (IHost Host, HttpClient Client)> _servers = [];
        private bool _disposed;

        public HttpClient CreateClientForMode(AuthMode mode)
        {
            if (!_servers.TryGetValue(mode, out var entry))
            {
                entry = BuildServer(mode);
                _servers[mode] = entry;
            }
            return entry.Client;
        }

        private static (IHost, HttpClient) BuildServer(AuthMode mode)
        {
            var host = new HostBuilder()
                .ConfigureWebHost(webHost =>
                {
                    webHost.UseTestServer();
                    webHost.UseEnvironment("Test");

                    webHost.ConfigureServices(services =>
                    {
                        services.AddSingleton<IAuthorizationContextProvider, StubAuthorizationContextProvider>();
                        services.AddLawnDartHttpCommands(typeof(TestAppFactory).Assembly);
                        services.AddOpenApi();

                        // Replace the real auth provider with a controllable fake
                        var existing = services.SingleOrDefault(
                            d => d.ServiceType == typeof(IAuthorizationProvider));
                        if (existing != null) services.Remove(existing);

                        services.AddSingleton<IAuthorizationProvider>(
                            new FakeAuthorizationProvider(mode));
                    });

                    webHost.Configure(app =>
                    {
                        app.UseRouting();
                        app.UseEndpoints(endpoints =>
                        {
                            endpoints.MapOpenApi();
                            endpoints.MapLawnDartCommands();
                        });
                    });
                })
                .Build();

            host.Start();
            var client = host.GetTestClient();
            return (host, client);
        }

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;
            foreach (var (host, client) in _servers.Values)
            {
                client.Dispose();
                host.Dispose();
            }
        }
    }

    public enum AuthMode { Authorized, Forbidden }

    private sealed class StubAuthorizationContextProvider : IAuthorizationContextProvider
    {
        public bool CanProvideContext() => true;

        public Task<AuthorizationContext?> GetAuthorizationContextAsync(CancellationToken cancellationToken = default)
            => Task.FromResult<AuthorizationContext?>(new AuthorizationContext
            {
                UserId = "test-user",
                TransportType = "HTTP"
            });
    }

    private class FakeAuthorizationProvider : IAuthorizationProvider
    {
        private readonly AuthMode _mode;
        public FakeAuthorizationProvider(AuthMode mode) => _mode = mode;

        public Task<AuthorizationResult> CheckPermissionAsync(
            AuthorizationContext context, string permission, CancellationToken ct = default)
            => Task.FromResult(_mode == AuthMode.Authorized
                ? AuthorizationResult.Success()
                : AuthorizationResult.Failure("Permission denied", permission));

        public Task<AuthorizationResult> CheckEntitlementAsync(
            AuthorizationContext context, string entitlement, CancellationToken ct = default)
            => Task.FromResult(AuthorizationResult.Success());

        public Task<AuthorizationResult> CheckPolicyAsync(
            AuthorizationContext context, string policyName, CancellationToken ct = default)
            => Task.FromResult(AuthorizationResult.Success());
    }

    private static async Task AssertProblemDetailsAsync(
        HttpResponseMessage response,
        HttpStatusCode expectedStatus,
        string expectedTitle,
        string expectedDetailFragment)
    {
        Assert.Equal(expectedStatus, response.StatusCode);

        var problem = await response.Content.ReadFromJsonAsync<ProblemDetails>();
        Assert.NotNull(problem);
        Assert.Equal((int)expectedStatus, problem!.Status);
        Assert.Equal(expectedTitle, problem.Title);
        Assert.Contains(expectedDetailFragment, problem.Detail ?? string.Empty);
    }
}
