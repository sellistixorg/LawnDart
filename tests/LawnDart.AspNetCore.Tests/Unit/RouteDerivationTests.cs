using LawnDart.AspNetCore;
using LawnDart.AspNetCore.Tests;
using LawnDart.AspNetCore.Tests.Namespaced.Orders.Commands;
using Xunit;

namespace LawnDart.AspNetCore.Tests.Unit;

public class RouteDerivationTests
{
    private static HttpCommandOptions DefaultOptions() => new();

    // ── ToKebabCase ─────────────────────────────────────────────────────────

    [Theory]
    [InlineData("CreateOrder", "create-order")]
    [InlineData("PlaceOrder", "place-order")]
    [InlineData("DeleteOrderLineItem", "delete-order-line-item")]
    [InlineData("Submit", "submit")]
    [InlineData("", "")]
    public void ToKebabCase_ConvertsCorrectly(string input, string expected)
    {
        var result = CommandEndpointRegistrar.ToKebabCase(input);
        Assert.Equal(expected, result);
    }

    // ── DeriveActionName ─────────────────────────────────────────────────────

    [Theory]
    [InlineData(typeof(CreateOrderCommand), "create-order")]
    [InlineData(typeof(ShipOrderCommand), "ship-order")]
    [InlineData(typeof(SubmitCommand), "submit")]
    public void DeriveActionName_StripsCommandSuffix(Type commandType, string expected)
    {
        var result = CommandEndpointRegistrar.DeriveActionName(commandType);
        Assert.Equal(expected, result);
    }

    // ── DeriveGroup (ByNamespace) ─────────────────────────────────────────────

    [Fact]
    public void DeriveGroup_UsesLastMeaningfulNamespaceSegment()
    {
        // Namespace: LawnDart.AspNetCore.Tests.Namespaced.Orders.Commands
        // After filtering out "Commands" and "Patterns" → last segment = "orders"
        var result = CommandEndpointRegistrar.DeriveGroup(
            typeof(Namespaced.Orders.Commands.CreateOrderCommand),
            DefaultOptions());

        Assert.Equal("orders", result);
    }

    [Fact]
    public void DeriveGroup_SkipsCommandsSegment()
    {
        var result = CommandEndpointRegistrar.DeriveGroup(
            typeof(Namespaced.Orders.Commands.CreateOrderCommand),
            DefaultOptions());

        Assert.NotEqual("commands", result);
    }

    [Fact]
    public void DeriveGroup_FlatStrategy_ReturnsFallbackTag()
    {
        var options = new HttpCommandOptions { TagStrategy = CommandTagStrategy.Flat, FallbackTag = "Commands" };
        var result = CommandEndpointRegistrar.DeriveGroup(typeof(CreateOrderCommand), options);

        Assert.Equal("commands", result);
    }

    // ── DeriveRoute ──────────────────────────────────────────────────────────

    [Fact]
    public void DeriveRoute_WithDefaultPrefix_BuildsExpectedPattern()
    {
        // Namespaced command → /api/orders/create-order
        var result = CommandEndpointRegistrar.DeriveRoute(
            typeof(Namespaced.Orders.Commands.CreateOrderCommand),
            DefaultOptions());

        Assert.Equal("/api/orders/create-order", result);
    }

    [Fact]
    public void DeriveRoute_WithCustomPrefix_UsesCustomPrefix()
    {
        var options = new HttpCommandOptions { RoutePrefix = "v2" };
        var result = CommandEndpointRegistrar.DeriveRoute(
            typeof(Namespaced.Orders.Commands.CreateOrderCommand), options);

        Assert.Equal("/v2/orders/create-order", result);
    }

    [Fact]
    public void DeriveRoute_WithEmptyPrefix_OmitsPrefix()
    {
        var options = new HttpCommandOptions { RoutePrefix = "" };
        var result = CommandEndpointRegistrar.DeriveRoute(
            typeof(Namespaced.Orders.Commands.CreateOrderCommand), options);

        Assert.Equal("/orders/create-order", result);
    }

    // ── HumanizeName ─────────────────────────────────────────────────────────

    [Theory]
    [InlineData(typeof(CreateOrderCommand), "Create Order")]
    [InlineData(typeof(ShipOrderCommand), "Ship Order")]
    public void HumanizeName_ReturnsReadableSummary(Type commandType, string expected)
    {
        var result = CommandEndpointRegistrar.HumanizeName(commandType);
        Assert.Equal(expected, result);
    }
}
