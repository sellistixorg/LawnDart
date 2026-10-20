using System.Text.RegularExpressions;
using Microsoft.Extensions.DependencyInjection;
using LawnDart.EventStore;
using LawnDart.Tagging;

namespace LawnDart.Tests;

public class LawnDartNamesTests
{
    private static readonly Regex CompiledPattern = new(
        LawnDartNames.Pattern,
        RegexOptions.CultureInvariant | RegexOptions.Compiled);

    [Theory]
    [InlineData("a")]
    [InlineData("A")]
    [InlineData("1")]
    [InlineData("default")]
    [InlineData("ordering")]
    [InlineData("catalog-test-ok")]
    [InlineData("acme_west")]
    [InlineData("a.b")]
    [InlineData("A1._-")]
    public void IsValid_AcceptsNamesInsideTheRule(string value)
    {
        Assert.True(LawnDartNames.IsValid(value));
        Assert.Matches(CompiledPattern, value);
        LawnDartNames.Validate(value, nameof(value));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData(" ")]
    [InlineData(".orders")]
    [InlineData("-orders")]
    [InlineData("_orders")]
    [InlineData("a:b")]
    [InlineData("has space")]
    [InlineData("café")]
    [InlineData("名前")]
    public void IsValid_RejectsEdges(string? value)
    {
        Assert.False(LawnDartNames.IsValid(value));
        if (value is not null)
            Assert.DoesNotMatch(CompiledPattern, value);
    }

    [Fact]
    public void IsValid_Accepts63_Rejects64()
    {
        var sixtyThree = "a" + new string('b', 62);
        var sixtyFour = "a" + new string('b', 63);

        Assert.Equal(LawnDartNames.MaxLength, sixtyThree.Length);
        Assert.True(LawnDartNames.IsValid(sixtyThree));
        Assert.Equal(64, sixtyFour.Length);
        Assert.False(LawnDartNames.IsValid(sixtyFour));

        var ex = Assert.Throws<ArgumentException>(() => LawnDartNames.Validate(sixtyFour, "name"));
        Assert.Contains(LawnDartNames.Pattern, ex.Message);
        Assert.Equal("name", ex.ParamName);
    }

    [Fact]
    public void ValidateContextName_RejectsWrongDefaultSpelling()
    {
        LawnDartNames.ValidateContextName("default", "contextName");

        var ex = Assert.Throws<ArgumentException>(() =>
            LawnDartNames.ValidateContextName("Default", "contextName"));
        Assert.Contains("default", ex.Message);
        Assert.DoesNotContain("Default", ex.Message.Replace("'Default'", "", StringComparison.Ordinal));
    }

    [Fact]
    public void ValidateContextName_WrongDefaultSpelling_RejectedWhenFormatCheckIsOff()
    {
        var ex = Assert.Throws<ArgumentException>(() =>
            LawnDartNames.ValidateContextName("DEFAULT", "contextName", validateFormat: false));
        Assert.Contains("'default'", ex.Message);
    }

    [Fact]
    public void ValidateTenantId_RejectsReservedDefault()
    {
        var ex = Assert.Throws<ArgumentException>(() =>
            LawnDartNames.ValidateTenantId("default", "tenantId"));
        Assert.Contains("reserved", ex.Message);
        Assert.Equal("tenantId", ex.ParamName);
    }

    [Fact]
    public void ValidateTenantId_ValidateNamesFalse_AllowsReservedDefault()
    {
        LawnDartNames.ValidateTenantId("default", "tenantId", validateFormat: false);
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    public void ValidateTenantId_RejectsEmpty_EvenWhenFormatCheckIsOff(string tenantId)
    {
        Assert.Throws<ArgumentException>(() =>
            LawnDartNames.ValidateTenantId(tenantId, "tenantId"));
        Assert.Throws<ArgumentException>(() =>
            LawnDartNames.ValidateTenantId(tenantId, "tenantId", validateFormat: false));
    }

    [Fact]
    public void ValidateTenantId_RejectsColonEvenWhenFormatCheckIsOff()
    {
        var ex = Assert.Throws<ArgumentException>(() =>
            LawnDartNames.ValidateTenantId("acme:west", "tenantId", validateFormat: false));
        Assert.Contains(":", ex.Message);
    }

    [Fact]
    public void ValidateTenantId_AllowsExistingSpellingWhenFormatCheckIsOff()
    {
        LawnDartNames.ValidateTenantId("My Tenant", "tenantId", validateFormat: false);
    }

    [Fact]
    public void AddBoundedContext_RejectsInvalidName_AndCaseDuplicate()
    {
        var services = new ServiceCollection();
        var ex = Assert.Throws<ArgumentException>(() => services.AddBoundedContext("Has Space"));
        Assert.Contains(LawnDartNames.Pattern, ex.Message);

        services.AddBoundedContext("Ordering");
        var duplicate = Assert.Throws<InvalidOperationException>(() => services.AddBoundedContext("ordering"));
        Assert.Contains("ordering", duplicate.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void AddBoundedContext_RejectsDefaultWithWrongCase()
    {
        var services = new ServiceCollection();
        var ex = Assert.Throws<ArgumentException>(() => services.AddBoundedContext("Default"));
        Assert.Contains("'default'", ex.Message);
    }

    [Fact]
    public void AddBoundedContext_ValidateNamesFalse_AllowsExistingName_StillRejectsDefaultCase()
    {
        var services = new ServiceCollection();
        services.AddLawnDart(o => o.ValidateNames = false);

        services.AddBoundedContext("My Context");
        var ex = Assert.Throws<ArgumentException>(() => services.AddBoundedContext("Default"));
        Assert.Contains("'default'", ex.Message);
    }

    [Fact]
    public void AddTagProvider_AndAddTagFormat_RejectInvalidContextName()
    {
        var services = new ServiceCollection();
        var provider = Assert.Throws<ArgumentException>(() =>
            services.AddTagProvider<StubTagProvider>("bad name"));
        var format = Assert.Throws<ArgumentException>(() =>
            services.AddTagFormat<StubTagFormat>(".bad"));
        Assert.Contains(LawnDartNames.Pattern, provider.Message);
        Assert.Contains(LawnDartNames.Pattern, format.Message);
    }

    private sealed class StubTagProvider : ITagProvider
    {
        public IEnumerable<string> GetTags(IEvent @event, object? context = null) => [];
    }

    private sealed class StubTagFormat : ITagFormat
    {
        public string Tag(string type, Guid id) => type;
        public string Tag(string type, string id) => type + ":" + id;
    }
}
