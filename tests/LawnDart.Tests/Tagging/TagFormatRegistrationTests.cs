using Microsoft.Extensions.DependencyInjection;
using LawnDart.Tagging;

namespace LawnDart.Tests.Tagging;

public class TagFormatRegistrationTests
{
    [Fact]
    public void AddLawnDart_RegistersLowercaseTagFormat()
    {
        var services = new ServiceCollection();
        services.AddLawnDart();

        var format = services.BuildServiceProvider().GetRequiredService<ITagFormat>();

        Assert.IsType<LowercaseTagFormat>(format);
    }

    [Fact]
    public void AddTagFormat_BeforeAddLawnDart_IsKept()
    {
        var services = new ServiceCollection();
        services.AddTagFormat<UppercaseTagFormat>();
        services.AddLawnDart();

        var format = services.BuildServiceProvider().GetRequiredService<ITagFormat>();

        Assert.IsType<UppercaseTagFormat>(format);
    }

    [Fact]
    public void AddTagFormat_AfterAddLawnDart_ReplacesDefault()
    {
        var services = new ServiceCollection();
        services.AddLawnDart();
        services.AddTagFormat<UppercaseTagFormat>();

        var format = services.BuildServiceProvider().GetRequiredService<ITagFormat>();

        Assert.IsType<UppercaseTagFormat>(format);
    }

    [Fact]
    public void AddTagFormat_WithContextName_RegistersKeyedFormat()
    {
        var services = new ServiceCollection();
        services.AddLawnDart();
        services.AddTagFormat<UppercaseTagFormat>("academy");

        var sp = services.BuildServiceProvider();

        Assert.IsType<UppercaseTagFormat>(sp.GetRequiredKeyedService<ITagFormat>("academy"));
        Assert.IsType<LowercaseTagFormat>(sp.GetRequiredService<ITagFormat>());
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void AddTagFormat_BlankContextName_Throws(string? contextName)
    {
        var services = new ServiceCollection();

        Assert.Throws<ArgumentException>(() => services.AddTagFormat<UppercaseTagFormat>(contextName!));
    }

    private sealed class UppercaseTagFormat : ITagFormat
    {
        public string Tag(string type, Guid id) => Tag(type, id.ToString("D"));

        public string Tag(string type, string id) => $"{type.ToUpperInvariant()}:{id}";
    }
}
