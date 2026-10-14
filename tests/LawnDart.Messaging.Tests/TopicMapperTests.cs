namespace LawnDart.Messaging.Tests;

public class TopicMapperTests
{
    [Fact]
    public void The_default_mapper_uses_the_declared_name()
    {
        var mapper = DeclaredTypeTopicMapper.Instance;
        Assert.Equal("order-placed", mapper.TopicFor("order-placed"));
        Assert.Same(mapper, TopicMapperSelection.Select(configured: null, registered: null));
        Assert.Same(mapper, TopicMapperSelection.Select(mapper, registered: null));
    }

    [Fact]
    public void A_configured_mapper_wins_over_a_registered_one()
    {
        var configured = new SharedTopicMapper("from-options");
        var registered = new SharedTopicMapper("from-di");
        var selected = TopicMapperSelection.Select(configured, registered);
        Assert.Same(configured, selected);
        Assert.Equal("from-options", selected.TopicFor("order-placed"));
    }

    [Fact]
    public void A_registered_mapper_is_used_when_options_keep_the_default()
    {
        var registered = new SharedTopicMapper("from-di");
        var selected = TopicMapperSelection.Select(DeclaredTypeTopicMapper.Instance, registered);
        Assert.Same(registered, selected);
        Assert.Equal("from-di", selected.TopicFor("order-shipped"));
    }

    [Fact]
    public void A_shared_mapper_returns_one_topic()
    {
        var mapper = new SharedTopicMapper("lawndart");
        Assert.Equal("lawndart", mapper.TopicFor("order-placed"));
        Assert.Equal("lawndart", mapper.TopicFor("invoice-issued"));
    }

    [Fact]
    public void A_group_maps_listed_types_and_leaves_the_rest_on_their_own_names()
    {
        var mapper = new GroupTopicMapper(new Dictionary<string, string>
        {
            ["order-placed"] = "orders",
            ["order-shipped"] = "orders",
        });

        Assert.Equal("orders", mapper.TopicFor("order-placed"));
        Assert.Equal("orders", mapper.TopicFor("order-shipped"));
        Assert.Equal("invoice-issued", mapper.TopicFor("invoice-issued"));
        Assert.Null(mapper.FallbackTopic);
    }

    [Fact]
    public void A_group_fallback_catches_types_that_are_not_listed()
    {
        var mapper = new GroupTopicMapper(
            new Dictionary<string, string> { ["order-placed"] = "orders" },
            fallbackTopic: "lawndart");

        Assert.Equal("orders", mapper.TopicFor("order-placed"));
        Assert.Equal("lawndart", mapper.TopicFor("invoice-issued"));
        Assert.Equal("lawndart", mapper.FallbackTopic);
    }

    [Fact]
    public void A_prefix_mapper_uses_the_text_before_the_separator()
    {
        var mapper = new PrefixTopicMapper('.');
        Assert.Equal('.', mapper.Separator);
        Assert.Equal("orders", mapper.TopicFor("orders.placed"));
        Assert.Equal("orders", mapper.TopicFor("orders.placed.v2"));
        Assert.Equal("invoice-issued", mapper.TopicFor("invoice-issued"));
        Assert.Equal(".placed", mapper.TopicFor(".placed"));
    }

    [Fact]
    public void A_delegate_mapper_returns_the_function_result()
    {
        var mapper = new DelegateTopicMapper(type => type.StartsWith("order-", StringComparison.Ordinal) ? "orders" : type);
        Assert.Equal("orders", mapper.TopicFor("order-placed"));
        Assert.Equal("invoice-issued", mapper.TopicFor("invoice-issued"));
    }

    [Fact]
    public void A_blank_topic_is_rejected()
    {
        Assert.Throws<ArgumentException>(() => new SharedTopicMapper(" "));
        Assert.Throws<ArgumentException>(() => new GroupTopicMapper(new Dictionary<string, string>
        {
            ["order-placed"] = " ",
        }));
        Assert.Throws<ArgumentException>(() => new PrefixTopicMapper(' '));
        Assert.Throws<ArgumentNullException>(() => new DelegateTopicMapper(null!));

        var mapper = new DelegateTopicMapper(_ => " ");
        var ex = Assert.Throws<InvalidOperationException>(() => mapper.TopicFor("order-placed"));
        Assert.Contains("order-placed", ex.Message, StringComparison.Ordinal);
    }
}
