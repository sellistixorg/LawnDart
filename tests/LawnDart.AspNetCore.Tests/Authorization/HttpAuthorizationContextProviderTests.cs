using System.Diagnostics;
using Microsoft.AspNetCore.Http;
using LawnDart.Authorization.AspNetCore;

namespace LawnDart.AspNetCore.Tests.Authorization;

public class HttpAuthorizationContextProviderTests
{
    [Fact]
    public async Task GetAuthorizationContext_DoesNotUseHttpTraceIdentifierAsCorrelationId()
    {
        var http = new DefaultHttpContext();
        http.TraceIdentifier = "kestrel-request-id";

        var provider = new HttpAuthorizationContextProvider(
            new HttpContextAccessor { HttpContext = http });

        var context = await provider.GetAuthorizationContextAsync();

        Assert.NotNull(context);
        Assert.NotEqual(http.TraceIdentifier, context.CorrelationId);
        Assert.Null(context.CorrelationId);
    }

    [Fact]
    public async Task GetAuthorizationContext_UsesTraceparentTraceId()
    {
        var http = new DefaultHttpContext();
        http.TraceIdentifier = "kestrel-request-id";
        http.Request.Headers["traceparent"] =
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

        var provider = new HttpAuthorizationContextProvider(
            new HttpContextAccessor { HttpContext = http });

        var context = await provider.GetAuthorizationContextAsync();

        Assert.NotNull(context);
        Assert.Equal("0af7651916cd43dd8448eb211c80319c", context.CorrelationId);
        Assert.NotEqual(http.TraceIdentifier, context.CorrelationId);
    }

    [Fact]
    public async Task GetAuthorizationContext_PrefersActivityTraceId()
    {
        using var listener = new ActivityListener
        {
            ShouldListenTo = static _ => true,
            Sample = static (ref ActivityCreationOptions<ActivityContext> _) =>
                ActivitySamplingResult.AllDataAndRecorded
        };
        ActivitySource.AddActivityListener(listener);

        using var source = new ActivitySource("lawndart-w5-auth");
        using var activity = source.StartActivity("http");
        Assert.NotNull(activity);

        var http = new DefaultHttpContext();
        http.TraceIdentifier = "kestrel-request-id";

        var provider = new HttpAuthorizationContextProvider(
            new HttpContextAccessor { HttpContext = http });

        var context = await provider.GetAuthorizationContextAsync();

        Assert.NotNull(context);
        Assert.Equal(activity.TraceId.ToHexString(), context.CorrelationId);
        Assert.NotEqual(http.TraceIdentifier, context.CorrelationId);
    }
}
