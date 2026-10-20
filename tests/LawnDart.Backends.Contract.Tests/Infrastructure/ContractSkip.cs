using Xunit.Abstractions;
using Xunit.Sdk;

namespace LawnDart.Backends.Contract.Tests;

/// <summary>
/// Skips the current test with a reason a person can read.
/// The test result is skipped, not passed and not failed.
/// </summary>
public static class ContractSkip
{
    public static void Unless(bool available, string reason)
    {
        if (!available)
            throw new ContractSkipException(reason);
    }
}

public sealed class ContractSkipException : Exception
{
    public ContractSkipException(string reason)
        : base(reason)
    {
    }
}

[XunitTestCaseDiscoverer(
    "LawnDart.Backends.Contract.Tests.ContractFactDiscoverer",
    "LawnDart.Backends.Contract.Tests")]
[AttributeUsage(AttributeTargets.Method)]
public sealed class ContractFactAttribute : FactAttribute;

public sealed class ContractFactDiscoverer : IXunitTestCaseDiscoverer
{
    private readonly IMessageSink _diagnosticMessageSink;

    public ContractFactDiscoverer(IMessageSink diagnosticMessageSink)
        => _diagnosticMessageSink = diagnosticMessageSink;

    public IEnumerable<IXunitTestCase> Discover(
        ITestFrameworkDiscoveryOptions discoveryOptions,
        ITestMethod testMethod,
        IAttributeInfo factAttribute)
    {
        yield return new ContractTestCase(
            _diagnosticMessageSink,
            discoveryOptions.MethodDisplayOrDefault(),
            discoveryOptions.MethodDisplayOptionsOrDefault(),
            testMethod);
    }
}

public sealed class ContractTestCase : XunitTestCase
{
    [Obsolete("Called by the de-serializer", true)]
    public ContractTestCase()
    {
    }

    public ContractTestCase(
        IMessageSink diagnosticMessageSink,
        TestMethodDisplay defaultMethodDisplay,
        TestMethodDisplayOptions defaultMethodDisplayOptions,
        ITestMethod testMethod,
        object[]? testMethodArguments = null)
        : base(diagnosticMessageSink, defaultMethodDisplay, defaultMethodDisplayOptions, testMethod, testMethodArguments)
    {
    }

    public override async Task<RunSummary> RunAsync(
        IMessageSink diagnosticMessageSink,
        IMessageBus messageBus,
        object[] constructorArguments,
        ExceptionAggregator aggregator,
        CancellationTokenSource cancellationTokenSource)
    {
        var bus = new ContractSkipMessageBus(messageBus);
        var summary = await base.RunAsync(
            diagnosticMessageSink,
            bus,
            constructorArguments,
            aggregator,
            cancellationTokenSource).ConfigureAwait(false);

        if (bus.DynamicallySkippedTestCount > 0)
        {
            summary.Failed -= bus.DynamicallySkippedTestCount;
            summary.Skipped += bus.DynamicallySkippedTestCount;
        }

        return summary;
    }
}

sealed class ContractSkipMessageBus : IMessageBus
{
    private readonly IMessageBus _inner;

    public ContractSkipMessageBus(IMessageBus inner) => _inner = inner;

    public int DynamicallySkippedTestCount { get; private set; }

    public bool QueueMessage(IMessageSinkMessage message)
    {
        if (message is ITestFailed failed
            && failed.ExceptionTypes?.Any(type => type == typeof(ContractSkipException).FullName) == true)
        {
            DynamicallySkippedTestCount++;
            var reason = failed.Messages?.FirstOrDefault() ?? "Skipped.";
            return _inner.QueueMessage(new TestSkipped(failed.Test, reason));
        }

        return _inner.QueueMessage(message);
    }

    public void Dispose() => _inner.Dispose();
}
