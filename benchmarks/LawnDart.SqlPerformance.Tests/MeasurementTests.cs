using System.Diagnostics;

namespace LawnDart.SqlPerformance.Tests;

public sealed class MeasurementTests
{
    [Fact]
    public void Subscription_poll_throughput_counts_the_wait_only()
    {
        var operations = new OperationLog();
        operations.Start();
        Thread.Sleep(200);
        var origin = Stopwatch.GetTimestamp();
        var tenMilliseconds = Stopwatch.Frequency / 100;
        operations.Add(origin, origin + tenMilliseconds, 1);
        operations.Add(origin + Stopwatch.Frequency, origin + Stopwatch.Frequency + tenMilliseconds, 1);

        var counted = operations.Freeze("catch-up", "subscription poll", throughputFromSamples: true);
        var wall = operations.Freeze("catch-up", "subscription poll");
        var sampleSeconds = (2d * tenMilliseconds) / Stopwatch.Frequency;

        Assert.Equal(2, counted.Events);
        Assert.Equal(2d / sampleSeconds, counted.ThroughputPerSecond, 6);
        Assert.True(wall.ThroughputPerSecond < 20d);
    }
}
