using System.Diagnostics.Metrics;
using LawnDart.Projections.Telemetry;

namespace LawnDart.Projections.Lightweight.Tests.Unit;

public class ProjectionTelemetryOptionsTests : IDisposable
{
    private readonly ProjectionTelemetryOptions _previous;

    public ProjectionTelemetryOptionsTests()
    {
        _previous = Clone(ProjectionTelemetry.Options);
    }

    public void Dispose() => ProjectionTelemetry.Configure(_previous);

    [Fact]
    public void Configure_UpdatesProcessWideOptions()
    {
        ProjectionTelemetry.Configure(new ProjectionTelemetryOptions
        {
            EnableCacheMetrics = false,
            EnableFlushMetrics = true,
            EnableExtendedMetrics = false,
            EnableHighDetailTags = true
        });

        Assert.False(ProjectionTelemetry.Options.EnableCacheMetrics);
        Assert.True(ProjectionTelemetry.Options.EnableFlushMetrics);
        Assert.False(ProjectionTelemetry.Options.EnableExtendedMetrics);
        Assert.True(ProjectionTelemetry.Options.EnableHighDetailTags);
    }

    [Fact]
    public void RecordRead_WhenCacheMetricsEnabled_IncrementsInstruments()
    {
        ProjectionTelemetry.Configure(new ProjectionTelemetryOptions { EnableCacheMetrics = true });

        using var listener = CreateListener(
            "projections.read.total",
            "projections.read.cache.hit",
            "projections.read.cache.miss");

        ProjectionTelemetry.RecordRead("Counter:v1", cacheHit: true, readSource: "memory");
        ProjectionTelemetry.RecordRead("Counter:v1", cacheHit: false, readSource: "sql");

        Assert.Equal(2, listener.GetSum("projections.read.total"));
        Assert.Equal(1, listener.GetSum("projections.read.cache.hit"));
        Assert.Equal(1, listener.GetSum("projections.read.cache.miss"));
    }

    [Fact]
    public void RecordRead_WhenCacheMetricsDisabled_IsSilent()
    {
        ProjectionTelemetry.Configure(new ProjectionTelemetryOptions { EnableCacheMetrics = false });

        using var listener = CreateListener(
            "projections.read.total",
            "projections.read.cache.hit",
            "projections.read.cache.miss");

        ProjectionTelemetry.RecordRead("Counter:v1", cacheHit: true, readSource: "memory");
        ProjectionTelemetry.RecordRead("Counter:v1", cacheHit: false, readSource: "sql");

        Assert.Equal(0, listener.GetSum("projections.read.total"));
        Assert.Equal(0, listener.GetSum("projections.read.cache.hit"));
        Assert.Equal(0, listener.GetSum("projections.read.cache.miss"));
    }

    [Fact]
    public void RecordFlush_WhenFlushMetricsEnabled_RecordsDurationAndViews()
    {
        ProjectionTelemetry.Configure(new ProjectionTelemetryOptions { EnableFlushMetrics = true });

        using var listener = CreateListener("projections.flush.duration", "projections.flush.views");

        ProjectionTelemetry.RecordFlush("Counter:v1", durationMs: 12.5, viewCount: 3);

        Assert.True(listener.GetSum("projections.flush.duration") > 0);
        Assert.Equal(3, listener.GetSum("projections.flush.views"));
    }

    [Fact]
    public void RecordFlush_WhenFlushMetricsDisabled_IsSilent()
    {
        ProjectionTelemetry.Configure(new ProjectionTelemetryOptions { EnableFlushMetrics = false });

        using var listener = CreateListener(
            "projections.flush.duration",
            "projections.flush.views",
            "projections.flush.errors");

        ProjectionTelemetry.RecordFlush("Counter:v1", durationMs: 9, viewCount: 2);
        ProjectionTelemetry.RecordFlushError("Counter:v1");

        Assert.Equal(0, listener.GetSum("projections.flush.duration"));
        Assert.Equal(0, listener.GetSum("projections.flush.views"));
        Assert.Equal(0, listener.GetSum("projections.flush.errors"));
    }

    [Fact]
    public void RecordStaleRejected_RespectsExtendedMetricsFlag()
    {
        ProjectionTelemetry.Configure(new ProjectionTelemetryOptions { EnableExtendedMetrics = true });
        using (var on = CreateListener("projections.read.stale_rejected"))
        {
            ProjectionTelemetry.RecordStaleRejected("Counter:v1");
            Assert.Equal(1, on.GetSum("projections.read.stale_rejected"));
        }

        ProjectionTelemetry.Configure(new ProjectionTelemetryOptions { EnableExtendedMetrics = false });
        using var off = CreateListener("projections.read.stale_rejected");
        ProjectionTelemetry.RecordStaleRejected("Counter:v1");
        Assert.Equal(0, off.GetSum("projections.read.stale_rejected"));
    }

    [Fact]
    public void RecordWorkingSetEviction_RespectsExtendedMetricsFlag()
    {
        ProjectionTelemetry.Configure(new ProjectionTelemetryOptions { EnableExtendedMetrics = true });
        using (var on = CreateListener("projections.workingset.evictions"))
        {
            ProjectionTelemetry.RecordWorkingSetEviction("Counter:v1", 4);
            Assert.Equal(4, on.GetSum("projections.workingset.evictions"));
        }

        ProjectionTelemetry.Configure(new ProjectionTelemetryOptions { EnableExtendedMetrics = false });
        using var off = CreateListener("projections.workingset.evictions");
        ProjectionTelemetry.RecordWorkingSetEviction("Counter:v1", 4);
        Assert.Equal(0, off.GetSum("projections.workingset.evictions"));
    }

    private static MetricSumListener CreateListener(params string[] instrumentNames) =>
        new(instrumentNames);

    private static ProjectionTelemetryOptions Clone(ProjectionTelemetryOptions o) => new()
    {
        EnableCacheMetrics = o.EnableCacheMetrics,
        EnableFlushMetrics = o.EnableFlushMetrics,
        EnableExtendedMetrics = o.EnableExtendedMetrics,
        EnableHighDetailTags = o.EnableHighDetailTags
    };

    private sealed class MetricSumListener : IDisposable
    {
        private readonly MeterListener _listener = new();
        private readonly Dictionary<string, long> _sums = new(StringComparer.Ordinal);
        private readonly HashSet<string> _names;

        public MetricSumListener(IEnumerable<string> instrumentNames)
        {
            _names = new HashSet<string>(instrumentNames, StringComparer.Ordinal);
            foreach (var name in _names)
                _sums[name] = 0;

            _listener.InstrumentPublished = (instrument, listener) =>
            {
                if (instrument.Meter.Name == ProjectionTelemetry.MeterName
                    && _names.Contains(instrument.Name))
                {
                    listener.EnableMeasurementEvents(instrument);
                }
            };

            _listener.SetMeasurementEventCallback<long>((inst, measurement, _, _) =>
            {
                if (_sums.ContainsKey(inst.Name))
                    _sums[inst.Name] += measurement;
            });
            _listener.SetMeasurementEventCallback<int>((inst, measurement, _, _) =>
            {
                if (_sums.ContainsKey(inst.Name))
                    _sums[inst.Name] += measurement;
            });
            _listener.SetMeasurementEventCallback<double>((inst, measurement, _, _) =>
            {
                if (_sums.ContainsKey(inst.Name))
                    _sums[inst.Name] += (long)Math.Ceiling(measurement);
            });

            _listener.Start();
        }

        public long GetSum(string instrumentName) =>
            _sums.TryGetValue(instrumentName, out var v) ? v : 0;

        public void Dispose() => _listener.Dispose();
    }
}
