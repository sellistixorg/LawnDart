using Microsoft.Extensions.Logging;

namespace LawnDart.Projections.Lightweight.Hosting;

internal static partial class LightweightProjectionSharedPipeLog
{
    [LoggerMessage(EventId = 1, Level = LogLevel.Information, Message = "Shared pipe join={Join} (live={Live}, private={Private}, threshold={Threshold})")]
    internal static partial void ClassifiedSlots(ILogger logger, long join, string live, string @private, long threshold);

    [LoggerMessage(EventId = 2, Level = LogLevel.Warning, Message = "Shared pipe pump fault; runners will poll-recover")]
    internal static partial void PumpFault(ILogger logger, Exception exception);
}
