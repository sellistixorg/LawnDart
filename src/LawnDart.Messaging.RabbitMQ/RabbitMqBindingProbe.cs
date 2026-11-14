using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;

namespace LawnDart.Messaging.RabbitMQ;

/// <summary>
/// Reads whether a queue is bound to a routing key.
/// Uses the broker's binding list. A missing list returns null so the caller
/// does not treat a failed read as "no queue".
/// </summary>
internal static class RabbitMqBindingProbe
{
    private const int ManagementPort = 15672;

    public static async Task<bool?> QueueIsBoundAsync(
        RabbitMqTransportOptions options,
        string exchange,
        string routingKey,
        CancellationToken cancellationToken)
    {
        var factory = RabbitMqConnector.CreateFactory(options);
        var host = factory.HostName;
        var user = factory.UserName ?? string.Empty;
        var password = factory.Password ?? string.Empty;
        var vhost = string.IsNullOrEmpty(factory.VirtualHost) ? "/" : factory.VirtualHost;
        var url =
            "http://" + host + ":" + ManagementPort.ToString(System.Globalization.CultureInfo.InvariantCulture) +
            "/api/exchanges/" + Uri.EscapeDataString(vhost) + "/" + Uri.EscapeDataString(exchange) + "/bindings/source";

        using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(2) };
        using var request = new HttpRequestMessage(HttpMethod.Get, url);
        var token = Convert.ToBase64String(Encoding.UTF8.GetBytes(user + ":" + password));
        request.Headers.Authorization = new AuthenticationHeaderValue("Basic", token);

        HttpResponseMessage response;
        try
        {
            response = await http.SendAsync(request, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is HttpRequestException or TaskCanceledException)
        {
            return null;
        }

        using (response)
        {
            if (response.StatusCode == System.Net.HttpStatusCode.NotFound)
                return false;
            if (!response.IsSuccessStatusCode)
                return null;

            var json = await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
            return BindingListHasQueue(json, routingKey);
        }
    }

    /// <summary>True when the binding list names a queue for <paramref name="routingKey"/>.</summary>
    internal static bool BindingListHasQueue(string json, string routingKey)
    {
        if (string.IsNullOrWhiteSpace(json))
            return false;

        using var document = JsonDocument.Parse(json);
        if (document.RootElement.ValueKind != JsonValueKind.Array)
            return false;

        foreach (var binding in document.RootElement.EnumerateArray())
        {
            if (!binding.TryGetProperty("destination_type", out var destinationType) ||
                !string.Equals(destinationType.GetString(), "queue", StringComparison.Ordinal))
            {
                continue;
            }

            if (binding.TryGetProperty("routing_key", out var key) &&
                string.Equals(key.GetString(), routingKey, StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }
}
