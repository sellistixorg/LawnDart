using System.Net;
using System.Net.Http.Json;
using LawnDart.Demo.Shop.Domain.Order.Commands;
using LawnDart.Demo.Shop.Domain.Product.Commands;
using LawnDart.Demo.Shop.Projections;

namespace LawnDart.Demo.Shop;

/// <summary>
/// InMemory walk: sign in, create one unit of stock, race two buys, wait until the winner ships.
/// </summary>
internal static class ShopSmoke
{
    public static async Task<int> RunAsync(string baseUrl)
    {
        using var handler = new HttpClientHandler
        {
            UseCookies = true,
            CookieContainer = new System.Net.CookieContainer(),
            AllowAutoRedirect = true
        };
        using var http = new HttpClient(handler) { BaseAddress = new Uri(baseUrl) };

        if (!await Ok(http, "/health").ConfigureAwait(false))
            return Fail("GET /health");

        if (!await Ok(http, "/login").ConfigureAwait(false))
            return Fail("GET /login");

        if (!await Ok(http, "/auth/login?user=bob").ConfigureAwait(false))
            return Fail("sign in as bob");

        var productId = Guid.NewGuid();
        var create = await http.PostAsJsonAsync(
            "/api/product/create-product",
            new CreateProductCommand(
                Guid.NewGuid(),
                productId,
                "Smoke Widget",
                "One unit for the DCB race.",
                9.50m,
                1,
                "SMK-" + productId.ToString("N")[..8],
                "bob",
                "Bob")).ConfigureAwait(false);

        if (create.StatusCode != HttpStatusCode.Accepted)
            return Fail($"create product {(int)create.StatusCode} {await create.Content.ReadAsStringAsync().ConfigureAwait(false)}");

        if (!await Ok(http, "/auth/login?user=alice").ConfigureAwait(false))
            return Fail("sign in as alice");

        var orderA = Guid.NewGuid();
        var orderB = Guid.NewGuid();
        var first = http.PostAsJsonAsync("/api/order/place-order", Order(orderA, productId));
        var second = http.PostAsJsonAsync("/api/order/place-order", Order(orderB, productId));
        await Task.WhenAll(first, second).ConfigureAwait(false);
        var left = await first.ConfigureAwait(false);
        var right = await second.ConfigureAwait(false);
        var wins = (left.StatusCode == HttpStatusCode.Accepted ? 1 : 0)
            + (right.StatusCode == HttpStatusCode.Accepted ? 1 : 0);
        if (wins != 1)
        {
            return Fail(
                $"expected one reservation to win, got {wins}. " +
                $"{(int)left.StatusCode} {await left.Content.ReadAsStringAsync().ConfigureAwait(false)} " +
                $"{(int)right.StatusCode} {await right.Content.ReadAsStringAsync().ConfigureAwait(false)}");
        }

        var winner = left.StatusCode == HttpStatusCode.Accepted ? orderA : orderB;
        var deadline = DateTime.UtcNow.AddSeconds(15);
        while (DateTime.UtcNow < deadline)
        {
            var orders = await http.GetAsync("/api/views/orders").ConfigureAwait(false);
            if (orders.IsSuccessStatusCode)
            {
                var list = await orders.Content.ReadFromJsonAsync<List<OrderSummaryView>>().ConfigureAwait(false) ?? [];
                var mine = list.FirstOrDefault(o => o.OrderId == winner);
                if (mine?.Status == "Shipped")
                {
                    Console.WriteLine($"Shop smoke ok. Order {winner:N} shipped. The other reservation lost.");
                    return 0;
                }
            }

            await Task.Delay(200).ConfigureAwait(false);
        }

        return Fail($"order {winner:N} did not reach Shipped");
    }

    private static PlaceOrderCommand Order(Guid orderId, Guid productId)
        => new(Guid.NewGuid(), orderId, productId, "Smoke Widget", 1, 9.50m, "bob", "Bob");

    private static async Task<bool> Ok(HttpClient http, string path)
    {
        var response = await http.GetAsync(path).ConfigureAwait(false);
        return response.IsSuccessStatusCode;
    }

    private static int Fail(string message)
    {
        Console.Error.WriteLine("Shop smoke failed: " + message);
        return 1;
    }
}
