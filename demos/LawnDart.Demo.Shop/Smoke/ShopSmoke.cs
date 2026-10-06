using System.Net;
using System.Net.Http.Json;
using LawnDart.Demo.Shop.Domain.Order.Commands;
using LawnDart.Demo.Shop.Domain.Product.Commands;
using LawnDart.Demo.Shop.Projections;

namespace LawnDart.Demo.Shop;

/// <summary>
/// InMemory walk: race two buys, then check ship, cancel, pay, and a repeated order id.
/// </summary>
internal static class ShopSmoke
{
    private const string BuyerTenant = "buyers";
    private const string SellerTenant = "bobs-store";

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
        var created = await CreateProduct(http, productId, "Smoke Widget", 1, "SMK-" + productId.ToString("N")[..8])
            .ConfigureAwait(false);
        if (created is not null)
            return created.Value;

        if (!await Ok(http, "/auth/login?user=alice").ConfigureAwait(false))
            return Fail("sign in as alice");

        var orderA = Guid.NewGuid();
        var orderB = Guid.NewGuid();
        var first = http.PostAsJsonAsync("/api/order/place-order", Order(orderA, productId, "Smoke Widget"));
        var second = http.PostAsJsonAsync("/api/order/place-order", Order(orderB, productId, "Smoke Widget"));
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
        var shipped = await WaitUntilShipped(http, winner).ConfigureAwait(false);
        if (shipped is not null)
            return shipped.Value;

        if (!await Ok(http, "/auth/login?user=carol").ConfigureAwait(false))
            return Fail("sign in as carol");

        var carolShip = await http.PostAsJsonAsync(
            "/api/order/ship-order",
            new ShipOrderCommand(Guid.NewGuid(), winner, "MANUAL-smoke", BuyerTenant)).ConfigureAwait(false);
        var carolBody = await carolShip.Content.ReadAsStringAsync().ConfigureAwait(false);
        if (carolShip.StatusCode != HttpStatusCode.UnprocessableEntity
            || carolBody.Contains("does not exist", StringComparison.OrdinalIgnoreCase)
            || (!carolBody.Contains("payment processed", StringComparison.OrdinalIgnoreCase)
                && !carolBody.Contains("already", StringComparison.OrdinalIgnoreCase)))
        {
            return Fail($"carol ship expected 422 about payment or already shipped, got {(int)carolShip.StatusCode} {carolBody}");
        }

        if (!await Ok(http, "/auth/login?user=bob").ConfigureAwait(false))
            return Fail("sign in as bob");

        var bobCancel = await http.PostAsJsonAsync(
            "/api/order/cancel-order",
            new CancelOrderCommand(Guid.NewGuid(), winner, "smoke cancel", BuyerTenant)).ConfigureAwait(false);
        var cancelMiss = await Expect(bobCancel, HttpStatusCode.UnprocessableEntity, "already shipped", "bob cancel")
            .ConfigureAwait(false);
        if (cancelMiss is not null)
            return cancelMiss.Value;

        if (!await Ok(http, "/auth/login?user=alice").ConfigureAwait(false))
            return Fail("sign in as alice");

        var alicePay = await http.PostAsJsonAsync(
            "/api/order/process-payment",
            new ProcessPaymentCommand(Guid.NewGuid(), winner, "PAY-SMOKE")).ConfigureAwait(false);
        var payMiss = await Expect(alicePay, HttpStatusCode.Forbidden, null, "alice pay").ConfigureAwait(false);
        if (payMiss is not null)
            return payMiss.Value;

        var duplicate = await http.PostAsJsonAsync("/api/order/place-order", Order(winner, productId, "Smoke Widget"))
            .ConfigureAwait(false);
        var duplicateMiss = await Expect(duplicate, HttpStatusCode.UnprocessableEntity, "already", "duplicate winner")
            .ConfigureAwait(false);
        if (duplicateMiss is not null)
            return duplicateMiss.Value;

        var freshOnEmpty = await http.PostAsJsonAsync(
            "/api/order/place-order",
            Order(Guid.NewGuid(), productId, "Smoke Widget")).ConfigureAwait(false);
        var stockMiss = await Expect(freshOnEmpty, HttpStatusCode.UnprocessableEntity, "stock", "fresh order on empty stock")
            .ConfigureAwait(false);
        if (stockMiss is not null)
            return stockMiss.Value;

        if (!await Ok(http, "/auth/login?user=bob").ConfigureAwait(false))
            return Fail("sign in as bob for the second product");

        var secondId = Guid.NewGuid();
        var secondCreate = await CreateProduct(http, secondId, "Smoke Pair", 2, "SMK2-" + secondId.ToString("N")[..8])
            .ConfigureAwait(false);
        if (secondCreate is not null)
            return secondCreate.Value;

        if (!await Ok(http, "/auth/login?user=alice").ConfigureAwait(false))
            return Fail("sign in as alice for the second product");

        var kept = Guid.NewGuid();
        var placed = await http.PostAsJsonAsync("/api/order/place-order", Order(kept, secondId, "Smoke Pair"))
            .ConfigureAwait(false);
        if (placed.StatusCode != HttpStatusCode.Accepted)
            return Fail($"second product order {(int)placed.StatusCode} {await placed.Content.ReadAsStringAsync().ConfigureAwait(false)}");

        var stockAfterPlace = await WaitForStock(http, secondId, 1).ConfigureAwait(false);
        if (stockAfterPlace is not null)
            return stockAfterPlace.Value;

        var replay = await http.PostAsJsonAsync("/api/order/place-order", Order(kept, secondId, "Smoke Pair"))
            .ConfigureAwait(false);
        var replayMiss = await Expect(replay, HttpStatusCode.UnprocessableEntity, "already", "duplicate on second product")
            .ConfigureAwait(false);
        if (replayMiss is not null)
            return replayMiss.Value;

        await Task.Delay(1000).ConfigureAwait(false);
        var stockAfterReplay = await WaitForStock(http, secondId, 1).ConfigureAwait(false);
        if (stockAfterReplay is not null)
            return stockAfterReplay.Value;

        Console.WriteLine($"Shop smoke ok. Order {winner:N} shipped. The other reservation lost. Repeat, pay, and stock checks passed.");
        return 0;
    }

    private static PlaceOrderCommand Order(Guid orderId, Guid productId, string name)
        => new(Guid.NewGuid(), orderId, productId, name, 1, 9.50m, SellerTenant, "Bob");

    private static async Task<int?> CreateProduct(HttpClient http, Guid productId, string name, int stock, string sku)
    {
        var create = await http.PostAsJsonAsync(
            "/api/product/create-product",
            new CreateProductCommand(
                Guid.NewGuid(),
                productId,
                name,
                "Smoke stock.",
                9.50m,
                stock,
                sku,
                SellerTenant,
                "Bob")).ConfigureAwait(false);

        if (create.StatusCode == HttpStatusCode.Accepted)
            return null;

        return Fail($"create product {(int)create.StatusCode} {await create.Content.ReadAsStringAsync().ConfigureAwait(false)}");
    }

    private static async Task<int?> WaitUntilShipped(HttpClient http, Guid winner)
    {
        var deadline = DateTime.UtcNow.AddSeconds(15);
        while (DateTime.UtcNow < deadline)
        {
            var orders = await http.GetAsync("/api/views/orders").ConfigureAwait(false);
            if (orders.IsSuccessStatusCode)
            {
                var list = await orders.Content.ReadFromJsonAsync<List<OrderSummaryView>>().ConfigureAwait(false) ?? [];
                if (list.Any(o => o.OrderId == winner && o.Status == "Shipped"))
                    return null;
            }

            await Task.Delay(200).ConfigureAwait(false);
        }

        return Fail($"order {winner:N} did not reach Shipped");
    }

    private static async Task<int?> WaitForStock(HttpClient http, Guid productId, int expected)
    {
        var deadline = DateTime.UtcNow.AddSeconds(10);
        string? last = null;
        while (DateTime.UtcNow < deadline)
        {
            var response = await http.GetAsync("/api/views/products").ConfigureAwait(false);
            if (response.IsSuccessStatusCode)
            {
                var products = await response.Content.ReadFromJsonAsync<List<ProductCatalogView>>().ConfigureAwait(false) ?? [];
                var match = products.FirstOrDefault(p => p.ProductId == productId);
                if (match is not null && match.Stock == expected)
                    return null;
                last = match is null ? "missing" : match.Stock.ToString();
            }

            await Task.Delay(200).ConfigureAwait(false);
        }

        return Fail($"product {productId:N} stock stayed {last}, expected {expected}");
    }

    private static async Task<bool> Ok(HttpClient http, string path)
    {
        var response = await http.GetAsync(path).ConfigureAwait(false);
        return response.IsSuccessStatusCode;
    }

    private static async Task<int?> Expect(HttpResponseMessage response, HttpStatusCode status, string? bodyContains, string label)
    {
        var body = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
        if (response.StatusCode != status)
            return Fail($"{label} expected {(int)status}, got {(int)response.StatusCode} {body}");
        if (bodyContains is not null && !body.Contains(bodyContains, StringComparison.OrdinalIgnoreCase))
            return Fail($"{label} body missing '{bodyContains}': {body}");
        return null;
    }

    private static int Fail(string message)
    {
        Console.Error.WriteLine("Shop smoke failed: " + message);
        return 1;
    }
}
