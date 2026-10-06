using LawnDart;
using LawnDart.Metadata;

namespace LawnDart.Demo.ECommerce.Dcb;

/// <summary>
/// Demo data seeder using DCB approach.
/// Creates products using tag-based events instead of aggregates.
/// </summary>
public class DcbDemoDataSeeder
{
    private readonly DcbProductService _productService;
    private readonly IMetadataProvider _metadataProvider;

    public DcbDemoDataSeeder(DcbProductService productService, IMetadataProvider metadataProvider)
    {
        _productService = productService;
        _metadataProvider = metadataProvider;
    }

    public async Task<List<Guid>> SeedProductsAsync(CancellationToken cancellationToken = default)
    {
        var productData = new[]
        {
            ("Laptop", "LAP-001", 999.99m, 10),
            ("Mouse", "MOU-001", 29.99m, 50),
            ("Keyboard", "KEY-001", 79.99m, 30),
            ("Monitor", "MON-001", 299.99m, 15),
            ("Headphones", "HEA-001", 149.99m, 25)
        };

        var productIds = new List<Guid>();
        var commandMetadata = _metadataProvider.CaptureCommandMetadata();
        // TenantId is auto-populated from ITenantContextProvider
        commandMetadata.UserId = "system";

        for (int i = 0; i < productData.Length; i++)
        {
            var (name, sku, price, inventory) = productData[i];
            var productId = Guid.NewGuid();

            await _productService.CreateProductAsync(productId, name, sku, price, inventory);
            productIds.Add(productId);
        }

        return productIds;
    }
}

