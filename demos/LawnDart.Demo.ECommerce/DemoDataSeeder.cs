using LawnDart;
using LawnDart.Aggregates;
using LawnDart.Metadata;
using LawnDart.Demo.ECommerce.Domain.Product;
using LawnDart.Demo.ECommerce.Domain.Product.Commands;

namespace LawnDart.Demo.ECommerce;

public class DemoDataSeeder
{
    private readonly IAggregateRepository _repository;
    private readonly IMetadataProvider _metadataProvider;

    public DemoDataSeeder(IAggregateRepository repository, IMetadataProvider metadataProvider)
    {
        _repository = repository;
        _metadataProvider = metadataProvider;
    }

    public async Task<List<Product>> SeedProductsAsync(CancellationToken cancellationToken = default)
    {
        var productData = new[]
        {
            ("Laptop", "LAP-001", 999.99m, 10),
            ("Mouse", "MOU-001", 29.99m, 50),
            ("Keyboard", "KEY-001", 79.99m, 30),
            ("Monitor", "MON-001", 299.99m, 15),
            ("Headphones", "HEA-001", 149.99m, 25)
        };

        var seededProducts = new List<Product>();
        
        for (int i = 0; i < productData.Length; i++)
        {
            var (name, sku, price, inventory) = productData[i];
            var productId = Guid.NewGuid();

            // Get or create aggregate (loads from event store if exists, creates if not)
            var product = await _repository.GetOrCreateAsync<Product>(productId, cancellationToken);

            var command = new CreateProductCommand(
                Guid.NewGuid(),
                productId,
                name,
                sku,
                price,
                inventory);

            var commandMetadata = _metadataProvider.CaptureCommandMetadata();
            // TenantId is auto-populated from ITenantContextProvider

            await _repository.HandleCommandAsync(product, command, commandMetadata, cancellationToken);
            
            seededProducts.Add(product);
        }

        return seededProducts;
    }
}

