using System;
using System.Linq;
using Xunit;

namespace Kuery.Tests.MySql
{
    public class SqlHelperTest : IClassFixture<MySqlFixture>
    {
        readonly MySqlFixture fixture;

        public SqlHelperTest(MySqlFixture fixture)
        {
            this.fixture = fixture;
        }

        [Fact]
        public void StartsWithIgnoreCaseUsesLike()
        {
            using var connection = fixture.OpenNewConnection();
            connection.DropTable(nameof(Product));
            connection.Execute($@"create table `{nameof(Product)}` (
                `{nameof(Product.Id)}` integer primary key auto_increment,
                `{nameof(Product.Name)}` varchar(50) not null,
                `{nameof(Product.Price)}` decimal(18,2) not null,
                `{nameof(Product.TotalSales)}` int not null
            );");

            connection.Insert(new Product { Name = "Apple", Price = 1, TotalSales = 10 });
            connection.Insert(new Product { Name = "banana", Price = 1, TotalSales = 10 });

            var result = connection.Table<Product>()
                .Where(x => x.Name.StartsWith("ap", StringComparison.OrdinalIgnoreCase))
                .ToList();

            Assert.Single(result);
            Assert.Equal("Apple", result[0].Name);
        }

        [Fact]
        public void EndsWithIgnoreCaseUsesLike()
        {
            using var connection = fixture.OpenNewConnection();
            connection.DropTable(nameof(Product));
            connection.Execute($@"create table `{nameof(Product)}` (
                `{nameof(Product.Id)}` integer primary key auto_increment,
                `{nameof(Product.Name)}` varchar(50) not null,
                `{nameof(Product.Price)}` decimal(18,2) not null,
                `{nameof(Product.TotalSales)}` int not null
            );");

            connection.Insert(new Product { Name = "Apple", Price = 1, TotalSales = 10 });
            connection.Insert(new Product { Name = "banana", Price = 1, TotalSales = 10 });

            var result = connection.Table<Product>()
                .Where(x => x.Name.EndsWith("NA", StringComparison.OrdinalIgnoreCase))
                .ToList();

            Assert.Single(result);
            Assert.Equal("banana", result[0].Name);
        }
    }
}
