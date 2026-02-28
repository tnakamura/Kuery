using System;
using System.Linq;
using Microsoft.Data.Sqlite;
using Xunit;

namespace Kuery.Tests.Sqlite
{
    public class QueryableGroupByCompositeKeyTest : IDisposable
    {
        readonly string _dataSource;

        public QueryableGroupByCompositeKeyTest()
        {
            var dbName = $"kuery_groupby_composite_test_{Guid.NewGuid():N}";
            _dataSource = System.IO.Path.Combine(
                AppContext.BaseDirectory,
                $"{dbName}.sqlite3");

            using (var connection = CreateConnection())
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = @"
                        CREATE TABLE OrderLine (
                            Id INTEGER PRIMARY KEY AUTOINCREMENT,
                            OrderId INTEGER NOT NULL,
                            ProductId INTEGER NOT NULL,
                            Quantity INTEGER NOT NULL,
                            UnitPrice DECIMAL NOT NULL,
                            Status INTEGER NOT NULL
                        );";
                    command.ExecuteNonQuery();
                }
            }
        }

        public void Dispose()
        {
            try
            {
                if (System.IO.File.Exists(_dataSource))
                {
                    System.IO.File.Delete(_dataSource);
                }
            }
            catch (System.IO.IOException) { }
        }

        private SqliteConnection CreateConnection()
        {
            var csb = new SqliteConnectionStringBuilder { DataSource = _dataSource };
            return new SqliteConnection(csb.ToString());
        }

        private SqliteConnection OpenNewConnection()
        {
            var connection = CreateConnection();
            connection.Open();
            return connection;
        }

        private void SeedData()
        {
            using (var connection = OpenNewConnection())
            {
                connection.Insert(new OrderLine
                {
                    OrderId = 1,
                    ProductId = 10,
                    Quantity = 2,
                    UnitPrice = 100,
                    Status = OrderLineStatus.Placed,
                });
                connection.Insert(new OrderLine
                {
                    OrderId = 1,
                    ProductId = 10,
                    Quantity = 3,
                    UnitPrice = 100,
                    Status = OrderLineStatus.Placed,
                });
                connection.Insert(new OrderLine
                {
                    OrderId = 1,
                    ProductId = 20,
                    Quantity = 1,
                    UnitPrice = 200,
                    Status = OrderLineStatus.Shipped,
                });
                connection.Insert(new OrderLine
                {
                    OrderId = 2,
                    ProductId = 10,
                    Quantity = 5,
                    UnitPrice = 100,
                    Status = OrderLineStatus.Placed,
                });
                connection.Insert(new OrderLine
                {
                    OrderId = 2,
                    ProductId = 30,
                    Quantity = 4,
                    UnitPrice = 50,
                    Status = OrderLineStatus.Placed,
                });
            }
        }

        [Fact]
        public void GroupByCompositeKeyWithCountTest()
        {
            SeedData();

            using (var connection = OpenNewConnection())
            {
                var result = connection.Query<OrderLine>()
                    .GroupBy(x => new { x.OrderId, x.ProductId })
                    .Select(g => new { OrderId = g.Key.OrderId, ProductId = g.Key.ProductId, Count = g.Count() })
                    .ToList();

                // (1,10):2, (1,20):1, (2,10):1, (2,30):1
                Assert.Equal(4, result.Count);

                var group1_10 = result.First(x => x.OrderId == 1 && x.ProductId == 10);
                Assert.Equal(2, group1_10.Count);

                var group1_20 = result.First(x => x.OrderId == 1 && x.ProductId == 20);
                Assert.Equal(1, group1_20.Count);

                var group2_10 = result.First(x => x.OrderId == 2 && x.ProductId == 10);
                Assert.Equal(1, group2_10.Count);

                var group2_30 = result.First(x => x.OrderId == 2 && x.ProductId == 30);
                Assert.Equal(1, group2_30.Count);
            }
        }

        [Fact]
        public void GroupByCompositeKeyWithSumTest()
        {
            SeedData();

            using (var connection = OpenNewConnection())
            {
                var result = connection.Query<OrderLine>()
                    .GroupBy(x => new { x.OrderId, x.ProductId })
                    .Select(g => new { OrderId = g.Key.OrderId, ProductId = g.Key.ProductId, TotalQuantity = g.Sum(x => x.Quantity) })
                    .ToList();

                Assert.Equal(4, result.Count);

                var group1_10 = result.First(x => x.OrderId == 1 && x.ProductId == 10);
                Assert.Equal(5, group1_10.TotalQuantity);  // 2 + 3

                var group2_10 = result.First(x => x.OrderId == 2 && x.ProductId == 10);
                Assert.Equal(5, group2_10.TotalQuantity);
            }
        }

        [Fact]
        public void GroupByCompositeKeyWithHavingTest()
        {
            SeedData();

            using (var connection = OpenNewConnection())
            {
                // Only groups with count > 1
                var result = connection.Query<OrderLine>()
                    .GroupBy(x => new { x.OrderId, x.ProductId })
                    .Where(g => g.Count() > 1)
                    .Select(g => new { OrderId = g.Key.OrderId, ProductId = g.Key.ProductId, Count = g.Count() })
                    .ToList();

                // Only (1,10) has 2 items
                Assert.Single(result);
                Assert.Equal(1, result[0].OrderId);
                Assert.Equal(10, result[0].ProductId);
                Assert.Equal(2, result[0].Count);
            }
        }

        [Fact]
        public void GroupByCompositeKeyWithMultipleAggregatesTest()
        {
            SeedData();

            using (var connection = OpenNewConnection())
            {
                var result = connection.Query<OrderLine>()
                    .GroupBy(x => new { x.OrderId, x.ProductId })
                    .Select(g => new
                    {
                        OrderId = g.Key.OrderId,
                        ProductId = g.Key.ProductId,
                        Count = g.Count(),
                        TotalQuantity = g.Sum(x => x.Quantity),
                    })
                    .ToList();

                Assert.Equal(4, result.Count);

                var group1_10 = result.First(x => x.OrderId == 1 && x.ProductId == 10);
                Assert.Equal(2, group1_10.Count);
                Assert.Equal(5, group1_10.TotalQuantity);
            }
        }
    }
}
