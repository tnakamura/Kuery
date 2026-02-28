using System;
using System.Linq;
using Microsoft.Data.Sqlite;
using Xunit;

namespace Kuery.Tests.Sqlite
{
    public class QueryableGroupByTest : IDisposable
    {
        readonly string _dataSource;

        public QueryableGroupByTest()
        {
            var dbName = $"kuery_groupby_test_{Guid.NewGuid():N}";
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
                    ProductId = 20,
                    Quantity = 1,
                    UnitPrice = 200,
                    Status = OrderLineStatus.Shipped,
                });
                connection.Insert(new OrderLine
                {
                    OrderId = 2,
                    ProductId = 10,
                    Quantity = 3,
                    UnitPrice = 100,
                    Status = OrderLineStatus.Placed,
                });
                connection.Insert(new OrderLine
                {
                    OrderId = 2,
                    ProductId = 30,
                    Quantity = 5,
                    UnitPrice = 50,
                    Status = OrderLineStatus.Placed,
                });
            }
        }

        [Fact]
        public void GroupByWithCountTest()
        {
            SeedData();

            using (var connection = OpenNewConnection())
            {
                var result = connection.Query<OrderLine>()
                    .GroupBy(x => x.OrderId)
                    .Select(g => new { OrderId = g.Key, Count = g.Count() })
                    .ToList();

                Assert.Equal(2, result.Count);

                var order1 = result.First(x => x.OrderId == 1);
                Assert.Equal(2, order1.Count);

                var order2 = result.First(x => x.OrderId == 2);
                Assert.Equal(2, order2.Count);
            }
        }

        [Fact]
        public void GroupByWithSumTest()
        {
            SeedData();

            using (var connection = OpenNewConnection())
            {
                var result = connection.Query<OrderLine>()
                    .GroupBy(x => x.OrderId)
                    .Select(g => new { OrderId = g.Key, TotalQuantity = g.Sum(x => x.Quantity) })
                    .ToList();

                Assert.Equal(2, result.Count);

                var order1 = result.First(x => x.OrderId == 1);
                Assert.Equal(3, order1.TotalQuantity);

                var order2 = result.First(x => x.OrderId == 2);
                Assert.Equal(8, order2.TotalQuantity);
            }
        }

        [Fact]
        public void GroupByWithMinTest()
        {
            SeedData();

            using (var connection = OpenNewConnection())
            {
                var result = connection.Query<OrderLine>()
                    .GroupBy(x => x.OrderId)
                    .Select(g => new { OrderId = g.Key, MinQuantity = g.Min(x => x.Quantity) })
                    .ToList();

                Assert.Equal(2, result.Count);

                var order1 = result.First(x => x.OrderId == 1);
                Assert.Equal(1, order1.MinQuantity);

                var order2 = result.First(x => x.OrderId == 2);
                Assert.Equal(3, order2.MinQuantity);
            }
        }

        [Fact]
        public void GroupByWithMaxTest()
        {
            SeedData();

            using (var connection = OpenNewConnection())
            {
                var result = connection.Query<OrderLine>()
                    .GroupBy(x => x.OrderId)
                    .Select(g => new { OrderId = g.Key, MaxQuantity = g.Max(x => x.Quantity) })
                    .ToList();

                Assert.Equal(2, result.Count);

                var order1 = result.First(x => x.OrderId == 1);
                Assert.Equal(2, order1.MaxQuantity);

                var order2 = result.First(x => x.OrderId == 2);
                Assert.Equal(5, order2.MaxQuantity);
            }
        }

        [Fact]
        public void GroupByWithWhereBeforeGroupByTest()
        {
            SeedData();

            using (var connection = OpenNewConnection())
            {
                var result = connection.Query<OrderLine>()
                    .Where(x => x.Status == OrderLineStatus.Placed)
                    .GroupBy(x => x.OrderId)
                    .Select(g => new { OrderId = g.Key, Count = g.Count() })
                    .ToList();

                Assert.Equal(2, result.Count);

                var order1 = result.First(x => x.OrderId == 1);
                Assert.Equal(1, order1.Count);

                var order2 = result.First(x => x.OrderId == 2);
                Assert.Equal(2, order2.Count);
            }
        }

        [Fact]
        public void GroupByWithMultipleAggregatesTest()
        {
            SeedData();

            using (var connection = OpenNewConnection())
            {
                var result = connection.Query<OrderLine>()
                    .GroupBy(x => x.ProductId)
                    .Select(g => new
                    {
                        ProductId = g.Key,
                        Count = g.Count(),
                        TotalQuantity = g.Sum(x => x.Quantity),
                    })
                    .ToList();

                Assert.Equal(3, result.Count);

                var product10 = result.First(x => x.ProductId == 10);
                Assert.Equal(2, product10.Count);
                Assert.Equal(5, product10.TotalQuantity);

                var product20 = result.First(x => x.ProductId == 20);
                Assert.Equal(1, product20.Count);
                Assert.Equal(1, product20.TotalQuantity);

                var product30 = result.First(x => x.ProductId == 30);
                Assert.Equal(1, product30.Count);
                Assert.Equal(5, product30.TotalQuantity);
            }
        }

        [Fact]
        public void GroupByFirstTest()
        {
            SeedData();

            using (var connection = OpenNewConnection())
            {
                var result = connection.Query<OrderLine>()
                    .GroupBy(x => x.OrderId)
                    .Select(g => new { OrderId = g.Key, Count = g.Count() })
                    .First();

                Assert.True(result.OrderId == 1 || result.OrderId == 2);
                Assert.Equal(2, result.Count);
            }
        }

        [Fact]
        public void GroupByFirstOrDefaultEmptyTest()
        {
            using (var connection = OpenNewConnection())
            {
                var result = connection.Query<OrderLine>()
                    .GroupBy(x => x.OrderId)
                    .Select(g => new { OrderId = g.Key, Count = g.Count() })
                    .FirstOrDefault();

                Assert.Null(result);
            }
        }
    }
}
