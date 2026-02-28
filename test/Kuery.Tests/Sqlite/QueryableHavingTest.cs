using System;
using System.Linq;
using Microsoft.Data.Sqlite;
using Xunit;

namespace Kuery.Tests.Sqlite
{
    public class QueryableHavingTest : IDisposable
    {
        readonly string _dataSource;

        public QueryableHavingTest()
        {
            var dbName = $"kuery_having_test_{Guid.NewGuid():N}";
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
                connection.Insert(new OrderLine
                {
                    OrderId = 3,
                    ProductId = 10,
                    Quantity = 1,
                    UnitPrice = 100,
                    Status = OrderLineStatus.Shipped,
                });
            }
        }

        [Fact]
        public void HavingCountTest()
        {
            SeedData();

            using (var connection = OpenNewConnection())
            {
                // Groups with count > 1
                var result = connection.Query<OrderLine>()
                    .GroupBy(x => x.OrderId)
                    .Where(g => g.Count() > 1)
                    .Select(g => new { OrderId = g.Key, Count = g.Count() })
                    .ToList();

                // OrderId=1 has 2 lines, OrderId=2 has 2 lines, OrderId=3 has 1 line
                Assert.Equal(2, result.Count);
                Assert.Contains(result, x => x.OrderId == 1 && x.Count == 2);
                Assert.Contains(result, x => x.OrderId == 2 && x.Count == 2);
            }
        }

        [Fact]
        public void HavingSumTest()
        {
            SeedData();

            using (var connection = OpenNewConnection())
            {
                // Groups where total quantity >= 3
                var result = connection.Query<OrderLine>()
                    .GroupBy(x => x.OrderId)
                    .Where(g => g.Sum(x => x.Quantity) >= 3)
                    .Select(g => new { OrderId = g.Key, TotalQuantity = g.Sum(x => x.Quantity) })
                    .ToList();

                // OrderId=1: Qty=2+1=3, OrderId=2: Qty=3+5=8, OrderId=3: Qty=1
                Assert.Equal(2, result.Count);
                Assert.Contains(result, x => x.OrderId == 1 && x.TotalQuantity == 3);
                Assert.Contains(result, x => x.OrderId == 2 && x.TotalQuantity == 8);
            }
        }

        [Fact]
        public void HavingWithWhereBeforeGroupByTest()
        {
            SeedData();

            using (var connection = OpenNewConnection())
            {
                // Filter to Placed status first, then group and apply having
                var result = connection.Query<OrderLine>()
                    .Where(x => x.Status == OrderLineStatus.Placed)
                    .GroupBy(x => x.OrderId)
                    .Where(g => g.Count() > 1)
                    .Select(g => new { OrderId = g.Key, Count = g.Count() })
                    .ToList();

                // After filtering Placed: OrderId=1 has 1, OrderId=2 has 2
                Assert.Single(result);
                Assert.Equal(2, result[0].OrderId);
                Assert.Equal(2, result[0].Count);
            }
        }

        [Fact]
        public void HavingCountEqualsTest()
        {
            SeedData();

            using (var connection = OpenNewConnection())
            {
                // Groups with exactly 2 items
                var result = connection.Query<OrderLine>()
                    .GroupBy(x => x.OrderId)
                    .Where(g => g.Count() == 2)
                    .Select(g => new { OrderId = g.Key, Count = g.Count() })
                    .ToList();

                Assert.Equal(2, result.Count);
                Assert.All(result, x => Assert.Equal(2, x.Count));
            }
        }

        [Fact]
        public void HavingMaxTest()
        {
            SeedData();

            using (var connection = OpenNewConnection())
            {
                // Groups where max quantity > 2
                var result = connection.Query<OrderLine>()
                    .GroupBy(x => x.OrderId)
                    .Where(g => g.Max(x => x.Quantity) > 2)
                    .Select(g => new { OrderId = g.Key, MaxQuantity = g.Max(x => x.Quantity) })
                    .ToList();

                // OrderId=1: max=2, OrderId=2: max=5, OrderId=3: max=1
                Assert.Single(result);
                Assert.Equal(2, result[0].OrderId);
                Assert.Equal(5, result[0].MaxQuantity);
            }
        }

        [Fact]
        public void HavingMinTest()
        {
            SeedData();

            using (var connection = OpenNewConnection())
            {
                // Groups where min quantity == 1
                var result = connection.Query<OrderLine>()
                    .GroupBy(x => x.OrderId)
                    .Where(g => g.Min(x => x.Quantity) == 1)
                    .Select(g => new { OrderId = g.Key, MinQuantity = g.Min(x => x.Quantity) })
                    .ToList();

                // OrderId=1: min=1, OrderId=2: min=3, OrderId=3: min=1
                Assert.Equal(2, result.Count);
                Assert.Contains(result, x => x.OrderId == 1);
                Assert.Contains(result, x => x.OrderId == 3);
            }
        }
    }
}
