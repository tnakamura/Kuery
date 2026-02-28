using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using Xunit;

namespace Kuery.Tests.Sqlite
{
    public class QueryableJoinTest : IDisposable
    {
        readonly string _dataSource;

        public QueryableJoinTest()
        {
            var dbName = $"kuery_join_test_{Guid.NewGuid():N}";
            _dataSource = System.IO.Path.Combine(
                AppContext.BaseDirectory,
                $"{dbName}.sqlite3");

            using (var connection = CreateConnection())
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = @"
                        CREATE TABLE [Order] (
                            Id INTEGER PRIMARY KEY AUTOINCREMENT,
                            PlacedTime DATETIME NOT NULL
                        );
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
                connection.Insert(new Order
                {
                    PlacedTime = new DateTime(2025, 1, 1),
                });
                connection.Insert(new Order
                {
                    PlacedTime = new DateTime(2025, 2, 1),
                });

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
            }
        }

        [Fact]
        public void JoinToListTest()
        {
            SeedData();

            using (var connection = OpenNewConnection())
            {
                var result = connection.Query<Order>()
                    .Join(
                        connection.Query<OrderLine>(),
                        o => o.Id,
                        ol => ol.OrderId,
                        (o, ol) => new { o.Id, o.PlacedTime, ol.ProductId, ol.Quantity })
                    .ToList();

                Assert.Equal(3, result.Count);
            }
        }

        [Fact]
        public void JoinWithWhereBeforeJoinTest()
        {
            SeedData();

            using (var connection = OpenNewConnection())
            {
                var result = connection.Query<Order>()
                    .Where(o => o.Id == 1)
                    .Join(
                        connection.Query<OrderLine>(),
                        o => o.Id,
                        ol => ol.OrderId,
                        (o, ol) => new { o.Id, ol.ProductId, ol.Quantity })
                    .ToList();

                Assert.Equal(2, result.Count);
                Assert.All(result, r => Assert.Equal(1, r.Id));
            }
        }

        [Fact]
        public void JoinFirstTest()
        {
            SeedData();

            using (var connection = OpenNewConnection())
            {
                var result = connection.Query<Order>()
                    .Join(
                        connection.Query<OrderLine>(),
                        o => o.Id,
                        ol => ol.OrderId,
                        (o, ol) => new { o.Id, ol.ProductId, ol.Quantity })
                    .First();

                Assert.NotNull(result);
                Assert.Equal(1, result.Id);
            }
        }

        [Fact]
        public void JoinFirstOrDefaultEmptyTest()
        {
            // No data seeded, so join yields no rows
            using (var connection = OpenNewConnection())
            {
                var result = connection.Query<Order>()
                    .Join(
                        connection.Query<OrderLine>(),
                        o => o.Id,
                        ol => ol.OrderId,
                        (o, ol) => new { o.Id, ol.ProductId })
                    .FirstOrDefault();

                Assert.Null(result);
            }
        }

        [Fact]
        public void JoinCountTest()
        {
            SeedData();

            using (var connection = OpenNewConnection())
            {
                var count = connection.Query<Order>()
                    .Join(
                        connection.Query<OrderLine>(),
                        o => o.Id,
                        ol => ol.OrderId,
                        (o, ol) => new { o.Id, ol.ProductId })
                    .Count();

                Assert.Equal(3, count);
            }
        }

        [Fact]
        public void JoinValuesAreCorrectTest()
        {
            SeedData();

            using (var connection = OpenNewConnection())
            {
                var result = connection.Query<Order>()
                    .Join(
                        connection.Query<OrderLine>(),
                        o => o.Id,
                        ol => ol.OrderId,
                        (o, ol) => new
                        {
                            OrderId = o.Id,
                            o.PlacedTime,
                            ol.ProductId,
                            ol.Quantity,
                            ol.UnitPrice,
                        })
                    .ToList();

                Assert.Equal(3, result.Count);

                var first = result.First(r => r.OrderId == 1 && r.ProductId == 10);
                Assert.Equal(2, first.Quantity);
                Assert.Equal(100, first.UnitPrice);

                var second = result.First(r => r.OrderId == 1 && r.ProductId == 20);
                Assert.Equal(1, second.Quantity);
                Assert.Equal(200, second.UnitPrice);

                var third = result.First(r => r.OrderId == 2 && r.ProductId == 10);
                Assert.Equal(3, third.Quantity);
            }
        }

        [Fact]
        public void JoinNoMatchReturnsEmptyTest()
        {
            using (var connection = OpenNewConnection())
            {
                // Insert an order with no order lines
                connection.Insert(new Order
                {
                    PlacedTime = new DateTime(2025, 1, 1),
                });

                var result = connection.Query<Order>()
                    .Join(
                        connection.Query<OrderLine>(),
                        o => o.Id,
                        ol => ol.OrderId,
                        (o, ol) => new { o.Id, ol.ProductId })
                    .ToList();

                Assert.Empty(result);
            }
        }
    }
}
