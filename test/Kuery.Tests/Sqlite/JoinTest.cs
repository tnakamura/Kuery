using System.Linq;
using Microsoft.Data.Sqlite;
using Xunit;

namespace Kuery.Tests.Sqlite
{
    public class JoinTest : IClassFixture<SqliteFixture>
    {
        readonly SqliteFixture fixture;

        public JoinTest(SqliteFixture fixture)
        {
            this.fixture = fixture;
        }

        void CreateTables(SqliteConnection connection)
        {
            connection.DropTable(nameof(Product));
            connection.DropTable(nameof(Order));
            connection.DropTable(nameof(OrderLine));
            connection.DropTable(nameof(OrderHistory));

            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = $@"
                    create table if not exists {nameof(Product)} (
                        {nameof(Product.Id)} integer primary key autoincrement,
                        {nameof(Product.Name)} nvarchar(50) not null,
                        {nameof(Product.Price)} decimal not null,
                        {nameof(Product.TotalSales)} int not null
                    );";
                cmd.ExecuteNonQuery();
            }
            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = $@"
                    create table if not exists [{nameof(Order)}] (
                        {nameof(Order.Id)} integer primary key autoincrement,
                        {nameof(Order.PlacedTime)} datetime not null
                    );";
                cmd.ExecuteNonQuery();
            }
            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = $@"
                    create table if not exists {nameof(OrderHistory)} (
                        {nameof(OrderHistory.Id)} integer primary key autoincrement,
                        {nameof(OrderHistory.OrderId)} int not null,
                        {nameof(OrderHistory.Time)} datetime not null,
                        {nameof(OrderHistory.Comment)} nvarchar(50) null
                    );";
                cmd.ExecuteNonQuery();
            }
            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = $@"
                    create table if not exists {nameof(OrderLine)} (
                        {nameof(OrderLine.Id)} integer primary key autoincrement,
                        {nameof(OrderLine.OrderId)} integer not null,
                        {nameof(OrderLine.ProductId)} integer not null,
                        {nameof(OrderLine.Quantity)} integer not null,
                        {nameof(OrderLine.UnitPrice)} integer not null,
                        {nameof(OrderLine.Status)} integer not null
                    );";
                cmd.ExecuteNonQuery();
            }
        }

        [Fact]
        public void InnerJoin()
        {
            using var con = fixture.OpenNewConnection();
            CreateTables(con);

            var order1 = new Order
            {
                PlacedTime = System.DateTime.Now,
            };
            con.Insert(order1);

            var order2 = new Order
            {
                PlacedTime = System.DateTime.Now,
            };
            con.Insert(order2);

            con.Insert(new OrderLine
            {
                OrderId = 1,
                ProductId = 1,
                Quantity = 2,
                UnitPrice = 100,
                Status = OrderLineStatus.Placed,
            });

            con.Insert(new OrderLine
            {
                OrderId = 1,
                ProductId = 2,
                Quantity = 1,
                UnitPrice = 200,
                Status = OrderLineStatus.Shipped,
            });

            // order2 has no order lines

            // Join Order with OrderLine - only orders with order lines should be returned
            var result = con.Table<Order>()
                .Join(
                    con.Table<OrderLine>(),
                    o => o.Id,
                    ol => ol.OrderId)
                .ToList();

            // order1 has 2 order lines, so it appears twice; order2 has none
            Assert.Equal(2, result.Count);
            Assert.All(result, o => Assert.Equal(1, o.Id));
        }

        [Fact]
        public void InnerJoinWithWhere()
        {
            using var con = fixture.OpenNewConnection();
            CreateTables(con);

            con.Insert(new Product { Name = "A", Price = 10, TotalSales = 0 });
            con.Insert(new Product { Name = "B", Price = 20, TotalSales = 0 });
            con.Insert(new Product { Name = "C", Price = 30, TotalSales = 0 });

            var order = new Order { PlacedTime = System.DateTime.Now };
            con.Insert(order);

            con.Insert(new OrderLine
            {
                OrderId = 1,
                ProductId = 1,
                Quantity = 5,
                UnitPrice = 10,
                Status = OrderLineStatus.Placed,
            });
            con.Insert(new OrderLine
            {
                OrderId = 1,
                ProductId = 2,
                Quantity = 3,
                UnitPrice = 20,
                Status = OrderLineStatus.Shipped,
            });
            // Product C (Id=3) has no order line

            // Join Product with OrderLine and filter by Status
            var result = con.Table<OrderLine>()
                .Join(
                    con.Table<Product>(),
                    ol => ol.ProductId,
                    p => p.Id)
                .Where(ol => ol.Status == OrderLineStatus.Shipped)
                .ToList();

            Assert.Single(result);
            Assert.Equal(2, result[0].ProductId);
        }

        [Fact]
        public void InnerJoinWithOrderBy()
        {
            using var con = fixture.OpenNewConnection();
            CreateTables(con);

            con.Insert(new Product { Name = "A", Price = 10, TotalSales = 0 });
            con.Insert(new Product { Name = "B", Price = 20, TotalSales = 0 });

            var order = new Order { PlacedTime = System.DateTime.Now };
            con.Insert(order);

            con.Insert(new OrderLine
            {
                OrderId = 1,
                ProductId = 1,
                Quantity = 5,
                UnitPrice = 10,
                Status = OrderLineStatus.Placed,
            });
            con.Insert(new OrderLine
            {
                OrderId = 1,
                ProductId = 2,
                Quantity = 3,
                UnitPrice = 20,
                Status = OrderLineStatus.Placed,
            });

            var result = con.Table<OrderLine>()
                .Join(
                    con.Table<Product>(),
                    ol => ol.ProductId,
                    p => p.Id)
                .OrderByDescending(ol => ol.Quantity)
                .ToList();

            Assert.Equal(2, result.Count);
            Assert.Equal(5, result[0].Quantity);
            Assert.Equal(3, result[1].Quantity);
        }

        [Fact]
        public void InnerJoinWithTake()
        {
            using var con = fixture.OpenNewConnection();
            CreateTables(con);

            con.Insert(new Product { Name = "A", Price = 10, TotalSales = 0 });
            con.Insert(new Product { Name = "B", Price = 20, TotalSales = 0 });

            var order = new Order { PlacedTime = System.DateTime.Now };
            con.Insert(order);

            con.Insert(new OrderLine
            {
                OrderId = 1,
                ProductId = 1,
                Quantity = 5,
                UnitPrice = 10,
                Status = OrderLineStatus.Placed,
            });
            con.Insert(new OrderLine
            {
                OrderId = 1,
                ProductId = 2,
                Quantity = 3,
                UnitPrice = 20,
                Status = OrderLineStatus.Placed,
            });

            var result = con.Table<OrderLine>()
                .Join(
                    con.Table<Product>(),
                    ol => ol.ProductId,
                    p => p.Id)
                .Take(1)
                .ToList();

            Assert.Single(result);
        }

        [Fact]
        public void InnerJoinCount()
        {
            using var con = fixture.OpenNewConnection();
            CreateTables(con);

            con.Insert(new Product { Name = "A", Price = 10, TotalSales = 0 });
            con.Insert(new Product { Name = "B", Price = 20, TotalSales = 0 });
            con.Insert(new Product { Name = "C", Price = 30, TotalSales = 0 });

            var order = new Order { PlacedTime = System.DateTime.Now };
            con.Insert(order);

            con.Insert(new OrderLine
            {
                OrderId = 1,
                ProductId = 1,
                Quantity = 5,
                UnitPrice = 10,
                Status = OrderLineStatus.Placed,
            });
            con.Insert(new OrderLine
            {
                OrderId = 1,
                ProductId = 2,
                Quantity = 3,
                UnitPrice = 20,
                Status = OrderLineStatus.Placed,
            });

            var count = con.Table<OrderLine>()
                .Join(
                    con.Table<Product>(),
                    ol => ol.ProductId,
                    p => p.Id)
                .Count();

            Assert.Equal(2, count);
        }

        [Fact]
        public void InnerJoinNoMatch()
        {
            using var con = fixture.OpenNewConnection();
            CreateTables(con);

            con.Insert(new Order { PlacedTime = System.DateTime.Now });

            // No order lines exist
            var result = con.Table<Order>()
                .Join(
                    con.Table<OrderLine>(),
                    o => o.Id,
                    ol => ol.OrderId)
                .ToList();

            Assert.Empty(result);
        }

        [Fact]
        public void InnerJoinWithResultSelector()
        {
            using var con = fixture.OpenNewConnection();
            CreateTables(con);

            con.Insert(new Product { Name = "A", Price = 10, TotalSales = 0 });
            con.Insert(new Product { Name = "B", Price = 20, TotalSales = 0 });

            var order = new Order { PlacedTime = System.DateTime.Now };
            con.Insert(order);

            con.Insert(new OrderLine
            {
                OrderId = 1,
                ProductId = 1,
                Quantity = 5,
                UnitPrice = 10,
                Status = OrderLineStatus.Placed,
            });
            con.Insert(new OrderLine
            {
                OrderId = 1,
                ProductId = 2,
                Quantity = 3,
                UnitPrice = 20,
                Status = OrderLineStatus.Placed,
            });

            // Use Join with result selector that selects the Product type
            var result = con.Table<OrderLine>()
                .Join<Product, int, Product>(
                    con.Table<Product>(),
                    ol => ol.ProductId,
                    p => p.Id,
                    (ol, p) => p)
                .ToList();

            Assert.Equal(2, result.Count);
            Assert.Contains(result, p => p.Name == "A");
            Assert.Contains(result, p => p.Name == "B");
        }
    }
}
