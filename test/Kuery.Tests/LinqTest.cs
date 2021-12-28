using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using Xunit;

namespace Kuery.Tests
{
    public class LinqTest : IClassFixture<SqliteFixture>
    {
        readonly SqliteFixture fixture;

        public LinqTest(SqliteFixture fixture)
        {
            this.fixture = fixture;
        }

        void CreateTables(DbConnection connection)
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
        public void FunctionParameter()
        {
            using var con = fixture.OpenNewConnection();
            CreateTables(con);

            con.Insert(new Product
            {
                Name = "A",
                Price = 20,
            });

            con.Insert(new Product
            {
                Name = "B",
                Price = 10,
            });

            Func<decimal, List<Product>> GetProductsWithPriceAtLeast = (val) =>
            {
                return (from p in con.Table<Product>() where p.Price > val select p).ToList();
            };

            var r = GetProductsWithPriceAtLeast(15);
            Assert.Single(r);
            Assert.Equal("A", r[0].Name);
        }

        [Fact]
        public void WhereGreaterThan()
        {
            using var con = fixture.OpenNewConnection();
            CreateTables(con);

            con.Insert(new Product
            {
                Name = "A",
                Price = 20,
            });

            con.Insert(new Product
            {
                Name = "B",
                Price = 10,
            });

            Assert.Equal(2, con.Table<Product>().Count());

            var r = (from p in con.Table<Product>() where p.Price > 15 select p).ToList();
            Assert.Single(r);
            Assert.Equal("A", r[0].Name);
        }

        [Fact]
        public void GetWithExpression()
        {
            using var con = fixture.OpenNewConnection();
            CreateTables(con);

            con.Insert(new Product
            {
                Name = "A",
                Price = 20,
            });

            con.Insert(new Product
            {
                Name = "B",
                Price = 10,
            });

            con.Insert(new Product
            {
                Name = "C",
                Price = 5,
            });

            Assert.Equal(3, con.Table<Product>().Count());

            var r = con.Get<Product>(x => x.Price == 10);
            Assert.NotNull(r);
            Assert.Equal("B", r.Name);
        }

        [Fact]
        public void FindWithExpression()
        {
            using var con = fixture.OpenNewConnection();
            CreateTables(con);

            var r = con.Find<Product>(x => x.Price == 10);
            Assert.Null(r);
        }

        [Fact]
        public void OrderByCast()
        {
            using var con = fixture.OpenNewConnection();
            CreateTables(con);

            con.Insert(new Product
            {
                Name = "A",
                TotalSales = 1,
            });
            con.Insert(new Product
            {
                Name = "B",
                TotalSales = 100,
            });

            var nocast = (from p in con.Table<Product>() orderby p.TotalSales descending select p).ToList();
            Assert.Equal(2, nocast.Count);
            Assert.Equal("B", nocast[0].Name);

            var cast = (from p in con.Table<Product>() orderby (int)p.TotalSales descending select p).ToList();
            Assert.Equal(2, cast.Count);
            Assert.Equal("B", cast[0].Name);
        }

        [Fact]
        public void QuerySelectAverage()
        {
            using var con = fixture.OpenNewConnection();
            CreateTables(con);

            con.Insert(new Product
            {
                Name = "A",
                Price = 20,
                TotalSales = 100,
            });

            con.Insert(new Product
            {
                Name = "B",
                Price = 10,
                TotalSales = 100,
            });

            con.Insert(new Product
            {
                Name = "C",
                Price = 1000,
                TotalSales = 1,
            });

            var r = con.Table<Product>().Where(x => x.TotalSales > 50).Select(s => s.Price).Average();

            Assert.Equal(15m, r);
        }

        [Fact]
        public void ReplaceWith2Args()
        {
            using var con = fixture.OpenNewConnection();
            CreateTables(con);

            con.Insert(new Product
            {
                Name = "I am not B X B",
            });
            con.Insert(new Product
            {
                Name = "I am B O B",
            });

            var cl = (
                from c in con.Table<Product>()
                where c.Name.Replace(" ", "").Contains("BOB")
                select c
            ).FirstOrDefault();

            Assert.Equal(2, cl.Id);
            Assert.Equal("I am B O B", cl.Name);
        }
    }
}
