using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Xunit;

namespace Kuery.Tests.SqlClient
{
    public class LinqTest : IClassFixture<SqlClientFixture>
    {
        readonly SqlClientFixture fixture;

        public LinqTest(SqlClientFixture fixture)
        {
            this.fixture = fixture;
        }

        void CreateTables(SqlConnection connection)
        {
            connection.DropTable(nameof(Product));
            connection.DropTable(nameof(Order));
            connection.DropTable(nameof(OrderLine));
            connection.DropTable(nameof(OrderHistory));

            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = $@"
                    create table {nameof(Product)} (
                        {nameof(Product.Id)} integer primary key identity,
                        {nameof(Product.Name)} nvarchar(50) not null,
                        {nameof(Product.Price)} decimal not null,
                        {nameof(Product.TotalSales)} int not null
                    );";
                cmd.ExecuteNonQuery();
            }
            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = $@"
                    create table [{nameof(Order)}] (
                        {nameof(Order.Id)} int primary key identity,
                        {nameof(Order.PlacedTime)} datetime not null
                    );";
                cmd.ExecuteNonQuery();
            }
            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = $@"
                    create table {nameof(OrderHistory)} (
                        {nameof(OrderHistory.Id)} int primary key identity,
                        {nameof(OrderHistory.OrderId)} int not null,
                        {nameof(OrderHistory.Time)} datetime not null,
                        {nameof(OrderHistory.Comment)} nvarchar(50) null
                    );";
                cmd.ExecuteNonQuery();
            }
            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = $@"
                    create table {nameof(OrderLine)} (
                        {nameof(OrderLine.Id)} int primary key identity,
                        {nameof(OrderLine.OrderId)} int not null,
                        {nameof(OrderLine.ProductId)} int not null,
                        {nameof(OrderLine.Quantity)} int not null,
                        {nameof(OrderLine.UnitPrice)} int not null,
                        {nameof(OrderLine.Status)} int not null
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

        [Fact]
        public async Task QueryEntryPointSupportsQueryableOperators()
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

            var query = con.Query<Product>()
                .Where(p => p.Price >= 10)
                .OrderBy(p => p.Price);

            Assert.Equal(2, query.Count());
            Assert.Equal(2L, query.LongCount());
            Assert.True(query.Any());
            Assert.True(query.All(p => p.Price >= 10));
            Assert.Equal("B", query.First().Name);

            var list = await query.ToListAsync();
            Assert.Equal(2, list.Count);
            Assert.Equal("B", list[0].Name);
            Assert.Equal("A", list[1].Name);
        }

        [Fact]
        public void QueryEntryPointSupportsSelect()
        {
            using var con = fixture.OpenNewConnection();
            CreateTables(con);

            con.Insert(new Product
            {
                Name = "A",
                Price = 20,
            });

            var values = con.Query<Product>()
                .Select(x => x.Name)
                .ToList();

            Assert.Single(values);
            Assert.Equal("A", values[0]);
        }

        [Fact]
        public void QueryEntryPointExecuteDeleteDeletesMatchingRows()
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

            var deleted = con.Query<Product>()
                .Where(p => p.Price >= 10)
                .ExecuteDelete();

            Assert.Equal(2, deleted);
            Assert.Single(con.Query<Product>().ToList());
            Assert.Equal("C", con.Query<Product>().First().Name);
        }

        [Fact]
        public async Task QueryEntryPointExecuteDeleteAsyncDeletesMatchingRows()
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

            var deleted = await con.Query<Product>()
                .Where(p => p.Price >= 10)
                .ExecuteDeleteAsync();

            Assert.Equal(2, deleted);
            Assert.Single(con.Query<Product>().ToList());
            Assert.Equal("C", con.Query<Product>().First().Name);
        }

        [Fact]
        public void QueryEntryPointExecuteUpdateUpdatesMatchingRows()
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

            var updated = con.Query<Product>()
                .Where(p => p.Price >= 10)
                .ExecuteUpdate(s => s
                    .SetProperty(p => p.Name, "Updated")
                    .SetProperty(p => p.TotalSales, p => p.TotalSales + 1));

            Assert.Equal(2, updated);

            var list = con.Query<Product>()
                .OrderBy(p => p.Price)
                .ToList();
            Assert.Equal("C", list[0].Name);
            Assert.Equal(0, list[0].TotalSales);
            Assert.Equal("Updated", list[1].Name);
            Assert.Equal(1, list[1].TotalSales);
            Assert.Equal("Updated", list[2].Name);
            Assert.Equal(1, list[2].TotalSales);
        }

        [Fact]
        public async Task QueryEntryPointExecuteUpdateAsyncUpdatesMatchingRows()
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

            var updated = await con.Query<Product>()
                .Where(p => p.Price >= 10)
                .ExecuteUpdateAsync(s => s.SetProperty(p => p.Name, "AsyncUpdated"));

            Assert.Equal(2, updated);

            var list = con.Query<Product>()
                .OrderBy(p => p.Price)
                .ToList();
            Assert.Equal("C", list[0].Name);
            Assert.Equal("AsyncUpdated", list[1].Name);
            Assert.Equal("AsyncUpdated", list[2].Name);
        }

        [Fact]
        public void QueryEntryPointExecuteUpdateRequiresWherePredicate()
        {
            using var con = fixture.OpenNewConnection();
            CreateTables(con);

            con.Insert(new Product
            {
                Name = "A",
                Price = 20,
            });

            var ex = Assert.Throws<InvalidOperationException>(() => con.Query<Product>()
                .ExecuteUpdate(s => s.SetProperty(p => p.Name, "Updated")));
            Assert.Equal("No condition specified", ex.Message);
        }

        [Fact]
        public void QueryEntryPointExecuteDeleteRequiresWherePredicate()
        {
            using var con = fixture.OpenNewConnection();
            CreateTables(con);

            con.Insert(new Product
            {
                Name = "A",
                Price = 20,
            });

            var ex = Assert.Throws<InvalidOperationException>(() => con.Query<Product>().ExecuteDelete());
            Assert.Equal("No condition specified", ex.Message);
        }
    }
}
