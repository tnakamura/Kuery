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
                    if object_id (N'{nameof(Product)}') is null
                        create table {nameof(Product)} (
                            {nameof(Product.Id)} integer identity(1,1) primary key not null,
                            {nameof(Product.Name)} nvarchar(50) not null,
                            {nameof(Product.Price)} decimal not null,
                            {nameof(Product.TotalSales)} int not null
                        );";
                cmd.ExecuteNonQuery();
            }
            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = $@"
                    if object_id (N'{nameof(Order)}') is null
                        create table {nameof(Order)} (
                            {nameof(Order.Id)} integer identity(1,1) primary key not null,
                            {nameof(Order.PlacedTime)} datetime not null
                        );";
                cmd.ExecuteNonQuery();
            }
            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = $@"
                    if object_id (N'{nameof(OrderHistory)}') is null
                        create table {nameof(OrderHistory)} (
                            {nameof(OrderHistory.Id)} integer identity(1,1) primary key not null,
                            {nameof(OrderHistory.OrderId)} int not null,
                            {nameof(OrderHistory.Time)} datetime not null,
                            {nameof(OrderHistory.Comment)} nvarchar(50) null
                        );";
                cmd.ExecuteNonQuery();
            }
            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = $@"
                    if object_id (N'{nameof(OrderLine)}') is null
                        create table {nameof(OrderLine)} (
                            {nameof(OrderLine.Id)} integer identity(1,1) primary key not null,
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

        public class Issue96_A
        {
            [AutoIncrement, PrimaryKey]
            public int ID { get; set; }
            public string AddressLine { get; set; }

            [Indexed]
            public int? ClassB { get; set; }
            [Indexed]
            public int? ClassC { get; set; }
        }

        public class Issue96_B
        {
            [AutoIncrement, PrimaryKey]
            public int ID { get; set; }
            public string CustomerName { get; set; }
        }

        public class Issue96_C
        {
            [AutoIncrement, PrimaryKey]
            public int ID { get; set; }
            public string SupplierName { get; set; }
        }

        [Fact]
        public void NullabelIntsInQueries()
        {
            using var con = fixture.OpenNewConnection();
            CreateTables(con);

            var id = 42;

            con.Insert(new Issue96_A
            {
                ClassB = id,
            });
            con.Insert(new Issue96_A
            {
                ClassB = null,
            });
            con.Insert(new Issue96_A
            {
                ClassB = null,
            });
            con.Insert(new Issue96_A
            {
                ClassB = null,
            });


            Assert.Equal(1, con.Table<Issue96_A>().Where(p => p.ClassB == id).Count());
            Assert.Equal(3, con.Table<Issue96_A>().Where(p => p.ClassB == null).Count());
        }

        public class Issue303_A
        {
            [PrimaryKey, NotNull]
            public int Id { get; set; }
            public string Name { get; set; }
        }

        public class Issue303_B
        {
            [PrimaryKey, NotNull]
            public int Id { get; set; }
            public bool Flag { get; set; }
        }

        [Fact]
        public void Issue303_WhereNot_A()
        {
            using var con = fixture.OpenNewConnection();
            CreateTables(con);

            con.Insert(new Issue303_A { Id = 1, Name = "aa" });
            con.Insert(new Issue303_A { Id = 2, Name = null });
            con.Insert(new Issue303_A { Id = 3, Name = "test" });
            con.Insert(new Issue303_A { Id = 4, Name = null });

            var r = (from p in con.Table<Issue303_A>() where !(p.Name == null) select p).ToList();
            Assert.Equal(2, r.Count);
            Assert.Equal(1, r[0].Id);
            Assert.Equal(3, r[1].Id);
        }

        [Fact]
        public void Issue303_WhereNot_B()
        {
            using var db = fixture.OpenNewConnection();
            CreateTables(db);

            db.Insert(new Issue303_B { Id = 1, Flag = true });
            db.Insert(new Issue303_B { Id = 2, Flag = false });
            db.Insert(new Issue303_B { Id = 3, Flag = true });
            db.Insert(new Issue303_B { Id = 4, Flag = false });

            var r = (from p in db.Table<Issue303_B>() where !p.Flag select p).ToList();
            Assert.Equal(2, r.Count);
            Assert.Equal(2, r[0].Id);
            Assert.Equal(4, r[1].Id);
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

        interface IEntity
        {
            int Id { get; set; }
            string Value { get; set; }
        }

        class Entity : IEntity
        {
            [AutoIncrement, PrimaryKey]
            public int Id { get; set; }
            public string Value { get; set; }
        }

        static T GetEntity<T>(DbConnection db, int id)
            where T : IEntity, new()
        {
            return db.Table<T>().FirstOrDefault(x => x.Id == id);
        }

        [Fact]
        public void CastedParameters()
        {
            using var con = fixture.OpenNewConnection();
            CreateTables(con);

            con.Insert(new Entity
            {
                Value = "Foo",
            });

            var r = GetEntity<Entity>(con, 1);

            Assert.Equal("Foo", r.Value);
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
