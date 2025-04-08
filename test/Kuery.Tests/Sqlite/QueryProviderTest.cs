using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using Kuery.Linq;
using Microsoft.Data.Sqlite;
using Xunit;
using Xunit.Abstractions;
using Xunit.Sdk;

namespace Kuery.Tests.Sqlite
{
    public class Customers
    {
        [PrimaryKey]
        public string CustomerID;
        public string ContactName;
        public string Phone;
        public string City;
        public string Country;
    }

    public class Orders
    {
        public int OrderID;
        public string CustomerID;
        public DateTime OrderDate;
    }

    internal class Northwind
    {
        internal Query<Customers> Customers { get; }

        internal Query<Orders> Orders { get; }

        internal Northwind(DbConnection connection)
        {
            var provider = new DbQueryProvider(connection);
            Customers = new Query<Customers>(provider);
            Orders = new Query<Orders>(provider);
        }
    }

    public class QueryProviderTest : IClassFixture<SqliteFixture>
    {
        private readonly SqliteFixture fixture;

        private readonly ITestOutputHelper output;

        public QueryProviderTest(SqliteFixture fixture, ITestOutputHelper output)
        {
            this.fixture = fixture;
            this.output = output;
        }

        void CreateTables(SqliteConnection connection)
        {
            connection.DropTable(nameof(Orders));
            connection.DropTable(nameof(Customers));

            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = $@"
                    create table if not exists {nameof(Customers)} (
                        {nameof(Customers.CustomerID)} nvarchar(50) primary key,
                        {nameof(Customers.ContactName)} nvarchar(50) not null,
                        {nameof(Customers.Phone)} nvarchar(50) not null,
                        {nameof(Customers.City)} nvarchar(100) not null,
                        {nameof(Customers.Country)} nvarchar(100) not null
                    );";
                cmd.ExecuteNonQuery();
            }
            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = $@"
                    create table if not exists {nameof(Orders)} (
                        {nameof(Orders.OrderID)} integer primary key autoincrement,
                        {nameof(Orders.CustomerID)} nvarchar(50) not null,
                        {nameof(Orders.OrderDate)} datetime not null
                    );";
                cmd.ExecuteNonQuery();
            }
        }

        private void InsertCustomers(SqliteConnection connection, Customers customer)
        {
            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = $@"
insert into {nameof(Customers)} (
    {nameof(Customers.CustomerID)},
    {nameof(Customers.ContactName)},
    {nameof(Customers.Phone)},
    {nameof(Customers.Country)},
    {nameof(Customers.City)}
) values (
    @{nameof(Customers.CustomerID)},
    @{nameof(Customers.ContactName)},
    @{nameof(Customers.Phone)},
    @{nameof(Customers.Country)},
    @{nameof(Customers.City)}
)";
                cmd.Parameters.AddWithValue($"@{nameof(customer.CustomerID)}", customer.CustomerID);
                cmd.Parameters.AddWithValue($"@{nameof(customer.ContactName)}", customer.ContactName);
                cmd.Parameters.AddWithValue($"@{nameof(customer.Phone)}", customer.Phone);
                cmd.Parameters.AddWithValue($"@{nameof(customer.Country)}", customer.Country);
                cmd.Parameters.AddWithValue($"@{nameof(customer.City)}", customer.City);
                cmd.ExecuteNonQuery();
            }
        }

        [Fact]
        public void ExecuteTest()
        {
            #region Arrange
            using var con = fixture.OpenNewConnection();

            CreateTables(con);

            InsertCustomers(con, new Customers
            {
                CustomerID = Guid.CreateVersion7().ToString(),
                ContactName = "Tomiyasu",
                Country = "England",
                Phone = "123-456-789",
                City = "London",
            });
            InsertCustomers(con, new Customers
            {
                CustomerID = Guid.CreateVersion7().ToString(),
                ContactName = "Endo",
                Country = "England",
                Phone = "123-456-789",
                City = "Liverpool",
            });
            #endregion

            #region Act
            var db = new Northwind(con);
            var query = db.Customers.Where(x => x.City == "London");
            var customers = query.ToList();
            #endregion

            #region Assert
            Assert.NotNull(customers);
            Assert.Single(customers);

            var actual = customers[0];
            Assert.Equal("London", actual.City);
            #endregion
        }

        [Fact]
        public void LocalVariableReferencesTest()
        {
            #region Arrange
            using var con = fixture.OpenNewConnection();

            CreateTables(con);

            InsertCustomers(con, new Customers
            {
                CustomerID = Guid.CreateVersion7().ToString(),
                ContactName = "Tomiyasu",
                Country = "England",
                Phone = "123-456-789",
                City = "London",
            });
            InsertCustomers(con, new Customers
            {
                CustomerID = Guid.CreateVersion7().ToString(),
                ContactName = "Endo",
                Country = "England",
                Phone = "123-456-789",
                City = "Liverpool",
            });
            #endregion

            #region Act
            var city = "London";
            var db = new Northwind(con);
            var query = db.Customers.Where(x => x.City == city);
            var customers = query.ToList();
            #endregion

            #region Assert
            Assert.NotNull(customers);
            Assert.Single(customers);

            var actual = customers[0];
            Assert.Equal("London", actual.City);
            #endregion
        }

        [Fact]
        public void SelectTest()
        {
            #region Arrange
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
            #endregion

            #region Act
            var provider = new DbQueryProvider(con);
            var name = "A";
            var query = new Query<Product>(provider)
                .Where(x => x.Name == name)
                .Select(x => new
                {
                    Name = x.Name,
                    Price = x.Price,
                });
            var queryText = provider.GetQueryText(query.Expression);
            #endregion

            #region Assert
            output.WriteLine(queryText);

            Assert.Equal(
                $"SELECT Name, Price FROM (SELECT * FROM (SELECT * FROM Product) AS T WHERE (Name = 'A')) AS T",
                actual: queryText);
            #endregion
        }

        [Fact]
        public void ColumnBindingTest()
        {
            #region Arrange
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
            #endregion

            #region Act
            var provider = new DbQueryProvider(con);
            var price = 20;
            var query = new Query<Product>(provider)
                .Select(x => new
                {
                    Name = x.Name,
                    Info = new
                    {
                        Price = x.Price,
                        TotalSales = x.TotalSales,
                    },
                })
                .Where(x => x.Info.Price == price);
            var queryText = provider.GetQueryText(query.Expression);
            #endregion

            #region Assert
            output.WriteLine(queryText);

            Assert.Equal(
                @"SELECT t2.Name, t2.Price, t2.TotalSales
FROM (
  SELECT t1.Name, t1.Price, t1.TotalSales
  FROM (
    SELECT t0.Name, t0.Price, t0.TotalSales
    FROM Product AS t0
  ) AS t1
) AS t2
WHERE (t2.Price = 20)",
                actual: queryText);
            #endregion
        }
    }
}
