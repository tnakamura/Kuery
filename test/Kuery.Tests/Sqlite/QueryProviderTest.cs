using System.Collections.Generic;
using System.Linq;
using Kuery.Linq;
using Microsoft.Data.Sqlite;
using Xunit;
using Xunit.Abstractions;

namespace Kuery.Tests.Sqlite
{
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
        public void GetQueryTextTest()
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
            var query = new Query<Product>(provider).Where(x => x.Name == "A");
            var queryText = provider.GetQueryText(query.Expression);
            #endregion

            #region Assert
            output.WriteLine(queryText);

            Assert.Equal(
                $"SELECT * FROM (SELECT * FROM Product) AS T WHERE (Name = 'A')",
                actual: queryText);
            #endregion
        }

        [Fact]
        public void ExecuteTest()
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
            var query = new Query<Product>(provider).Where(x => x.Name == "A");
            var products = (provider.Execute(query.Expression) as IEnumerable<Product>)?.ToList();
            #endregion

            #region Assert
            Assert.NotNull(products);
            Assert.Single(products);

            var actual = products[0];
            Assert.Equal("A", actual.Name);
            Assert.Equal(20, actual.Price);
            #endregion
        }

        [Fact]
        public void LocalVariableReferencesTest()
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
            var query = new Query<Product>(provider).Where(x => x.Name == name);
            var queryText = provider.GetQueryText(query.Expression);
            #endregion

            #region Assert
            output.WriteLine(queryText);

            Assert.Equal(
                $"SELECT * FROM (SELECT * FROM Product) AS T WHERE (Name = 'A')",
                actual: queryText);
            #endregion
        }
    }
}
