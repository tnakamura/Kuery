using System;
using System.Collections.Generic;
using System.Linq;
using MySqlConnector;
using Xunit;

namespace Kuery.Tests.MySql
{
    public class LinqTest : IClassFixture<MySqlFixture>
    {
        readonly MySqlFixture fixture;

        public LinqTest(MySqlFixture fixture)
        {
            this.fixture = fixture;
        }

        void CreateTables(MySqlConnection connection)
        {
            connection.DropTable(nameof(Product));
            connection.DropTable(nameof(Order));
            connection.DropTable(nameof(OrderLine));
            connection.DropTable(nameof(OrderHistory));

            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = $@"
                    create table {nameof(Product)} (
                        {nameof(Product.Id)} integer primary key auto_increment,
                        {nameof(Product.Name)} varchar(50) not null,
                        {nameof(Product.Price)} decimal(18,2) not null,
                        {nameof(Product.TotalSales)} int not null
                    );";
                cmd.ExecuteNonQuery();
            }
            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = $@"
                    create table `{nameof(Order)}` (
                        {nameof(Order.Id)} int primary key auto_increment,
                        {nameof(Order.PlacedTime)} datetime not null
                    );";
                cmd.ExecuteNonQuery();
            }
            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = $@"
                    create table {nameof(OrderHistory)} (
                        {nameof(OrderHistory.Id)} int primary key auto_increment,
                        {nameof(OrderHistory.OrderId)} int not null,
                        {nameof(OrderHistory.Time)} datetime not null,
                        {nameof(OrderHistory.Comment)} varchar(50) null
                    );";
                cmd.ExecuteNonQuery();
            }
            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = $@"
                    create table {nameof(OrderLine)} (
                        {nameof(OrderLine.Id)} int primary key auto_increment,
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

            con.Insert(new Product { Name = "A", Price = 20 });
            con.Insert(new Product { Name = "B", Price = 10 });

            Func<decimal, List<Product>> getProductsWithPriceAtLeast = val =>
            {
                return (from p in con.Table<Product>() where p.Price > val select p).ToList();
            };

            var r = getProductsWithPriceAtLeast(15);
            Assert.Single(r);
            Assert.Equal("A", r[0].Name);
        }
    }
}
