using System;
using System.Linq;
using Microsoft.Data.Sqlite;
using Xunit;

namespace Kuery.Tests.Sqlite
{
    public class QueryablePhase5SubqueryTest : IDisposable
    {
        readonly string _dataSource;

        public QueryablePhase5SubqueryTest()
        {
            var dbName = $"kuery_phase5_test_{Guid.NewGuid():N}";
            _dataSource = System.IO.Path.Combine(
                AppContext.BaseDirectory,
                $"{dbName}.sqlite3");

            using (var connection = CreateConnection())
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = @"
                        CREATE TABLE SubCustomer (
                            Id INTEGER PRIMARY KEY NOT NULL,
                            Name TEXT NOT NULL,
                            City TEXT NOT NULL
                        );
                        CREATE TABLE SubOrder (
                            Id INTEGER PRIMARY KEY NOT NULL,
                            CustomerId INTEGER NOT NULL,
                            Amount REAL NOT NULL,
                            Product TEXT NOT NULL
                        );
                        CREATE TABLE SubCategory (
                            Id INTEGER PRIMARY KEY NOT NULL,
                            Name TEXT NOT NULL
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

        // --- Model classes ---

        [Table("SubCustomer")]
        public class SubCustomer
        {
            [PrimaryKey]
            [Column("Id")]
            public int Id { get; set; }

            [Column("Name")]
            public string Name { get; set; }

            [Column("City")]
            public string City { get; set; }
        }

        [Table("SubOrder")]
        public class SubOrder
        {
            [PrimaryKey]
            [Column("Id")]
            public int Id { get; set; }

            [Column("CustomerId")]
            public int CustomerId { get; set; }

            [Column("Amount")]
            public double Amount { get; set; }

            [Column("Product")]
            public string Product { get; set; }
        }

        [Table("SubCategory")]
        public class SubCategory
        {
            [PrimaryKey]
            [Column("Id")]
            public int Id { get; set; }

            [Column("Name")]
            public string Name { get; set; }
        }

        // --- Seed helpers ---

        private void SeedData()
        {
            using (var connection = OpenNewConnection())
            {
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = @"
                        INSERT INTO SubCustomer (Id, Name, City) VALUES
                            (1, 'Alice', 'Tokyo'),
                            (2, 'Bob', 'Osaka'),
                            (3, 'Charlie', 'Tokyo'),
                            (4, 'Diana', 'Nagoya');

                        INSERT INTO SubOrder (Id, CustomerId, Amount, Product) VALUES
                            (1, 1, 100.0, 'Widget'),
                            (2, 1, 200.0, 'Gadget'),
                            (3, 2, 150.0, 'Widget'),
                            (4, 3, 300.0, 'Gizmo'),
                            (5, 3, 50.0, 'Widget');

                        INSERT INTO SubCategory (Id, Name) VALUES
                            (1, 'Electronics'),
                            (2, 'Books');";
                    command.ExecuteNonQuery();
                }
            }
        }

        // =====================================================
        // 5-1: WHERE IN subquery (Queryable.Contains → col IN (SELECT ...))
        // =====================================================

        [Fact]
        public void WhereInSubqueryTest()
        {
            SeedData();

            using (var connection = OpenNewConnection())
            {
                // Customers who have placed orders
                var customerIdsWithOrders = connection.Query<SubOrder>()
                    .Select(o => o.CustomerId);

                var result = connection.Query<SubCustomer>()
                    .Where(c => customerIdsWithOrders.Contains(c.Id))
                    .ToList();

                // Alice (1), Bob (2), Charlie (3) have orders; Diana (4) does not
                Assert.Equal(3, result.Count);
                Assert.DoesNotContain(result, c => c.Name == "Diana");
            }
        }

        [Fact]
        public void WhereInSubqueryWithFilterTest()
        {
            SeedData();

            using (var connection = OpenNewConnection())
            {
                // Customers with high-value orders (> 150)
                var highValueCustomerIds = connection.Query<SubOrder>()
                    .Where(o => o.Amount > 150)
                    .Select(o => o.CustomerId);

                var result = connection.Query<SubCustomer>()
                    .Where(c => highValueCustomerIds.Contains(c.Id))
                    .ToList();

                // Alice has 200 order, Charlie has 300 order
                Assert.Equal(2, result.Count);
                Assert.Contains(result, c => c.Name == "Alice");
                Assert.Contains(result, c => c.Name == "Charlie");
            }
        }

        [Fact]
        public void WhereNotInSubqueryTest()
        {
            SeedData();

            using (var connection = OpenNewConnection())
            {
                // Customers who have NOT placed orders
                var customerIdsWithOrders = connection.Query<SubOrder>()
                    .Select(o => o.CustomerId);

                var result = connection.Query<SubCustomer>()
                    .Where(c => !customerIdsWithOrders.Contains(c.Id))
                    .ToList();

                // Only Diana (4) has no orders
                Assert.Single(result);
                Assert.Equal("Diana", result[0].Name);
            }
        }

        [Fact]
        public void WhereInSubqueryWithDistinctTest()
        {
            SeedData();

            using (var connection = OpenNewConnection())
            {
                // Use Distinct in the subquery to avoid duplicate customer IDs
                var customerIdsWithOrders = connection.Query<SubOrder>()
                    .Select(o => o.CustomerId)
                    .Distinct();

                var result = connection.Query<SubCustomer>()
                    .Where(c => customerIdsWithOrders.Contains(c.Id))
                    .ToList();

                Assert.Equal(3, result.Count);
            }
        }

        // =====================================================
        // 5-2: EXISTS subquery (Queryable.Any → EXISTS (SELECT ...))
        // =====================================================

        [Fact]
        public void WhereExistsSubqueryTest()
        {
            SeedData();

            using (var connection = OpenNewConnection())
            {
                // Customers who have at least one order
                var result = connection.Query<SubCustomer>()
                    .Where(c => connection.Query<SubOrder>().Any(o => o.CustomerId == c.Id))
                    .ToList();

                // Alice, Bob, Charlie have orders
                Assert.Equal(3, result.Count);
                Assert.DoesNotContain(result, c => c.Name == "Diana");
            }
        }

        [Fact]
        public void WhereNotExistsSubqueryTest()
        {
            SeedData();

            using (var connection = OpenNewConnection())
            {
                // Customers who have NO orders
                var result = connection.Query<SubCustomer>()
                    .Where(c => !connection.Query<SubOrder>().Any(o => o.CustomerId == c.Id))
                    .ToList();

                // Only Diana has no orders
                Assert.Single(result);
                Assert.Equal("Diana", result[0].Name);
            }
        }

        [Fact]
        public void WhereExistsWithConditionTest()
        {
            SeedData();

            using (var connection = OpenNewConnection())
            {
                // Customers who have orders with amount > 200
                var result = connection.Query<SubCustomer>()
                    .Where(c => connection.Query<SubOrder>()
                        .Where(o => o.Amount > 200)
                        .Any(o => o.CustomerId == c.Id))
                    .ToList();

                // Only Charlie has an order with amount 300
                Assert.Single(result);
                Assert.Equal("Charlie", result[0].Name);
            }
        }

        // =====================================================
        // 5-3: SelectMany() → CROSS JOIN
        // =====================================================

        [Fact]
        public void SelectManyCrossJoinWithResultSelectorTest()
        {
            SeedData();

            using (var connection = OpenNewConnection())
            {
                // Cross join customers with categories
                var result = connection.Query<SubCustomer>()
                    .SelectMany(
                        c => connection.Query<SubCategory>(),
                        (c, cat) => new { CustomerName = c.Name, CategoryName = cat.Name })
                    .ToList();

                // 4 customers × 2 categories = 8 rows
                Assert.Equal(8, result.Count);
            }
        }

        [Fact]
        public void SelectManyCrossJoinWithoutResultSelectorTest()
        {
            SeedData();

            using (var connection = OpenNewConnection())
            {
                // Cross join: each customer × each category, returning categories
                var result = connection.Query<SubCustomer>()
                    .SelectMany(c => connection.Query<SubCategory>())
                    .ToList();

                // 4 customers × 2 categories = 8 rows (all SubCategory)
                Assert.Equal(8, result.Count);
                Assert.All(result, item => Assert.NotNull(item.Name));
            }
        }

        [Fact]
        public void SelectManyCrossJoinWithWhereTest()
        {
            SeedData();

            using (var connection = OpenNewConnection())
            {
                // Cross join with filtering on the outer table
                var result = connection.Query<SubCustomer>()
                    .Where(c => c.City == "Tokyo")
                    .SelectMany(
                        c => connection.Query<SubCategory>(),
                        (c, cat) => new { CustomerName = c.Name, CategoryName = cat.Name })
                    .ToList();

                // 2 Tokyo customers (Alice, Charlie) × 2 categories = 4 rows
                Assert.Equal(4, result.Count);
            }
        }

        // =====================================================
        // Combined tests
        // =====================================================

        [Fact]
        public void WhereInSubqueryWithAdditionalFilterTest()
        {
            SeedData();

            using (var connection = OpenNewConnection())
            {
                // Tokyo customers who have orders
                var customerIdsWithOrders = connection.Query<SubOrder>()
                    .Select(o => o.CustomerId);

                var result = connection.Query<SubCustomer>()
                    .Where(c => customerIdsWithOrders.Contains(c.Id) && c.City == "Tokyo")
                    .ToList();

                // Alice and Charlie are in Tokyo and have orders
                Assert.Equal(2, result.Count);
                Assert.Contains(result, c => c.Name == "Alice");
                Assert.Contains(result, c => c.Name == "Charlie");
            }
        }

        [Fact]
        public void WhereExistsSubqueryWithAdditionalFilterTest()
        {
            SeedData();

            using (var connection = OpenNewConnection())
            {
                // Tokyo customers who have orders
                var result = connection.Query<SubCustomer>()
                    .Where(c => c.City == "Tokyo"
                        && connection.Query<SubOrder>().Any(o => o.CustomerId == c.Id))
                    .ToList();

                // Alice and Charlie are in Tokyo and have orders
                Assert.Equal(2, result.Count);
                Assert.Contains(result, c => c.Name == "Alice");
                Assert.Contains(result, c => c.Name == "Charlie");
            }
        }
    }
}
