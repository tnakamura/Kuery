using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace Kuery.Tests.Sqlite
{
    public class QueryableExtendedTest : IClassFixture<SqliteFixture>, IDisposable
    {
        readonly SqliteFixture fixture;

        public QueryableExtendedTest(SqliteFixture fixture)
        {
            this.fixture = fixture;
        }

        public void Dispose()
        {
            using (var connection = fixture.CreateConnection())
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = "DELETE FROM customers";
                    command.ExecuteNonQuery();
                }
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = "DROP TABLE IF EXISTS Product";
                    command.ExecuteNonQuery();
                }
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = "DROP TABLE IF EXISTS NumItem";
                    command.ExecuteNonQuery();
                }
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = "DROP TABLE IF EXISTS BoolItem";
                    command.ExecuteNonQuery();
                }
            }
        }

        private void SeedThreeCustomers()
        {
            using (var connection = fixture.CreateConnection())
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        @"INSERT INTO customers (id, code, name)
                          VALUES (1, '1', 'aaa')
                               , (2, '2', 'bbb')
                               , (3, '3', 'ccc')";
                    command.ExecuteNonQuery();
                }
            }
        }

        private void CreateProductTable()
        {
            using (var connection = fixture.CreateConnection())
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = @"
                        CREATE TABLE IF NOT EXISTS Product (
                            Name TEXT PRIMARY KEY NOT NULL
                        )";
                    command.ExecuteNonQuery();
                }
            }
        }

        private void SeedProducts()
        {
            CreateProductTable();
            using (var connection = fixture.OpenNewConnection())
            {
                connection.Insert(new StringProduct { Name = "Foo" });
                connection.Insert(new StringProduct { Name = "Bar" });
                connection.Insert(new StringProduct { Name = "Foobar" });
            }
        }

        private void CreateNumItemTable()
        {
            using (var connection = fixture.CreateConnection())
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = @"
                        CREATE TABLE IF NOT EXISTS NumItem (
                            Id INTEGER PRIMARY KEY NOT NULL,
                            Value REAL NOT NULL
                        )";
                    command.ExecuteNonQuery();
                }
            }
        }

        private void SeedNumItems()
        {
            CreateNumItemTable();
            using (var connection = fixture.OpenNewConnection())
            {
                connection.Insert(new NumItem { Id = 1, Value = 10.0 });
                connection.Insert(new NumItem { Id = 2, Value = 20.0 });
                connection.Insert(new NumItem { Id = 3, Value = 30.0 });
            }
        }

        // --- String.Contains ---

        [Fact]
        public void StringContainsTest()
        {
            SeedProducts();

            using (var connection = fixture.OpenNewConnection())
            {
                var result = connection.Query<StringProduct>()
                    .Where(x => x.Name.Contains("oo"))
                    .ToList();

                Assert.Equal(2, result.Count);
            }
        }

        [Fact]
        public void StringContainsNoMatchTest()
        {
            SeedProducts();

            using (var connection = fixture.OpenNewConnection())
            {
                var result = connection.Query<StringProduct>()
                    .Where(x => x.Name.Contains("z"))
                    .ToList();

                Assert.Empty(result);
            }
        }

        // --- String.StartsWith ---

        [Fact]
        public void StringStartsWithTest()
        {
            SeedProducts();

            using (var connection = fixture.OpenNewConnection())
            {
                var result = connection.Query<StringProduct>()
                    .Where(x => x.Name.StartsWith("F"))
                    .ToList();

                Assert.Equal(2, result.Count);
            }
        }

        [Fact]
        public void StringStartsWithCaseInsensitiveTest()
        {
            SeedProducts();

            using (var connection = fixture.OpenNewConnection())
            {
                var result = connection.Query<StringProduct>()
                    .Where(x => x.Name.StartsWith("f", StringComparison.OrdinalIgnoreCase))
                    .ToList();

                Assert.Equal(2, result.Count);
            }
        }

        // --- String.EndsWith ---

        [Fact]
        public void StringEndsWithTest()
        {
            SeedProducts();

            using (var connection = fixture.OpenNewConnection())
            {
                var result = connection.Query<StringProduct>()
                    .Where(x => x.Name.EndsWith("ar"))
                    .ToList();

                Assert.Equal(2, result.Count);
            }
        }

        [Fact]
        public void StringEndsWithNoMatchTest()
        {
            SeedProducts();

            using (var connection = fixture.OpenNewConnection())
            {
                var result = connection.Query<StringProduct>()
                    .Where(x => x.Name.EndsWith("Ar"))
                    .ToList();

                Assert.Empty(result);
            }
        }

        // --- ToUpper / ToLower in predicates ---

        [Fact]
        public void StringToUpperContainsTest()
        {
            SeedProducts();

            using (var connection = fixture.OpenNewConnection())
            {
                var result = connection.Query<StringProduct>()
                    .Where(x => x.Name.ToUpper().Contains("OO"))
                    .ToList();

                Assert.Equal(2, result.Count);
            }
        }

        [Fact]
        public void StringToLowerContainsTest()
        {
            SeedProducts();

            using (var connection = fixture.OpenNewConnection())
            {
                var result = connection.Query<StringProduct>()
                    .Where(x => x.Name.ToLower().Contains("bar"))
                    .ToList();

                Assert.Equal(2, result.Count);
            }
        }

        // --- Collection Contains (IN clause) ---

        [Fact]
        public void CollectionContainsArrayTest()
        {
            SeedThreeCustomers();

            using (var connection = fixture.OpenNewConnection())
            {
                var codes = new[] { "1", "3" };
                var result = connection.Query<Customer>()
                    .Where(x => codes.Contains(x.Code))
                    .ToList();

                Assert.Equal(2, result.Count);
                Assert.Contains(result, x => x.Code == "1");
                Assert.Contains(result, x => x.Code == "3");
            }
        }

        [Fact]
        public void CollectionContainsListTest()
        {
            SeedThreeCustomers();

            using (var connection = fixture.OpenNewConnection())
            {
                var ids = new List<int> { 2, 3 };
                var result = connection.Query<Customer>()
                    .Where(x => ids.Contains(x.Id))
                    .ToList();

                Assert.Equal(2, result.Count);
                Assert.Contains(result, x => x.Id == 2);
                Assert.Contains(result, x => x.Id == 3);
            }
        }

        // --- Distinct ---

        [Fact]
        public void DistinctTest()
        {
            using (var connection = fixture.OpenNewConnection())
            {
                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        @"INSERT INTO customers (id, code, name)
                          VALUES (1, 'A', 'aaa')
                               , (2, 'A', 'bbb')
                               , (3, 'B', 'ccc')";
                    command.ExecuteNonQuery();
                }

                var result = connection.Query<Customer>()
                    .Select(x => x.Code)
                    .Distinct()
                    .ToList();

                Assert.Equal(2, result.Count);
                Assert.Contains("A", result);
                Assert.Contains("B", result);
            }
        }

        // --- Single / SingleOrDefault ---

        [Fact]
        public void SingleTest()
        {
            SeedThreeCustomers();

            using (var connection = fixture.OpenNewConnection())
            {
                var result = connection.Query<Customer>()
                    .Single(x => x.Code == "2");

                Assert.Equal(2, result.Id);
                Assert.Equal("bbb", result.Name);
            }
        }

        [Fact]
        public void SingleThrowsWhenEmptyTest()
        {
            using (var connection = fixture.OpenNewConnection())
            {
                Assert.Throws<InvalidOperationException>(() =>
                    connection.Query<Customer>().Single());
            }
        }

        [Fact]
        public void SingleThrowsWhenMultipleTest()
        {
            SeedThreeCustomers();

            using (var connection = fixture.OpenNewConnection())
            {
                Assert.Throws<InvalidOperationException>(() =>
                    connection.Query<Customer>().Single());
            }
        }

        [Fact]
        public void SingleOrDefaultTest()
        {
            SeedThreeCustomers();

            using (var connection = fixture.OpenNewConnection())
            {
                var result = connection.Query<Customer>()
                    .SingleOrDefault(x => x.Code == "2");

                Assert.NotNull(result);
                Assert.Equal(2, result.Id);
            }
        }

        [Fact]
        public void SingleOrDefaultReturnsNullWhenEmptyTest()
        {
            using (var connection = fixture.OpenNewConnection())
            {
                var result = connection.Query<Customer>()
                    .SingleOrDefault();

                Assert.Null(result);
            }
        }

        [Fact]
        public void SingleOrDefaultThrowsWhenMultipleTest()
        {
            SeedThreeCustomers();

            using (var connection = fixture.OpenNewConnection())
            {
                Assert.Throws<InvalidOperationException>(() =>
                    connection.Query<Customer>().SingleOrDefault());
            }
        }

        // --- Sum ---

        [Fact]
        public void SumTest()
        {
            SeedNumItems();

            using (var connection = fixture.OpenNewConnection())
            {
                var result = connection.Query<NumItem>()
                    .Sum(x => x.Value);

                Assert.Equal(60.0, result);
            }
        }

        // --- Min ---

        [Fact]
        public void MinTest()
        {
            SeedNumItems();

            using (var connection = fixture.OpenNewConnection())
            {
                var result = connection.Query<NumItem>()
                    .Min(x => x.Value);

                Assert.Equal(10.0, result);
            }
        }

        // --- Max ---

        [Fact]
        public void MaxTest()
        {
            SeedNumItems();

            using (var connection = fixture.OpenNewConnection())
            {
                var result = connection.Query<NumItem>()
                    .Max(x => x.Value);

                Assert.Equal(30.0, result);
            }
        }

        // --- Average ---

        [Fact]
        public void AverageTest()
        {
            SeedNumItems();

            using (var connection = fixture.OpenNewConnection())
            {
                var result = connection.Query<NumItem>()
                    .Average(x => x.Value);

                Assert.Equal(20.0, result);
            }
        }

        // --- Sum/Min/Max with int column ---

        [Fact]
        public void SumIntTest()
        {
            SeedThreeCustomers();

            using (var connection = fixture.OpenNewConnection())
            {
                var result = connection.Query<Customer>()
                    .Sum(x => x.Id);

                Assert.Equal(6, result);
            }
        }

        [Fact]
        public void MinIntTest()
        {
            SeedThreeCustomers();

            using (var connection = fixture.OpenNewConnection())
            {
                var result = connection.Query<Customer>()
                    .Min(x => x.Id);

                Assert.Equal(1, result);
            }
        }

        [Fact]
        public void MaxIntTest()
        {
            SeedThreeCustomers();

            using (var connection = fixture.OpenNewConnection())
            {
                var result = connection.Query<Customer>()
                    .Max(x => x.Id);

                Assert.Equal(3, result);
            }
        }

        // --- Model classes ---

        [Table("Product")]
        public class StringProduct
        {
            [PrimaryKey]
            public string Name { get; set; }
        }

        [Table("NumItem")]
        public class NumItem
        {
            [PrimaryKey]
            [Column("Id")]
            public int Id { get; set; }

            [Column("Value")]
            public double Value { get; set; }
        }

        [Table("BoolItem")]
        public class BoolItem
        {
            [PrimaryKey]
            [Column("Id")]
            public int Id { get; set; }

            [Column("Name")]
            public string Name { get; set; }

            [Column("IsActive")]
            public bool IsActive { get; set; }
        }

        private void CreateBoolItemTable()
        {
            using (var connection = fixture.CreateConnection())
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = @"
                        CREATE TABLE IF NOT EXISTS BoolItem (
                            Id INTEGER PRIMARY KEY NOT NULL,
                            Name TEXT NOT NULL,
                            IsActive INTEGER NOT NULL
                        )";
                    command.ExecuteNonQuery();
                }
            }
        }

        private void SeedBoolItems()
        {
            CreateBoolItemTable();
            using (var connection = fixture.OpenNewConnection())
            {
                connection.Insert(new BoolItem { Id = 1, Name = "Active1", IsActive = true });
                connection.Insert(new BoolItem { Id = 2, Name = "Inactive", IsActive = false });
                connection.Insert(new BoolItem { Id = 3, Name = "Active2", IsActive = true });
            }
        }

        // --- Any ---

        [Fact]
        public void AnyReturnsTrueWhenMatchesExist()
        {
            SeedThreeCustomers();

            using (var connection = fixture.OpenNewConnection())
            {
                var result = connection.Query<Customer>().Any();

                Assert.True(result);
            }
        }

        [Fact]
        public void AnyReturnsFalseWhenEmpty()
        {
            using (var connection = fixture.OpenNewConnection())
            {
                var result = connection.Query<Customer>().Any();

                Assert.False(result);
            }
        }

        [Fact]
        public void AnyWithPredicateTest()
        {
            SeedThreeCustomers();

            using (var connection = fixture.OpenNewConnection())
            {
                Assert.True(connection.Query<Customer>().Any(x => x.Code == "2"));
                Assert.False(connection.Query<Customer>().Any(x => x.Code == "999"));
            }
        }

        // --- Bool property direct reference ---

        [Fact]
        public void WhereBoolPropertyDirectTest()
        {
            SeedBoolItems();

            using (var connection = fixture.OpenNewConnection())
            {
                var result = connection.Query<BoolItem>()
                    .Where(x => x.IsActive)
                    .ToList();

                Assert.Equal(2, result.Count);
                Assert.All(result, x => Assert.True(x.IsActive));
            }
        }

        [Fact]
        public void WhereNotBoolPropertyTest()
        {
            SeedBoolItems();

            using (var connection = fixture.OpenNewConnection())
            {
                var result = connection.Query<BoolItem>()
                    .Where(x => !x.IsActive)
                    .ToList();

                Assert.Single(result);
                Assert.Equal("Inactive", result[0].Name);
            }
        }

        // --- string.Replace ---

        [Fact]
        public void StringReplaceContainsTest()
        {
            SeedProducts();

            using (var connection = fixture.OpenNewConnection())
            {
                // "Foobar" → Replace("o","") → "Fbar", contains "Fb"
                var result = connection.Query<StringProduct>()
                    .Where(x => x.Name.Replace("o", "").Contains("Fb"))
                    .ToList();

                Assert.Single(result);
                Assert.Equal("Foobar", result[0].Name);
            }
        }

        [Fact]
        public void StringReplaceEqualsTest()
        {
            SeedProducts();

            using (var connection = fixture.OpenNewConnection())
            {
                // "Bar" → Replace("B","b") → "bar"
                var result = connection.Query<StringProduct>()
                    .Where(x => x.Name.Replace("B", "b") == "bar")
                    .ToList();

                Assert.Single(result);
                Assert.Equal("Bar", result[0].Name);
            }
        }
    }
}
