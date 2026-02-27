using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace Kuery.Tests.Sqlite
{
    public class QueryableNewFeaturesTest : IClassFixture<SqliteFixture>, IDisposable
    {
        readonly SqliteFixture fixture;

        public QueryableNewFeaturesTest(SqliteFixture fixture)
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
                connection.Insert(new StringProduct { Name = "  Baz  " });
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

        // --- string.Trim ---

        [Fact]
        public void StringTrimTest()
        {
            SeedProducts();

            using (var connection = fixture.OpenNewConnection())
            {
                var result = connection.Query<StringProduct>()
                    .Where(x => x.Name.Trim() == "Baz")
                    .ToList();

                Assert.Single(result);
                Assert.Equal("  Baz  ", result[0].Name);
            }
        }

        // --- string.TrimStart ---

        [Fact]
        public void StringTrimStartTest()
        {
            SeedProducts();

            using (var connection = fixture.OpenNewConnection())
            {
                var result = connection.Query<StringProduct>()
                    .Where(x => x.Name.TrimStart() == "Baz  ")
                    .ToList();

                Assert.Single(result);
                Assert.Equal("  Baz  ", result[0].Name);
            }
        }

        // --- string.TrimEnd ---

        [Fact]
        public void StringTrimEndTest()
        {
            SeedProducts();

            using (var connection = fixture.OpenNewConnection())
            {
                var result = connection.Query<StringProduct>()
                    .Where(x => x.Name.TrimEnd() == "  Baz")
                    .ToList();

                Assert.Single(result);
                Assert.Equal("  Baz  ", result[0].Name);
            }
        }

        // --- string.Length ---

        [Fact]
        public void StringLengthEqualTest()
        {
            SeedThreeCustomers();

            using (var connection = fixture.OpenNewConnection())
            {
                var result = connection.Query<Customer>()
                    .Where(x => x.Name.Length == 3)
                    .ToList();

                Assert.Equal(3, result.Count);
            }
        }

        [Fact]
        public void StringLengthGreaterThanTest()
        {
            SeedProducts();

            using (var connection = fixture.OpenNewConnection())
            {
                // "Foo" (3), "Bar" (3), "  Baz  " (7)
                var result = connection.Query<StringProduct>()
                    .Where(x => x.Name.Length > 3)
                    .ToList();

                Assert.Single(result);
                Assert.Equal("  Baz  ", result[0].Name);
            }
        }

        // --- string.Substring ---

        [Fact]
        public void StringSubstringWithStartIndexTest()
        {
            SeedThreeCustomers();

            using (var connection = fixture.OpenNewConnection())
            {
                // Name.Substring(1) takes from index 1 → "aa", "bb", "cc"
                var result = connection.Query<Customer>()
                    .Where(x => x.Name.Substring(1) == "aa")
                    .ToList();

                Assert.Single(result);
                Assert.Equal("aaa", result[0].Name);
            }
        }

        [Fact]
        public void StringSubstringWithStartIndexAndLengthTest()
        {
            SeedThreeCustomers();

            using (var connection = fixture.OpenNewConnection())
            {
                // Name.Substring(0, 2) → "aa", "bb", "cc"
                var result = connection.Query<Customer>()
                    .Where(x => x.Name.Substring(0, 2) == "bb")
                    .ToList();

                Assert.Single(result);
                Assert.Equal("bbb", result[0].Name);
            }
        }

        // --- Unary negate ---

        [Fact]
        public void UnaryNegateTest()
        {
            SeedNumItems();

            using (var connection = fixture.OpenNewConnection())
            {
                // -Value < -15 → Value > 15 → Id=2 (20) and Id=3 (30)
                var result = connection.Query<NumItem>()
                    .Where(x => -x.Value < -15)
                    .OrderBy(x => x.Id)
                    .ToList();

                Assert.Equal(2, result.Count);
                Assert.Equal(2, result[0].Id);
                Assert.Equal(3, result[1].Id);
            }
        }

        // --- All ---

        [Fact]
        public void AllReturnsTrueWhenAllMatchTest()
        {
            SeedThreeCustomers();

            using (var connection = fixture.OpenNewConnection())
            {
                var result = connection.Query<Customer>()
                    .All(x => x.Id > 0);

                Assert.True(result);
            }
        }

        [Fact]
        public void AllReturnsFalseWhenNotAllMatchTest()
        {
            SeedThreeCustomers();

            using (var connection = fixture.OpenNewConnection())
            {
                var result = connection.Query<Customer>()
                    .All(x => x.Id > 1);

                Assert.False(result);
            }
        }

        [Fact]
        public void AllReturnsTrueWhenEmptyTest()
        {
            using (var connection = fixture.OpenNewConnection())
            {
                var result = connection.Query<Customer>()
                    .All(x => x.Id > 100);

                Assert.True(result);
            }
        }

        [Fact]
        public void AllWithWhereTest()
        {
            SeedThreeCustomers();

            using (var connection = fixture.OpenNewConnection())
            {
                // Among customers with Id > 1, are all codes > "1"?
                var result = connection.Query<Customer>()
                    .Where(x => x.Id > 1)
                    .All(x => x.Code != "1");

                Assert.True(result);
            }
        }

        // --- string.Trim with Contains ---

        [Fact]
        public void StringTrimContainsTest()
        {
            SeedProducts();

            using (var connection = fixture.OpenNewConnection())
            {
                var result = connection.Query<StringProduct>()
                    .Where(x => x.Name.Trim().Contains("az"))
                    .ToList();

                Assert.Single(result);
                Assert.Equal("  Baz  ", result[0].Name);
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
    }
}
