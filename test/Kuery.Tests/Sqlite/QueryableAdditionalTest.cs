using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace Kuery.Tests.Sqlite
{
    public class QueryableAdditionalTest : IClassFixture<SqliteFixture>, IDisposable
    {
        readonly SqliteFixture fixture;

        public QueryableAdditionalTest(SqliteFixture fixture)
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
                    command.CommandText = "DROP TABLE IF EXISTS NullableItem";
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
                          VALUES (1, '1', 'Alice')
                               , (2, '2', 'Bob')
                               , (3, '3', 'Charlie')";
                    command.ExecuteNonQuery();
                }
            }
        }

        private void CreateNullableItemTable()
        {
            using (var connection = fixture.CreateConnection())
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = @"
                        CREATE TABLE IF NOT EXISTS NullableItem (
                            Id INTEGER PRIMARY KEY NOT NULL,
                            Score REAL NULL
                        )";
                    command.ExecuteNonQuery();
                }
            }
        }

        private void SeedNullableItems()
        {
            CreateNullableItemTable();
            using (var connection = fixture.OpenNewConnection())
            {
                connection.Insert(new NullableItem { Id = 1, Score = 10.0 });
                connection.Insert(new NullableItem { Id = 2, Score = null });
                connection.Insert(new NullableItem { Id = 3, Score = 30.0 });
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
                connection.Insert(new NumItem { Id = 1, Value = -10.0 });
                connection.Insert(new NumItem { Id = 2, Value = 20.0 });
                connection.Insert(new NumItem { Id = 3, Value = -30.0 });
            }
        }

        // --- Conditional (ternary) → CASE WHEN ---

        [Fact]
        public void ConditionalInWhereTest()
        {
            SeedThreeCustomers();

            using (var connection = fixture.OpenNewConnection())
            {
                // (x.Id > 1 ? x.Name : "none") == "Bob"
                var result = connection.Query<Customer>()
                    .Where(x => (x.Id > 1 ? x.Name : "none") == "Bob")
                    .ToList();

                Assert.Single(result);
                Assert.Equal(2, result[0].Id);
            }
        }

        [Fact]
        public void ConditionalInWhereWithConstantsTest()
        {
            SeedThreeCustomers();

            using (var connection = fixture.OpenNewConnection())
            {
                // (x.Id > 2 ? "high" : "low") == "high"
                var result = connection.Query<Customer>()
                    .Where(x => (x.Id > 2 ? "high" : "low") == "high")
                    .ToList();

                Assert.Single(result);
                Assert.Equal(3, result[0].Id);
            }
        }

        // --- Nullable<T>.HasValue ---

        [Fact]
        public void NullableHasValueTrueTest()
        {
            SeedNullableItems();

            using (var connection = fixture.OpenNewConnection())
            {
                var result = connection.Query<NullableItem>()
                    .Where(x => x.Score.HasValue)
                    .OrderBy(x => x.Id)
                    .ToList();

                Assert.Equal(2, result.Count);
                Assert.Equal(1, result[0].Id);
                Assert.Equal(3, result[1].Id);
            }
        }

        [Fact]
        public void NullableHasValueFalseTest()
        {
            SeedNullableItems();

            using (var connection = fixture.OpenNewConnection())
            {
                var result = connection.Query<NullableItem>()
                    .Where(x => !x.Score.HasValue)
                    .ToList();

                Assert.Single(result);
                Assert.Equal(2, result[0].Id);
            }
        }

        // --- String concatenation ---

        [Fact]
        public void StringConcatenationInWhereTest()
        {
            SeedThreeCustomers();

            using (var connection = fixture.OpenNewConnection())
            {
                var result = connection.Query<Customer>()
                    .Where(x => x.Code + x.Name == "2Bob")
                    .ToList();

                Assert.Single(result);
                Assert.Equal(2, result[0].Id);
            }
        }

        [Fact]
        public void StringConcatenationWithConstantTest()
        {
            SeedThreeCustomers();

            using (var connection = fixture.OpenNewConnection())
            {
                var result = connection.Query<Customer>()
                    .Where(x => x.Name + "!" == "Alice!")
                    .ToList();

                Assert.Single(result);
                Assert.Equal(1, result[0].Id);
            }
        }

        // --- String.IndexOf ---

        [Fact]
        public void StringIndexOfFoundTest()
        {
            SeedThreeCustomers();

            using (var connection = fixture.OpenNewConnection())
            {
                // "Alice".IndexOf("li") == 1 (0-based)
                var result = connection.Query<Customer>()
                    .Where(x => x.Name.IndexOf("li") == 1)
                    .ToList();

                Assert.Single(result);
                Assert.Equal("Alice", result[0].Name);
            }
        }

        [Fact]
        public void StringIndexOfNotFoundTest()
        {
            SeedThreeCustomers();

            using (var connection = fixture.OpenNewConnection())
            {
                // IndexOf returns -1 when not found
                var result = connection.Query<Customer>()
                    .Where(x => x.Name.IndexOf("xyz") == -1)
                    .ToList();

                Assert.Equal(3, result.Count);
            }
        }

        [Fact]
        public void StringIndexOfGreaterThanOrEqualZeroTest()
        {
            SeedThreeCustomers();

            using (var connection = fixture.OpenNewConnection())
            {
                // Names containing "ob": "Bob"
                var result = connection.Query<Customer>()
                    .Where(x => x.Name.IndexOf("ob") >= 0)
                    .ToList();

                Assert.Single(result);
                Assert.Equal("Bob", result[0].Name);
            }
        }

        // --- Math.Abs ---

        [Fact]
        public void MathAbsInWhereTest()
        {
            SeedNumItems();

            using (var connection = fixture.OpenNewConnection())
            {
                // abs(Value) > 15 → Id=2 (|20|=20) and Id=3 (|-30|=30)
                var result = connection.Query<NumItem>()
                    .Where(x => Math.Abs(x.Value) > 15)
                    .OrderBy(x => x.Id)
                    .ToList();

                Assert.Equal(2, result.Count);
                Assert.Equal(2, result[0].Id);
                Assert.Equal(3, result[1].Id);
            }
        }

        [Fact]
        public void MathAbsEqualTest()
        {
            SeedNumItems();

            using (var connection = fixture.OpenNewConnection())
            {
                // abs(Value) == 10 → Id=1 (|-10|=10)
                var result = connection.Query<NumItem>()
                    .Where(x => Math.Abs(x.Value) == 10)
                    .ToList();

                Assert.Single(result);
                Assert.Equal(1, result[0].Id);
            }
        }

        // --- Model classes ---

        [Table("NullableItem")]
        public class NullableItem
        {
            [PrimaryKey]
            [Column("Id")]
            public int Id { get; set; }

            [Column("Score")]
            public double? Score { get; set; }
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
