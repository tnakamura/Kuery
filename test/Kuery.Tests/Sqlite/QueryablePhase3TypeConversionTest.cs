using System;
using System.Linq;
using Xunit;

namespace Kuery.Tests.Sqlite
{
    public class QueryablePhase3TypeConversionTest : IClassFixture<SqliteFixture>, IDisposable
    {
        readonly SqliteFixture fixture;

        public QueryablePhase3TypeConversionTest(SqliteFixture fixture)
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
                    command.CommandText = "DROP TABLE IF EXISTS ConvertItem";
                    command.ExecuteNonQuery();
                }
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = "DROP TABLE IF EXISTS LikeItem";
                    command.ExecuteNonQuery();
                }
            }
        }

        // --- Model classes ---

        [Table("ConvertItem")]
        public class ConvertItem
        {
            [PrimaryKey]
            [Column("Id")]
            public int Id { get; set; }

            [Column("TextValue")]
            public string TextValue { get; set; }

            [Column("RealValue")]
            public double RealValue { get; set; }

            [Column("IntValue")]
            public int IntValue { get; set; }
        }

        [Table("LikeItem")]
        public class LikeItem
        {
            [PrimaryKey]
            [Column("Id")]
            public int Id { get; set; }

            [Column("Name")]
            public string Name { get; set; }
        }

        // --- Setup helpers ---

        private void CreateConvertItemTable()
        {
            using (var connection = fixture.CreateConnection())
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = @"
                        CREATE TABLE IF NOT EXISTS ConvertItem (
                            Id INTEGER PRIMARY KEY NOT NULL,
                            TextValue TEXT,
                            RealValue REAL NOT NULL,
                            IntValue INTEGER NOT NULL
                        )";
                    command.ExecuteNonQuery();
                }
            }
        }

        private void SeedConvertItems()
        {
            CreateConvertItemTable();
            using (var connection = fixture.CreateConnection())
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = @"
                        INSERT INTO ConvertItem (Id, TextValue, RealValue, IntValue) VALUES
                            (1, '100', 1.5, 10),
                            (2, '200', 2.7, 20),
                            (3, '50',  3.0, 30),
                            (4, '999', 4.9, 0)";
                    command.ExecuteNonQuery();
                }
            }
        }

        private void CreateLikeItemTable()
        {
            using (var connection = fixture.CreateConnection())
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = @"
                        CREATE TABLE IF NOT EXISTS LikeItem (
                            Id INTEGER PRIMARY KEY NOT NULL,
                            Name TEXT NOT NULL
                        )";
                    command.ExecuteNonQuery();
                }
            }
        }

        private void SeedLikeItems()
        {
            CreateLikeItemTable();
            using (var connection = fixture.CreateConnection())
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = @"
                        INSERT INTO LikeItem (Id, Name) VALUES
                            (1, 'Alice'),
                            (2, 'Bob'),
                            (3, 'Charlie'),
                            (4, 'ALICE'),
                            (5, 'David')";
                    command.ExecuteNonQuery();
                }
            }
        }

        // =====================================================
        // 3-1: Convert.ToInt32() → CAST(col AS integer)
        // =====================================================

        [Fact]
        public void ConvertToInt32Test()
        {
            SeedConvertItems();

            using (var connection = fixture.OpenNewConnection())
            {
                // Convert.ToInt32(x.RealValue) truncates to int; filter > 2
                var result = connection.Query<ConvertItem>()
                    .Where(x => Convert.ToInt32(x.RealValue) > 2)
                    .OrderBy(x => x.Id)
                    .ToList();

                // Id=3 (3.0→3), Id=4 (4.9→4)
                Assert.Equal(2, result.Count);
                Assert.Equal(3, result[0].Id);
                Assert.Equal(4, result[1].Id);
            }
        }

        [Fact]
        public void ConvertToInt32EqualsTest()
        {
            SeedConvertItems();

            using (var connection = fixture.OpenNewConnection())
            {
                var result = connection.Query<ConvertItem>()
                    .Where(x => Convert.ToInt32(x.RealValue) == 3)
                    .ToList();

                // Id=3 (3.0→3)
                Assert.Single(result);
                Assert.Equal(3, result[0].Id);
            }
        }

        // =====================================================
        // 3-1: Convert.ToInt64() → CAST(col AS integer)
        // =====================================================

        [Fact]
        public void ConvertToInt64Test()
        {
            SeedConvertItems();

            using (var connection = fixture.OpenNewConnection())
            {
                var result = connection.Query<ConvertItem>()
                    .Where(x => Convert.ToInt64(x.RealValue) >= 3)
                    .OrderBy(x => x.Id)
                    .ToList();

                Assert.Equal(2, result.Count);
                Assert.Equal(3, result[0].Id);
                Assert.Equal(4, result[1].Id);
            }
        }

        // =====================================================
        // 3-1: Convert.ToDouble() → CAST(col AS real)
        // =====================================================

        [Fact]
        public void ConvertToDoubleTest()
        {
            SeedConvertItems();

            using (var connection = fixture.OpenNewConnection())
            {
                // Cast IntValue to double and compare > 15.0
                var result = connection.Query<ConvertItem>()
                    .Where(x => Convert.ToDouble(x.IntValue) > 15.0)
                    .OrderBy(x => x.Id)
                    .ToList();

                // Id=2 (20), Id=3 (30)
                Assert.Equal(2, result.Count);
                Assert.Equal(2, result[0].Id);
                Assert.Equal(3, result[1].Id);
            }
        }

        // =====================================================
        // 3-1: Convert.ToBoolean() → CAST(col AS integer)
        // =====================================================

        [Fact]
        public void ConvertToBooleanTest()
        {
            SeedConvertItems();

            using (var connection = fixture.OpenNewConnection())
            {
                // Convert.ToBoolean(x.IntValue) → CAST(IntValue AS integer)
                // In SQLite, 0 is false, non-zero is true
                var result = connection.Query<ConvertItem>()
                    .Where(x => Convert.ToBoolean(x.IntValue) == false)
                    .ToList();

                // Id=4 (IntValue=0 → false)
                Assert.Single(result);
                Assert.Equal(4, result[0].Id);
            }
        }

        // =====================================================
        // 3-1: Convert.ToString() → CAST(col AS text)
        // =====================================================

        [Fact]
        public void ConvertToStringTest()
        {
            SeedConvertItems();

            using (var connection = fixture.OpenNewConnection())
            {
                var result = connection.Query<ConvertItem>()
                    .Where(x => Convert.ToString(x.IntValue) == "10")
                    .ToList();

                Assert.Single(result);
                Assert.Equal(1, result[0].Id);
            }
        }

        // =====================================================
        // 3-2: ToString() → CAST(col AS text)
        // =====================================================

        [Fact]
        public void ToStringTest()
        {
            SeedConvertItems();

            using (var connection = fixture.OpenNewConnection())
            {
                var result = connection.Query<ConvertItem>()
                    .Where(x => x.IntValue.ToString() == "20")
                    .ToList();

                Assert.Single(result);
                Assert.Equal(2, result[0].Id);
            }
        }

        [Fact]
        public void ToStringWithStringMethodTest()
        {
            SeedConvertItems();

            using (var connection = fixture.OpenNewConnection())
            {
                // Chain: x.IntValue.ToString() contains "0"
                var result = connection.Query<ConvertItem>()
                    .Where(x => x.IntValue.ToString().Contains("0"))
                    .OrderBy(x => x.Id)
                    .ToList();

                // Id=1 ("10"), Id=2 ("20"), Id=3 ("30"), Id=4 ("0")
                Assert.Equal(4, result.Count);
            }
        }

        // =====================================================
        // 3-3: KueryFunctions.Like() → LIKE
        // =====================================================

        [Fact]
        public void LikeWithPercentWildcardTest()
        {
            SeedLikeItems();

            using (var connection = fixture.OpenNewConnection())
            {
                // Names containing 'li' (case-insensitive in SQLite LIKE)
                var result = connection.Query<LikeItem>()
                    .Where(x => KueryFunctions.Like(x.Name, "%li%"))
                    .OrderBy(x => x.Id)
                    .ToList();

                // SQLite LIKE is case-insensitive for ASCII by default
                // Alice, Charlie, ALICE all contain "li" (case-insensitive)
                Assert.Equal(3, result.Count);
                Assert.Equal(1, result[0].Id);  // Alice
                Assert.Equal(3, result[1].Id);  // Charlie
                Assert.Equal(4, result[2].Id);  // ALICE
            }
        }

        [Fact]
        public void LikeWithStartPatternTest()
        {
            SeedLikeItems();

            using (var connection = fixture.OpenNewConnection())
            {
                var result = connection.Query<LikeItem>()
                    .Where(x => KueryFunctions.Like(x.Name, "A%"))
                    .OrderBy(x => x.Id)
                    .ToList();

                // Alice and ALICE (SQLite LIKE is case-insensitive for ASCII)
                Assert.Equal(2, result.Count);
                Assert.Equal(1, result[0].Id);
                Assert.Equal(4, result[1].Id);
            }
        }

        [Fact]
        public void LikeWithEndPatternTest()
        {
            SeedLikeItems();

            using (var connection = fixture.OpenNewConnection())
            {
                var result = connection.Query<LikeItem>()
                    .Where(x => KueryFunctions.Like(x.Name, "%b"))
                    .OrderBy(x => x.Id)
                    .ToList();

                // Bob
                Assert.Single(result);
                Assert.Equal(2, result[0].Id);
            }
        }

        [Fact]
        public void LikeWithUnderscoreWildcardTest()
        {
            SeedLikeItems();

            using (var connection = fixture.OpenNewConnection())
            {
                // _ matches exactly one character, so "_ob" matches "Bob"
                var result = connection.Query<LikeItem>()
                    .Where(x => KueryFunctions.Like(x.Name, "_ob"))
                    .ToList();

                Assert.Single(result);
                Assert.Equal(2, result[0].Id);
            }
        }

        [Fact]
        public void LikeWithExactPatternTest()
        {
            SeedLikeItems();

            using (var connection = fixture.OpenNewConnection())
            {
                var result = connection.Query<LikeItem>()
                    .Where(x => KueryFunctions.Like(x.Name, "David"))
                    .ToList();

                Assert.Single(result);
                Assert.Equal(5, result[0].Id);
            }
        }

        [Fact]
        public void LikeNoMatchTest()
        {
            SeedLikeItems();

            using (var connection = fixture.OpenNewConnection())
            {
                var result = connection.Query<LikeItem>()
                    .Where(x => KueryFunctions.Like(x.Name, "xyz%"))
                    .ToList();

                Assert.Empty(result);
            }
        }
    }
}
