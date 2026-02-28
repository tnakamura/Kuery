using System;
using System.Linq;
using Xunit;

namespace Kuery.Tests.Sqlite
{
    public class QueryablePhase2ExprTest : IClassFixture<SqliteFixture>, IDisposable
    {
        readonly SqliteFixture fixture;

        public QueryablePhase2ExprTest(SqliteFixture fixture)
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
                    command.CommandText = "DROP TABLE IF EXISTS NullableItem";
                    command.ExecuteNonQuery();
                }
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = "DROP TABLE IF EXISTS WhitespaceItem";
                    command.ExecuteNonQuery();
                }
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = "DROP TABLE IF EXISTS DateItem2";
                    command.ExecuteNonQuery();
                }
            }
        }

        // --- Model classes ---

        [Table("NullableItem")]
        public class NullableItem
        {
            [PrimaryKey]
            [Column("Id")]
            public int Id { get; set; }

            [Column("Name")]
            public string Name { get; set; }

            [Column("Score")]
            public int? Score { get; set; }
        }

        [Table("WhitespaceItem")]
        public class WhitespaceItem
        {
            [PrimaryKey]
            [Column("Id")]
            public int Id { get; set; }

            [Column("Value")]
            public string Value { get; set; }
        }

        [Table("DateItem2")]
        public class DateItem2
        {
            [PrimaryKey]
            [Column("Id")]
            public int Id { get; set; }

            [Column("Created")]
            public DateTime Created { get; set; }
        }

        // --- Setup helpers ---

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
                            Name TEXT,
                            Score INTEGER
                        )";
                    command.ExecuteNonQuery();
                }
            }
        }

        private void SeedNullableItems()
        {
            CreateNullableItemTable();
            using (var connection = fixture.CreateConnection())
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = @"
                        INSERT INTO NullableItem (Id, Name, Score) VALUES
                            (1, 'Alice', 80),
                            (2, NULL, 90),
                            (3, 'Charlie', NULL),
                            (4, NULL, NULL)";
                    command.ExecuteNonQuery();
                }
            }
        }

        private void CreateWhitespaceItemTable()
        {
            using (var connection = fixture.CreateConnection())
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = @"
                        CREATE TABLE IF NOT EXISTS WhitespaceItem (
                            Id INTEGER PRIMARY KEY NOT NULL,
                            Value TEXT
                        )";
                    command.ExecuteNonQuery();
                }
            }
        }

        private void SeedWhitespaceItems()
        {
            CreateWhitespaceItemTable();
            using (var connection = fixture.CreateConnection())
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = @"
                        INSERT INTO WhitespaceItem (Id, Value) VALUES
                            (1, 'hello'),
                            (2, ''),
                            (3, '   '),
                            (4, NULL),
                            (5, 'world')";
                    command.ExecuteNonQuery();
                }
            }
        }

        private void CreateDateItem2Table()
        {
            using (var connection = fixture.CreateConnection())
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = @"
                        CREATE TABLE IF NOT EXISTS DateItem2 (
                            Id INTEGER PRIMARY KEY NOT NULL,
                            Created TEXT NOT NULL
                        )";
                    command.ExecuteNonQuery();
                }
            }
        }

        private void SeedDateItems()
        {
            CreateDateItem2Table();
            using (var connection = fixture.CreateConnection())
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = @"
                        INSERT INTO DateItem2 (Id, Created) VALUES
                            (1, '2024-01-15 10:30:00'),
                            (2, '2024-06-20 14:00:00'),
                            (3, '2024-12-25 08:15:30')";
                    command.ExecuteNonQuery();
                }
            }
        }

        // =====================================================
        // 2-1: Null coalescing operator (??) → COALESCE
        // =====================================================

        [Fact]
        public void CoalesceStringWithDefaultTest()
        {
            SeedNullableItems();

            using (var connection = fixture.OpenNewConnection())
            {
                // Name ?? "Unknown" → COALESCE(Name, 'Unknown')
                var result = connection.Query<NullableItem>()
                    .Where(x => (x.Name ?? "Unknown") == "Unknown")
                    .OrderBy(x => x.Id)
                    .ToList();

                // Id=2 (NULL→"Unknown") and Id=4 (NULL→"Unknown")
                Assert.Equal(2, result.Count);
                Assert.Equal(2, result[0].Id);
                Assert.Equal(4, result[1].Id);
            }
        }

        [Fact]
        public void CoalesceNullableIntWithDefaultTest()
        {
            SeedNullableItems();

            using (var connection = fixture.OpenNewConnection())
            {
                // Score ?? 0 → COALESCE(Score, 0)
                var result = connection.Query<NullableItem>()
                    .Where(x => (x.Score ?? 0) > 50)
                    .OrderBy(x => x.Id)
                    .ToList();

                // Id=1 (80>50), Id=2 (90>50), Id=3 (0>50=false), Id=4 (0>50=false)
                Assert.Equal(2, result.Count);
                Assert.Equal(1, result[0].Id);
                Assert.Equal(2, result[1].Id);
            }
        }

        [Fact]
        public void CoalesceNullableIntEqualsZeroTest()
        {
            SeedNullableItems();

            using (var connection = fixture.OpenNewConnection())
            {
                // Score ?? 0 == 0 → items with NULL score
                var result = connection.Query<NullableItem>()
                    .Where(x => (x.Score ?? 0) == 0)
                    .OrderBy(x => x.Id)
                    .ToList();

                // Id=3 (Score=NULL→0), Id=4 (Score=NULL→0)
                Assert.Equal(2, result.Count);
                Assert.Equal(3, result[0].Id);
                Assert.Equal(4, result[1].Id);
            }
        }

        // =====================================================
        // 2-2: string.IsNullOrWhiteSpace()
        // =====================================================

        [Fact]
        public void IsNullOrWhiteSpaceTest()
        {
            SeedWhitespaceItems();

            using (var connection = fixture.OpenNewConnection())
            {
                var result = connection.Query<WhitespaceItem>()
                    .Where(x => string.IsNullOrWhiteSpace(x.Value))
                    .OrderBy(x => x.Id)
                    .ToList();

                // Id=2 (empty), Id=3 (spaces only), Id=4 (null)
                Assert.Equal(3, result.Count);
                Assert.Equal(2, result[0].Id);
                Assert.Equal(3, result[1].Id);
                Assert.Equal(4, result[2].Id);
            }
        }

        [Fact]
        public void IsNotNullOrWhiteSpaceTest()
        {
            SeedWhitespaceItems();

            using (var connection = fixture.OpenNewConnection())
            {
                var result = connection.Query<WhitespaceItem>()
                    .Where(x => !string.IsNullOrWhiteSpace(x.Value))
                    .OrderBy(x => x.Id)
                    .ToList();

                // Id=1 ("hello"), Id=5 ("world")
                Assert.Equal(2, result.Count);
                Assert.Equal(1, result[0].Id);
                Assert.Equal(5, result[1].Id);
            }
        }

        // =====================================================
        // 2-3: DateTime.Now / DateTime.UtcNow
        // =====================================================

        [Fact]
        public void DateTimeNowComparisonTest()
        {
            CreateDateItem2Table();
            using (var connection = fixture.CreateConnection())
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    // Insert a date far in the future and one in the past
                    command.CommandText = @"
                        INSERT INTO DateItem2 (Id, Created) VALUES
                            (1, '2000-01-01 00:00:00'),
                            (2, '2099-12-31 23:59:59')";
                    command.ExecuteNonQuery();
                }
            }

            using (var connection = fixture.OpenNewConnection())
            {
                // Created < DateTime.Now → only past dates
                var result = connection.Query<DateItem2>()
                    .Where(x => x.Created < DateTime.Now)
                    .ToList();

                Assert.Single(result);
                Assert.Equal(1, result[0].Id);
            }
        }

        [Fact]
        public void DateTimeUtcNowComparisonTest()
        {
            CreateDateItem2Table();
            using (var connection = fixture.CreateConnection())
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = @"
                        INSERT INTO DateItem2 (Id, Created) VALUES
                            (1, '2000-01-01 00:00:00'),
                            (2, '2099-12-31 23:59:59')";
                    command.ExecuteNonQuery();
                }
            }

            using (var connection = fixture.OpenNewConnection())
            {
                // Created > DateTime.UtcNow → only future dates
                var result = connection.Query<DateItem2>()
                    .Where(x => x.Created > DateTime.UtcNow)
                    .ToList();

                Assert.Single(result);
                Assert.Equal(2, result[0].Id);
            }
        }

        // =====================================================
        // 2-4: DateTime.AddDays() etc.
        // =====================================================

        [Fact]
        public void DateTimeAddDaysTest()
        {
            SeedDateItems();

            using (var connection = fixture.OpenNewConnection())
            {
                // Created.AddDays(10) shifts: 2024-01-15→2024-01-25, 2024-06-20→2024-06-30, 2024-12-25→2025-01-04
                // Check that AddDays(10) year == 2025 (only Id=3)
                var result = connection.Query<DateItem2>()
                    .Where(x => x.Created.AddDays(10).Year == 2025)
                    .ToList();

                Assert.Single(result);
                Assert.Equal(3, result[0].Id);
            }
        }

        [Fact]
        public void DateTimeAddMonthsTest()
        {
            SeedDateItems();

            using (var connection = fixture.OpenNewConnection())
            {
                // Created.AddMonths(1): 2024-01→Feb, 2024-06→Jul, 2024-12→Jan 2025
                // Month == 7 after AddMonths(1) → Id=2 (Jun→Jul)
                var result = connection.Query<DateItem2>()
                    .Where(x => x.Created.AddMonths(1).Month == 7)
                    .ToList();

                Assert.Single(result);
                Assert.Equal(2, result[0].Id);
            }
        }

        [Fact]
        public void DateTimeAddYearsTest()
        {
            SeedDateItems();

            using (var connection = fixture.OpenNewConnection())
            {
                // Created.AddYears(2) → Year == 2026
                var result = connection.Query<DateItem2>()
                    .Where(x => x.Created.AddYears(2).Year == 2026)
                    .OrderBy(x => x.Id)
                    .ToList();

                Assert.Equal(3, result.Count);
            }
        }

        [Fact]
        public void DateTimeAddHoursTest()
        {
            SeedDateItems();

            using (var connection = fixture.OpenNewConnection())
            {
                // Created.AddHours(2): 10:30→12:30, 14:00→16:00, 08:15→10:15
                // Hour == 16 → Id=2
                var result = connection.Query<DateItem2>()
                    .Where(x => x.Created.AddHours(2).Hour == 16)
                    .ToList();

                Assert.Single(result);
                Assert.Equal(2, result[0].Id);
            }
        }

        [Fact]
        public void DateTimeAddMinutesTest()
        {
            SeedDateItems();

            using (var connection = fixture.OpenNewConnection())
            {
                // Created.AddMinutes(30): 10:30→11:00, 14:00→14:30, 08:15→08:45
                // Minute == 0 → Id=1
                var result = connection.Query<DateItem2>()
                    .Where(x => x.Created.AddMinutes(30).Minute == 0)
                    .ToList();

                Assert.Single(result);
                Assert.Equal(1, result[0].Id);
            }
        }

        [Fact]
        public void DateTimeAddSecondsTest()
        {
            SeedDateItems();

            using (var connection = fixture.OpenNewConnection())
            {
                // Created.AddSeconds(30): 10:30:00→10:30:30, 14:00:00→14:00:30, 08:15:30→08:16:00
                // Second == 0 → Id=3 (08:15:30+30s=08:16:00)
                var result = connection.Query<DateItem2>()
                    .Where(x => x.Created.AddSeconds(30).Second == 0)
                    .ToList();

                Assert.Single(result);
                Assert.Equal(3, result[0].Id);
            }
        }
    }
}
