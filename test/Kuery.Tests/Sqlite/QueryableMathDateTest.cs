using System;
using System.Linq;
using Xunit;

namespace Kuery.Tests.Sqlite
{
    public class QueryableMathDateTest : IClassFixture<SqliteFixture>, IDisposable
    {
        readonly SqliteFixture fixture;

        public QueryableMathDateTest(SqliteFixture fixture)
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
                    command.CommandText = "DROP TABLE IF EXISTS MathItem";
                    command.ExecuteNonQuery();
                }
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = "DROP TABLE IF EXISTS DateItem";
                    command.ExecuteNonQuery();
                }
            }
        }

        private void CreateMathItemTable()
        {
            using (var connection = fixture.CreateConnection())
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = @"
                        CREATE TABLE IF NOT EXISTS MathItem (
                            Id INTEGER PRIMARY KEY NOT NULL,
                            Value REAL NOT NULL
                        )";
                    command.ExecuteNonQuery();
                }
            }
        }

        private void SeedMathItems()
        {
            CreateMathItemTable();
            using (var connection = fixture.OpenNewConnection())
            {
                connection.Insert(new MathItem { Id = 1, Value = 2.3 });
                connection.Insert(new MathItem { Id = 2, Value = 2.7 });
                connection.Insert(new MathItem { Id = 3, Value = -1.5 });
                connection.Insert(new MathItem { Id = 4, Value = 3.0 });
            }
        }

        private void CreateDateItemTable()
        {
            using (var connection = fixture.CreateConnection())
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = @"
                        CREATE TABLE IF NOT EXISTS DateItem (
                            Id INTEGER PRIMARY KEY NOT NULL,
                            Created TEXT NOT NULL
                        )";
                    command.ExecuteNonQuery();
                }
            }
        }

        private void SeedDateItems()
        {
            CreateDateItemTable();
            using (var connection = fixture.CreateConnection())
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    // Insert dates directly as ISO 8601 strings for SQLite
                    command.CommandText = @"
                        INSERT INTO DateItem (Id, Created) VALUES
                            (1, '2024-03-15 10:30:45'),
                            (2, '2024-07-20 14:00:00'),
                            (3, '2023-12-25 08:15:30')";
                    command.ExecuteNonQuery();
                }
            }
        }

        // --- Math.Round ---

        [Fact]
        public void MathRoundTest()
        {
            SeedMathItems();

            using (var connection = fixture.OpenNewConnection())
            {
                // round(2.3)=2, round(2.7)=3, round(-1.5)=-2, round(3.0)=3
                var result = connection.Query<MathItem>()
                    .Where(x => Math.Round(x.Value) == 3)
                    .OrderBy(x => x.Id)
                    .ToList();

                Assert.Equal(2, result.Count);
                Assert.Equal(2, result[0].Id);
                Assert.Equal(4, result[1].Id);
            }
        }

        [Fact]
        public void MathRoundWithDigitsTest()
        {
            CreateMathItemTable();
            using (var connection = fixture.OpenNewConnection())
            {
                connection.Insert(new MathItem { Id = 10, Value = 2.345 });
                connection.Insert(new MathItem { Id = 11, Value = 2.355 });
            }

            using (var connection = fixture.OpenNewConnection())
            {
                // round(2.345, 1)=2.3, round(2.355, 1)=2.4
                var result = connection.Query<MathItem>()
                    .Where(x => Math.Round(x.Value, 1) == 2.3)
                    .ToList();

                Assert.Single(result);
                Assert.Equal(10, result[0].Id);
            }
        }

        // --- Math.Floor ---

        [Fact]
        public void MathFloorTest()
        {
            SeedMathItems();

            using (var connection = fixture.OpenNewConnection())
            {
                // floor(2.3)=2, floor(2.7)=2, floor(-1.5)=-2, floor(3.0)=3
                var result = connection.Query<MathItem>()
                    .Where(x => Math.Floor(x.Value) == 2)
                    .OrderBy(x => x.Id)
                    .ToList();

                Assert.Equal(2, result.Count);
                Assert.Equal(1, result[0].Id);
                Assert.Equal(2, result[1].Id);
            }
        }

        [Fact]
        public void MathFloorNegativeTest()
        {
            SeedMathItems();

            using (var connection = fixture.OpenNewConnection())
            {
                // floor(-1.5) = -2
                var result = connection.Query<MathItem>()
                    .Where(x => Math.Floor(x.Value) == -2)
                    .ToList();

                Assert.Single(result);
                Assert.Equal(3, result[0].Id);
            }
        }

        // --- Math.Ceiling ---

        [Fact]
        public void MathCeilingTest()
        {
            SeedMathItems();

            using (var connection = fixture.OpenNewConnection())
            {
                // ceil(2.3)=3, ceil(2.7)=3, ceil(-1.5)=-1, ceil(3.0)=3
                var result = connection.Query<MathItem>()
                    .Where(x => Math.Ceiling(x.Value) == 3)
                    .OrderBy(x => x.Id)
                    .ToList();

                Assert.Equal(3, result.Count);
                Assert.Equal(1, result[0].Id);
                Assert.Equal(2, result[1].Id);
                Assert.Equal(4, result[2].Id);
            }
        }

        [Fact]
        public void MathCeilingNegativeTest()
        {
            SeedMathItems();

            using (var connection = fixture.OpenNewConnection())
            {
                // ceil(-1.5) = -1
                var result = connection.Query<MathItem>()
                    .Where(x => Math.Ceiling(x.Value) == -1)
                    .ToList();

                Assert.Single(result);
                Assert.Equal(3, result[0].Id);
            }
        }

        // --- Math.Max / Math.Min ---

        [Fact]
        public void MathMaxTest()
        {
            SeedMathItems();

            using (var connection = fixture.OpenNewConnection())
            {
                // max(Value, 2.5): max(2.3,2.5)=2.5, max(2.7,2.5)=2.7, max(-1.5,2.5)=2.5, max(3.0,2.5)=3.0
                // > 2.5 → Id=2 (2.7) and Id=4 (3.0)
                var result = connection.Query<MathItem>()
                    .Where(x => Math.Max(x.Value, 2.5) > 2.5)
                    .OrderBy(x => x.Id)
                    .ToList();

                Assert.Equal(2, result.Count);
                Assert.Equal(2, result[0].Id);
                Assert.Equal(4, result[1].Id);
            }
        }

        [Fact]
        public void MathMinTest()
        {
            SeedMathItems();

            using (var connection = fixture.OpenNewConnection())
            {
                // min(Value, 2.5): min(2.3,2.5)=2.3, min(2.7,2.5)=2.5, min(-1.5,2.5)=-1.5, min(3.0,2.5)=2.5
                // < 0 → only Id=3 (-1.5)
                var result = connection.Query<MathItem>()
                    .Where(x => Math.Min(x.Value, 2.5) < 0)
                    .ToList();

                Assert.Single(result);
                Assert.Equal(3, result[0].Id);
            }
        }

        // --- DateTime.Year ---

        [Fact]
        public void DateTimeYearTest()
        {
            SeedDateItems();

            using (var connection = fixture.OpenNewConnection())
            {
                var result = connection.Query<DateItem>()
                    .Where(x => x.Created.Year == 2024)
                    .OrderBy(x => x.Id)
                    .ToList();

                Assert.Equal(2, result.Count);
                Assert.Equal(1, result[0].Id);
                Assert.Equal(2, result[1].Id);
            }
        }

        // --- DateTime.Month ---

        [Fact]
        public void DateTimeMonthTest()
        {
            SeedDateItems();

            using (var connection = fixture.OpenNewConnection())
            {
                var result = connection.Query<DateItem>()
                    .Where(x => x.Created.Month == 7)
                    .ToList();

                Assert.Single(result);
                Assert.Equal(2, result[0].Id);
            }
        }

        // --- DateTime.Day ---

        [Fact]
        public void DateTimeDayTest()
        {
            SeedDateItems();

            using (var connection = fixture.OpenNewConnection())
            {
                var result = connection.Query<DateItem>()
                    .Where(x => x.Created.Day == 25)
                    .ToList();

                Assert.Single(result);
                Assert.Equal(3, result[0].Id);
            }
        }

        // --- DateTime.Hour ---

        [Fact]
        public void DateTimeHourTest()
        {
            SeedDateItems();

            using (var connection = fixture.OpenNewConnection())
            {
                var result = connection.Query<DateItem>()
                    .Where(x => x.Created.Hour == 14)
                    .ToList();

                Assert.Single(result);
                Assert.Equal(2, result[0].Id);
            }
        }

        // --- DateTime.Minute ---

        [Fact]
        public void DateTimeMinuteTest()
        {
            SeedDateItems();

            using (var connection = fixture.OpenNewConnection())
            {
                var result = connection.Query<DateItem>()
                    .Where(x => x.Created.Minute == 30)
                    .ToList();

                Assert.Single(result);
                Assert.Equal(1, result[0].Id);
            }
        }

        // --- DateTime.Second ---

        [Fact]
        public void DateTimeSecondTest()
        {
            SeedDateItems();

            using (var connection = fixture.OpenNewConnection())
            {
                var result = connection.Query<DateItem>()
                    .Where(x => x.Created.Second == 30)
                    .ToList();

                Assert.Single(result);
                Assert.Equal(3, result[0].Id);
            }
        }

        // --- DateTime comparison with year ---

        [Fact]
        public void DateTimeYearGreaterThanTest()
        {
            SeedDateItems();

            using (var connection = fixture.OpenNewConnection())
            {
                var result = connection.Query<DateItem>()
                    .Where(x => x.Created.Year > 2023)
                    .OrderBy(x => x.Id)
                    .ToList();

                Assert.Equal(2, result.Count);
                Assert.Equal(1, result[0].Id);
                Assert.Equal(2, result[1].Id);
            }
        }

        // --- Model classes ---

        [Table("MathItem")]
        public class MathItem
        {
            [PrimaryKey]
            [Column("Id")]
            public int Id { get; set; }

            [Column("Value")]
            public double Value { get; set; }
        }

        [Table("DateItem")]
        public class DateItem
        {
            [PrimaryKey]
            [Column("Id")]
            public int Id { get; set; }

            [Column("Created")]
            public DateTime Created { get; set; }
        }
    }
}
