using System;
using System.Linq;
using Xunit;

namespace Kuery.Tests.Sqlite
{
    public class QueryablePhase6MathDateBitwiseTest : IClassFixture<SqliteFixture>, IDisposable
    {
        readonly SqliteFixture fixture;

        public QueryablePhase6MathDateBitwiseTest(SqliteFixture fixture)
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
                    command.CommandText = "DROP TABLE IF EXISTS Phase6MathItem";
                    command.ExecuteNonQuery();
                }
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = "DROP TABLE IF EXISTS Phase6DateItem";
                    command.ExecuteNonQuery();
                }
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = "DROP TABLE IF EXISTS Phase6BitwiseItem";
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
                        CREATE TABLE IF NOT EXISTS Phase6MathItem (
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
                connection.Insert(new Phase6MathItem { Id = 1, Value = 4.0 });
                connection.Insert(new Phase6MathItem { Id = 2, Value = 9.0 });
                connection.Insert(new Phase6MathItem { Id = 3, Value = 16.0 });
                connection.Insert(new Phase6MathItem { Id = 4, Value = 100.0 });
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
                        CREATE TABLE IF NOT EXISTS Phase6DateItem (
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
                    // 2024-03-15 is Friday (DayOfWeek=5)
                    // 2024-03-17 is Sunday (DayOfWeek=0)
                    // 2024-03-18 is Monday (DayOfWeek=1)
                    command.CommandText = @"
                        INSERT INTO Phase6DateItem (Id, Created) VALUES
                            (1, '2024-03-15 10:30:45'),
                            (2, '2024-03-17 14:00:00'),
                            (3, '2024-03-18 08:15:30')";
                    command.ExecuteNonQuery();
                }
            }
        }

        private void CreateBitwiseItemTable()
        {
            using (var connection = fixture.CreateConnection())
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = @"
                        CREATE TABLE IF NOT EXISTS Phase6BitwiseItem (
                            Id INTEGER PRIMARY KEY NOT NULL,
                            Flags INTEGER NOT NULL
                        )";
                    command.ExecuteNonQuery();
                }
            }
        }

        private void SeedBitwiseItems()
        {
            CreateBitwiseItemTable();
            using (var connection = fixture.OpenNewConnection())
            {
                connection.Insert(new Phase6BitwiseItem { Id = 1, Flags = 0b0011 }); // 3
                connection.Insert(new Phase6BitwiseItem { Id = 2, Flags = 0b0101 }); // 5
                connection.Insert(new Phase6BitwiseItem { Id = 3, Flags = 0b1100 }); // 12
                connection.Insert(new Phase6BitwiseItem { Id = 4, Flags = 0b1111 }); // 15
            }
        }

        // --- Math.Pow ---

        [Fact]
        public void MathPowTest()
        {
            SeedMathItems();

            using (var connection = fixture.OpenNewConnection())
            {
                // pow(4, 2)=16, pow(9, 2)=81, pow(16, 2)=256, pow(100, 2)=10000
                // > 100 → Id=2 (81 is not > 100, so Id=3 and Id=4)
                var result = connection.Query<Phase6MathItem>()
                    .Where(x => Math.Pow(x.Value, 2) > 100)
                    .OrderBy(x => x.Id)
                    .ToList();

                Assert.Equal(2, result.Count);
                Assert.Equal(3, result[0].Id);
                Assert.Equal(4, result[1].Id);
            }
        }

        [Fact]
        public void MathPowEqualTest()
        {
            SeedMathItems();

            using (var connection = fixture.OpenNewConnection())
            {
                // pow(4, 0.5)=2, pow(9, 0.5)=3, pow(16, 0.5)=4, pow(100, 0.5)=10
                var result = connection.Query<Phase6MathItem>()
                    .Where(x => Math.Pow(x.Value, 0.5) == 3)
                    .ToList();

                Assert.Single(result);
                Assert.Equal(2, result[0].Id);
            }
        }

        // --- Math.Sqrt ---

        [Fact]
        public void MathSqrtTest()
        {
            SeedMathItems();

            using (var connection = fixture.OpenNewConnection())
            {
                // sqrt(4)=2, sqrt(9)=3, sqrt(16)=4, sqrt(100)=10
                // == 3 → Id=2
                var result = connection.Query<Phase6MathItem>()
                    .Where(x => Math.Sqrt(x.Value) == 3)
                    .ToList();

                Assert.Single(result);
                Assert.Equal(2, result[0].Id);
            }
        }

        [Fact]
        public void MathSqrtGreaterThanTest()
        {
            SeedMathItems();

            using (var connection = fixture.OpenNewConnection())
            {
                // sqrt(4)=2, sqrt(9)=3, sqrt(16)=4, sqrt(100)=10
                // > 3 → Id=3 (4) and Id=4 (10)
                var result = connection.Query<Phase6MathItem>()
                    .Where(x => Math.Sqrt(x.Value) > 3)
                    .OrderBy(x => x.Id)
                    .ToList();

                Assert.Equal(2, result.Count);
                Assert.Equal(3, result[0].Id);
                Assert.Equal(4, result[1].Id);
            }
        }

        // --- Math.Log ---

        [Fact]
        public void MathLogTest()
        {
            CreateMathItemTable();
            using (var connection = fixture.OpenNewConnection())
            {
                // e ≈ 2.71828, e^2 ≈ 7.389
                connection.Insert(new Phase6MathItem { Id = 10, Value = Math.E });
                connection.Insert(new Phase6MathItem { Id = 11, Value = Math.E * Math.E });
            }

            using (var connection = fixture.OpenNewConnection())
            {
                // ln(e) = 1, ln(e^2) = 2
                // > 1.5 → only Id=11
                var result = connection.Query<Phase6MathItem>()
                    .Where(x => Math.Log(x.Value) > 1.5)
                    .ToList();

                Assert.Single(result);
                Assert.Equal(11, result[0].Id);
            }
        }

        [Fact]
        public void MathLogWithBaseTest()
        {
            SeedMathItems();

            using (var connection = fixture.OpenNewConnection())
            {
                // log(100, 10)=2 → == 2 → Id=4
                var result = connection.Query<Phase6MathItem>()
                    .Where(x => Math.Log(x.Value, 10) > 1.5)
                    .ToList();

                Assert.Single(result);
                Assert.Equal(4, result[0].Id);
            }
        }

        // --- Math.Log10 ---

        [Fact]
        public void MathLog10Test()
        {
            SeedMathItems();

            using (var connection = fixture.OpenNewConnection())
            {
                // log10(4)≈0.602, log10(9)≈0.954, log10(16)≈1.204, log10(100)=2
                // > 1 → Id=3 and Id=4
                var result = connection.Query<Phase6MathItem>()
                    .Where(x => Math.Log10(x.Value) > 1)
                    .OrderBy(x => x.Id)
                    .ToList();

                Assert.Equal(2, result.Count);
                Assert.Equal(3, result[0].Id);
                Assert.Equal(4, result[1].Id);
            }
        }

        [Fact]
        public void MathLog10EqualTest()
        {
            SeedMathItems();

            using (var connection = fixture.OpenNewConnection())
            {
                // log10(100) = 2.0
                var result = connection.Query<Phase6MathItem>()
                    .Where(x => Math.Log10(x.Value) == 2)
                    .ToList();

                Assert.Single(result);
                Assert.Equal(4, result[0].Id);
            }
        }

        // --- DateTime.Date ---

        [Fact]
        public void DateTimeDateTest()
        {
            SeedDateItems();

            using (var connection = fixture.OpenNewConnection())
            {
                // date('2024-03-15 10:30:45') = '2024-03-15'
                var result = connection.Query<Phase6DateItem>()
                    .Where(x => x.Created.Date == new DateTime(2024, 3, 15))
                    .ToList();

                Assert.Single(result);
                Assert.Equal(1, result[0].Id);
            }
        }

        [Fact]
        public void DateTimeDateFilterMultipleTest()
        {
            SeedDateItems();

            using (var connection = fixture.OpenNewConnection())
            {
                // All dates in March 2024 where Date >= 2024-03-17
                var result = connection.Query<Phase6DateItem>()
                    .Where(x => x.Created.Date >= new DateTime(2024, 3, 17))
                    .OrderBy(x => x.Id)
                    .ToList();

                Assert.Equal(2, result.Count);
                Assert.Equal(2, result[0].Id);
                Assert.Equal(3, result[1].Id);
            }
        }

        // --- DateTime.DayOfWeek ---

        [Fact]
        public void DateTimeDayOfWeekTest()
        {
            SeedDateItems();

            using (var connection = fixture.OpenNewConnection())
            {
                // 2024-03-15 is Friday (DayOfWeek=5)
                var result = connection.Query<Phase6DateItem>()
                    .Where(x => (int)x.Created.DayOfWeek == 5)
                    .ToList();

                Assert.Single(result);
                Assert.Equal(1, result[0].Id);
            }
        }

        [Fact]
        public void DateTimeDayOfWeekSundayTest()
        {
            SeedDateItems();

            using (var connection = fixture.OpenNewConnection())
            {
                // 2024-03-17 is Sunday (DayOfWeek=0)
                var result = connection.Query<Phase6DateItem>()
                    .Where(x => (int)x.Created.DayOfWeek == 0)
                    .ToList();

                Assert.Single(result);
                Assert.Equal(2, result[0].Id);
            }
        }

        [Fact]
        public void DateTimeDayOfWeekMondayTest()
        {
            SeedDateItems();

            using (var connection = fixture.OpenNewConnection())
            {
                // 2024-03-18 is Monday (DayOfWeek=1)
                var result = connection.Query<Phase6DateItem>()
                    .Where(x => (int)x.Created.DayOfWeek == 1)
                    .ToList();

                Assert.Single(result);
                Assert.Equal(3, result[0].Id);
            }
        }

        // --- Bitwise AND ---

        [Fact]
        public void BitwiseAndTest()
        {
            SeedBitwiseItems();

            using (var connection = fixture.OpenNewConnection())
            {
                // Flags & 0b0001 != 0 → items with bit 0 set: 3 (0b0011), 5 (0b0101), 15 (0b1111)
                var result = connection.Query<Phase6BitwiseItem>()
                    .Where(x => (x.Flags & 1) != 0)
                    .OrderBy(x => x.Id)
                    .ToList();

                Assert.Equal(3, result.Count);
                Assert.Equal(1, result[0].Id);
                Assert.Equal(2, result[1].Id);
                Assert.Equal(4, result[2].Id);
            }
        }

        [Fact]
        public void BitwiseAndEqualTest()
        {
            SeedBitwiseItems();

            using (var connection = fixture.OpenNewConnection())
            {
                // Flags & 0b0011 == 0b0011 → items where both lower bits set: 3 (0b0011), 15 (0b1111)
                var result = connection.Query<Phase6BitwiseItem>()
                    .Where(x => (x.Flags & 3) == 3)
                    .OrderBy(x => x.Id)
                    .ToList();

                Assert.Equal(2, result.Count);
                Assert.Equal(1, result[0].Id);
                Assert.Equal(4, result[1].Id);
            }
        }

        // --- Bitwise OR ---

        [Fact]
        public void BitwiseOrTest()
        {
            SeedBitwiseItems();

            using (var connection = fixture.OpenNewConnection())
            {
                // Flags | 0b0010 → 3|2=3, 5|2=7, 12|2=14, 15|2=15
                // == 7 → Id=2 (5|2=7)
                var result = connection.Query<Phase6BitwiseItem>()
                    .Where(x => (x.Flags | 2) == 7)
                    .ToList();

                Assert.Single(result);
                Assert.Equal(2, result[0].Id);
            }
        }

        // --- Bitwise XOR ---

        [Fact]
        public void BitwiseXorTest()
        {
            SeedBitwiseItems();

            using (var connection = fixture.OpenNewConnection())
            {
                // Flags ^ 0b1111 → 3^15=12, 5^15=10, 12^15=3, 15^15=0
                // == 0 → Id=4
                var result = connection.Query<Phase6BitwiseItem>()
                    .Where(x => (x.Flags ^ 15) == 0)
                    .ToList();

                Assert.Single(result);
                Assert.Equal(4, result[0].Id);
            }
        }

        // --- Bitwise NOT ---

        [Fact]
        public void BitwiseNotTest()
        {
            SeedBitwiseItems();

            using (var connection = fixture.OpenNewConnection())
            {
                // ~Flags & 0xFF: ~3 & 0xFF = 252, ~5 & 0xFF = 250, ~12 & 0xFF = 243, ~15 & 0xFF = 240
                // We check ~Flags & 0b1100 == 0b1100 → bits 2,3 NOT set in original: 3 (0b0011)
                var result = connection.Query<Phase6BitwiseItem>()
                    .Where(x => (~x.Flags & 12) == 12)
                    .OrderBy(x => x.Id)
                    .ToList();

                Assert.Single(result);
                Assert.Equal(1, result[0].Id);
            }
        }

        // --- Model classes ---

        [Table("Phase6MathItem")]
        public class Phase6MathItem
        {
            [PrimaryKey]
            [Column("Id")]
            public int Id { get; set; }

            [Column("Value")]
            public double Value { get; set; }
        }

        [Table("Phase6DateItem")]
        public class Phase6DateItem
        {
            [PrimaryKey]
            [Column("Id")]
            public int Id { get; set; }

            [Column("Created")]
            public DateTime Created { get; set; }
        }

        [Table("Phase6BitwiseItem")]
        public class Phase6BitwiseItem
        {
            [PrimaryKey]
            [Column("Id")]
            public int Id { get; set; }

            [Column("Flags")]
            public int Flags { get; set; }
        }
    }
}
