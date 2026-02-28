using System;
using System.Linq;
using Xunit;

namespace Kuery.Tests.Sqlite
{
    public class QueryablePhase4SetOperationTest : IClassFixture<SqliteFixture>, IDisposable
    {
        readonly SqliteFixture fixture;

        public QueryablePhase4SetOperationTest(SqliteFixture fixture)
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
                    command.CommandText = "DROP TABLE IF EXISTS SetOpItem";
                    command.ExecuteNonQuery();
                }
            }
        }

        // --- Model class ---

        [Table("SetOpItem")]
        public class SetOpItem
        {
            [PrimaryKey]
            [Column("Id")]
            public int Id { get; set; }

            [Column("Name")]
            public string Name { get; set; }

            [Column("Category")]
            public string Category { get; set; }
        }

        // --- Setup helpers ---

        private void CreateTable()
        {
            using (var connection = fixture.CreateConnection())
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = @"
                        CREATE TABLE IF NOT EXISTS SetOpItem (
                            Id INTEGER PRIMARY KEY NOT NULL,
                            Name TEXT NOT NULL,
                            Category TEXT NOT NULL
                        )";
                    command.ExecuteNonQuery();
                }
            }
        }

        private void SeedData()
        {
            CreateTable();
            using (var connection = fixture.CreateConnection())
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = @"
                        INSERT INTO SetOpItem (Id, Name, Category) VALUES
                            (1, 'Apple', 'Fruit'),
                            (2, 'Banana', 'Fruit'),
                            (3, 'Carrot', 'Vegetable'),
                            (4, 'Daikon', 'Vegetable'),
                            (5, 'Eggplant', 'Vegetable')";
                    command.ExecuteNonQuery();
                }
            }
        }

        // =====================================================
        // 4-1: Union() → UNION
        // =====================================================

        [Fact]
        public void UnionTest()
        {
            SeedData();

            using (var connection = fixture.OpenNewConnection())
            {
                // Fruit items UNION Vegetable items → all 5, but duplicates removed
                var fruits = connection.Query<SetOpItem>()
                    .Where(x => x.Category == "Fruit");
                var vegetables = connection.Query<SetOpItem>()
                    .Where(x => x.Category == "Vegetable");

                var result = fruits.Union(vegetables)
                    .ToList();

                Assert.Equal(5, result.Count);
            }
        }

        [Fact]
        public void UnionRemovesDuplicatesTest()
        {
            SeedData();

            using (var connection = fixture.OpenNewConnection())
            {
                // Union of same query should remove duplicates
                var all1 = connection.Query<SetOpItem>();
                var all2 = connection.Query<SetOpItem>();

                var result = all1.Union(all2).ToList();

                // UNION removes duplicates, so we should get 5 rows
                Assert.Equal(5, result.Count);
            }
        }

        // =====================================================
        // 4-2: Concat() → UNION ALL
        // =====================================================

        [Fact]
        public void ConcatTest()
        {
            SeedData();

            using (var connection = fixture.OpenNewConnection())
            {
                var fruits = connection.Query<SetOpItem>()
                    .Where(x => x.Category == "Fruit");
                var vegetables = connection.Query<SetOpItem>()
                    .Where(x => x.Category == "Vegetable");

                var result = fruits.Concat(vegetables)
                    .ToList();

                Assert.Equal(5, result.Count);
            }
        }

        [Fact]
        public void ConcatKeepsDuplicatesTest()
        {
            SeedData();

            using (var connection = fixture.OpenNewConnection())
            {
                // Concat of same query should keep duplicates
                var all1 = connection.Query<SetOpItem>();
                var all2 = connection.Query<SetOpItem>();

                var result = all1.Concat(all2).ToList();

                // UNION ALL keeps duplicates, so we should get 10 rows
                Assert.Equal(10, result.Count);
            }
        }

        // =====================================================
        // 4-3: Intersect() → INTERSECT
        // =====================================================

        [Fact]
        public void IntersectTest()
        {
            SeedData();

            using (var connection = fixture.OpenNewConnection())
            {
                // Items that are Fruit AND have Id <= 3
                var fruits = connection.Query<SetOpItem>()
                    .Where(x => x.Category == "Fruit");
                var upToThree = connection.Query<SetOpItem>()
                    .Where(x => x.Id <= 3);

                var result = fruits.Intersect(upToThree)
                    .ToList();

                // Fruit items are Id=1,2; Items with Id<=3 are Id=1,2,3
                // Intersection: Id=1,2
                Assert.Equal(2, result.Count);
            }
        }

        [Fact]
        public void IntersectNoOverlapTest()
        {
            SeedData();

            using (var connection = fixture.OpenNewConnection())
            {
                var fruits = connection.Query<SetOpItem>()
                    .Where(x => x.Category == "Fruit");
                var highId = connection.Query<SetOpItem>()
                    .Where(x => x.Id > 3);

                var result = fruits.Intersect(highId).ToList();

                // No overlap: Fruit has Id=1,2; Id>3 has Id=4,5
                Assert.Empty(result);
            }
        }

        // =====================================================
        // 4-4: Except() → EXCEPT
        // =====================================================

        [Fact]
        public void ExceptTest()
        {
            SeedData();

            using (var connection = fixture.OpenNewConnection())
            {
                // All items except Fruit items
                var all = connection.Query<SetOpItem>();
                var fruits = connection.Query<SetOpItem>()
                    .Where(x => x.Category == "Fruit");

                var result = all.Except(fruits).ToList();

                // All 5 items minus 2 Fruit items = 3 Vegetable items
                Assert.Equal(3, result.Count);
            }
        }

        [Fact]
        public void ExceptSameQueryTest()
        {
            SeedData();

            using (var connection = fixture.OpenNewConnection())
            {
                // All items except all items = empty
                var all1 = connection.Query<SetOpItem>();
                var all2 = connection.Query<SetOpItem>();

                var result = all1.Except(all2).ToList();

                Assert.Empty(result);
            }
        }

        // =====================================================
        // Additional tests: with Where on both sides
        // =====================================================

        [Fact]
        public void UnionWithFilterOnBothSidesTest()
        {
            SeedData();

            using (var connection = fixture.OpenNewConnection())
            {
                var apples = connection.Query<SetOpItem>()
                    .Where(x => x.Name == "Apple");
                var carrots = connection.Query<SetOpItem>()
                    .Where(x => x.Name == "Carrot");

                var result = apples.Union(carrots).ToList();

                Assert.Equal(2, result.Count);
            }
        }

        [Fact]
        public void ConcatWithFilterOnBothSidesTest()
        {
            SeedData();

            using (var connection = fixture.OpenNewConnection())
            {
                // Same item on both sides → duplicated in UNION ALL
                var apples1 = connection.Query<SetOpItem>()
                    .Where(x => x.Name == "Apple");
                var apples2 = connection.Query<SetOpItem>()
                    .Where(x => x.Name == "Apple");

                var result = apples1.Concat(apples2).ToList();

                Assert.Equal(2, result.Count);
                Assert.Equal("Apple", result[0].Name);
                Assert.Equal("Apple", result[1].Name);
            }
        }
    }
}
