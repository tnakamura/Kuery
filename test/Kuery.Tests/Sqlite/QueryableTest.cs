using System;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace Kuery.Tests.Sqlite
{
    public class QueryableTest : IClassFixture<SqliteFixture>, IDisposable
    {
        readonly SqliteFixture fixture;

        public QueryableTest(SqliteFixture fixture)
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

        [Fact]
        public void ToListTest()
        {
            SeedThreeCustomers();

            using (var connection = fixture.OpenNewConnection())
            {
                var result = connection.Query<Customer>().ToList();

                Assert.Equal(3, result.Count);
            }
        }

        [Fact]
        public void WhereTest()
        {
            SeedThreeCustomers();

            using (var connection = fixture.OpenNewConnection())
            {
                var result = connection.Query<Customer>()
                    .Where(x => x.Code == "2")
                    .ToList();

                Assert.Single(result);
                Assert.Equal(2, result[0].Id);
                Assert.Equal("bbb", result[0].Name);
            }
        }

        [Fact]
        public void WhereAndWhereTest()
        {
            SeedThreeCustomers();

            using (var connection = fixture.OpenNewConnection())
            {
                var result = connection.Query<Customer>()
                    .Where(x => x.Id > 1)
                    .Where(x => x.Code == "3")
                    .ToList();

                Assert.Single(result);
                Assert.Equal(3, result[0].Id);
                Assert.Equal("ccc", result[0].Name);
            }
        }

        [Fact]
        public void OrderByTest()
        {
            SeedThreeCustomers();

            using (var connection = fixture.OpenNewConnection())
            {
                var result = connection.Query<Customer>()
                    .OrderBy(x => x.Code)
                    .ToList();

                Assert.Equal(3, result.Count);
                Assert.Equal("1", result[0].Code);
                Assert.Equal("2", result[1].Code);
                Assert.Equal("3", result[2].Code);
            }
        }

        [Fact]
        public void OrderByDescendingTest()
        {
            SeedThreeCustomers();

            using (var connection = fixture.OpenNewConnection())
            {
                var result = connection.Query<Customer>()
                    .OrderByDescending(x => x.Code)
                    .ToList();

                Assert.Equal(3, result.Count);
                Assert.Equal("3", result[0].Code);
                Assert.Equal("2", result[1].Code);
                Assert.Equal("1", result[2].Code);
            }
        }

        [Fact]
        public void ThenByTest()
        {
            using (var connection = fixture.OpenNewConnection())
            {
                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        @"INSERT INTO customers (id, code, name)
                          VALUES (1, '111', 'ccc')
                               , (2, '111', 'aaa')
                               , (3, '111', 'bbb')";
                    command.ExecuteNonQuery();
                }

                var result = connection.Query<Customer>()
                    .OrderBy(x => x.Code)
                    .ThenBy(x => x.Name)
                    .ToList();

                Assert.Equal("aaa", result[0].Name);
                Assert.Equal("bbb", result[1].Name);
                Assert.Equal("ccc", result[2].Name);
            }
        }

        [Fact]
        public void ThenByDescendingTest()
        {
            using (var connection = fixture.OpenNewConnection())
            {
                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        @"INSERT INTO customers (id, code, name)
                          VALUES (1, '111', 'ccc')
                               , (2, '111', 'aaa')
                               , (3, '111', 'bbb')";
                    command.ExecuteNonQuery();
                }

                var result = connection.Query<Customer>()
                    .OrderBy(x => x.Code)
                    .ThenByDescending(x => x.Name)
                    .ToList();

                Assert.Equal("ccc", result[0].Name);
                Assert.Equal("bbb", result[1].Name);
                Assert.Equal("aaa", result[2].Name);
            }
        }

        [Fact]
        public void TakeTest()
        {
            SeedThreeCustomers();

            using (var connection = fixture.OpenNewConnection())
            {
                var result = connection.Query<Customer>()
                    .OrderBy(x => x.Id)
                    .Take(2)
                    .ToList();

                Assert.Equal(2, result.Count);
                Assert.Equal(1, result[0].Id);
                Assert.Equal(2, result[1].Id);
            }
        }

        [Fact]
        public void SkipTest()
        {
            SeedThreeCustomers();

            using (var connection = fixture.OpenNewConnection())
            {
                var result = connection.Query<Customer>()
                    .OrderBy(x => x.Id)
                    .Skip(1)
                    .ToList();

                Assert.Equal(2, result.Count);
                Assert.Equal(2, result[0].Id);
                Assert.Equal(3, result[1].Id);
            }
        }

        [Fact]
        public void SkipTakeTest()
        {
            SeedThreeCustomers();

            using (var connection = fixture.OpenNewConnection())
            {
                var result = connection.Query<Customer>()
                    .OrderBy(x => x.Id)
                    .Skip(1)
                    .Take(1)
                    .ToList();

                Assert.Single(result);
                Assert.Equal(2, result[0].Id);
            }
        }

        [Fact]
        public void CountTest()
        {
            SeedThreeCustomers();

            using (var connection = fixture.OpenNewConnection())
            {
                var count = connection.Query<Customer>().Count();

                Assert.Equal(3, count);
            }
        }

        [Fact]
        public void CountWithPredicateTest()
        {
            SeedThreeCustomers();

            using (var connection = fixture.OpenNewConnection())
            {
                var count = connection.Query<Customer>()
                    .Count(x => x.Id > 1);

                Assert.Equal(2, count);
            }
        }

        [Fact]
        public void FirstTest()
        {
            SeedThreeCustomers();

            using (var connection = fixture.OpenNewConnection())
            {
                var result = connection.Query<Customer>()
                    .OrderBy(x => x.Id)
                    .First();

                Assert.Equal(1, result.Id);
                Assert.Equal("aaa", result.Name);
            }
        }

        [Fact]
        public void FirstWithPredicateTest()
        {
            SeedThreeCustomers();

            using (var connection = fixture.OpenNewConnection())
            {
                var result = connection.Query<Customer>()
                    .First(x => x.Code == "2");

                Assert.Equal(2, result.Id);
                Assert.Equal("bbb", result.Name);
            }
        }

        [Fact]
        public void FirstThrowsWhenEmptyTest()
        {
            using (var connection = fixture.OpenNewConnection())
            {
                Assert.Throws<InvalidOperationException>(() =>
                    connection.Query<Customer>().First());
            }
        }

        [Fact]
        public void FirstOrDefaultTest()
        {
            SeedThreeCustomers();

            using (var connection = fixture.OpenNewConnection())
            {
                var result = connection.Query<Customer>()
                    .OrderBy(x => x.Id)
                    .FirstOrDefault();

                Assert.NotNull(result);
                Assert.Equal(1, result.Id);
            }
        }

        [Fact]
        public void FirstOrDefaultWithPredicateTest()
        {
            SeedThreeCustomers();

            using (var connection = fixture.OpenNewConnection())
            {
                var result = connection.Query<Customer>()
                    .FirstOrDefault(x => x.Code == "2");

                Assert.NotNull(result);
                Assert.Equal(2, result.Id);
                Assert.Equal("bbb", result.Name);
            }
        }

        [Fact]
        public void FirstOrDefaultReturnsNullWhenEmptyTest()
        {
            using (var connection = fixture.OpenNewConnection())
            {
                var result = connection.Query<Customer>()
                    .FirstOrDefault();

                Assert.Null(result);
            }
        }

        [Fact]
        public void WhereWithOrderByAndTakeTest()
        {
            SeedThreeCustomers();

            using (var connection = fixture.OpenNewConnection())
            {
                var result = connection.Query<Customer>()
                    .Where(x => x.Id >= 2)
                    .OrderByDescending(x => x.Id)
                    .Take(1)
                    .ToList();

                Assert.Single(result);
                Assert.Equal(3, result[0].Id);
            }
        }

        [Fact]
        public void SelectAnonymousTypeTest()
        {
            SeedThreeCustomers();

            using (var connection = fixture.OpenNewConnection())
            {
                var result = connection.Query<Customer>()
                    .OrderBy(x => x.Id)
                    .Select(x => new { x.Id, x.Name })
                    .ToList();

                Assert.Equal(3, result.Count);
                Assert.Equal(1, result[0].Id);
                Assert.Equal("aaa", result[0].Name);
                Assert.Equal(2, result[1].Id);
                Assert.Equal("bbb", result[1].Name);
                Assert.Equal(3, result[2].Id);
                Assert.Equal("ccc", result[2].Name);
            }
        }

        [Fact]
        public void SelectWithWhereTest()
        {
            SeedThreeCustomers();

            using (var connection = fixture.OpenNewConnection())
            {
                var result = connection.Query<Customer>()
                    .Where(x => x.Id > 1)
                    .Select(x => new { x.Code })
                    .ToList();

                Assert.Equal(2, result.Count);
                Assert.Equal("2", result[0].Code);
                Assert.Equal("3", result[1].Code);
            }
        }

        [Fact]
        public void SelectSinglePropertyTest()
        {
            SeedThreeCustomers();

            using (var connection = fixture.OpenNewConnection())
            {
                var result = connection.Query<Customer>()
                    .OrderBy(x => x.Id)
                    .Select(x => x.Name)
                    .ToList();

                Assert.Equal(3, result.Count);
                Assert.Equal("aaa", result[0]);
                Assert.Equal("bbb", result[1]);
                Assert.Equal("ccc", result[2]);
            }
        }

        [Fact]
        public void SelectWithTakeTest()
        {
            SeedThreeCustomers();

            using (var connection = fixture.OpenNewConnection())
            {
                var result = connection.Query<Customer>()
                    .OrderBy(x => x.Id)
                    .Select(x => new { x.Id, x.Name })
                    .Take(2)
                    .ToList();

                Assert.Equal(2, result.Count);
                Assert.Equal(1, result[0].Id);
                Assert.Equal(2, result[1].Id);
            }
        }

        [Fact]
        public void SelectFirstTest()
        {
            SeedThreeCustomers();

            using (var connection = fixture.OpenNewConnection())
            {
                var result = connection.Query<Customer>()
                    .OrderBy(x => x.Id)
                    .Select(x => new { x.Id, x.Name })
                    .First();

                Assert.Equal(1, result.Id);
                Assert.Equal("aaa", result.Name);
            }
        }

        [Fact]
        public void SelectFirstOrDefaultEmptyTest()
        {
            using (var connection = fixture.OpenNewConnection())
            {
                var result = connection.Query<Customer>()
                    .Select(x => new { x.Id, x.Name })
                    .FirstOrDefault();

                Assert.Null(result);
            }
        }
    }
}
