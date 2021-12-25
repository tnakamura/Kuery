using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Transactions;
using Xunit;

namespace Kuery.Tests
{
    public class SqlHelperTest : IClassFixture<SqliteFixture>, IDisposable
    {
        readonly TransactionScope transactionScope;

        readonly SqliteFixture fixture;

        public SqlHelperTest(SqliteFixture fixture)
        {
            this.fixture = fixture;
            transactionScope = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled);
        }

        public void Dispose()
        {
            transactionScope?.Dispose();
        }

        [Fact]
        public void ToListTest()
        {
            using (var connection = fixture.CreateConnection())
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        @"INSERT customers (id, code, name)
                          VALUES (2, N'2', N'Google')";
                    command.ExecuteNonQuery();
                }

                var result = connection.Table<Customer>().ToList();
                Assert.Single(result);
                Assert.Equal(2, result[0].Id);
                Assert.Equal("2", result[0].Code);
                Assert.Equal("Google", result[0].Name);
            }
        }

        [Fact]
        public async Task ToListAsyncTest()
        {
            using (var connection = fixture.CreateConnection())
            {
                await connection.OpenAsync();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        @"INSERT customers (id, code, name)
                          VALUES (3, N'3', N'Apple')";
                    await command.ExecuteNonQueryAsync();
                }

                var result = await connection.Table<Customer>().ToListAsync();
                Assert.Single(result);
                Assert.Equal(3, result[0].Id);
                Assert.Equal("3", result[0].Code);
                Assert.Equal("Apple", result[0].Name);
            }
        }

        [Fact]
        public async Task CountAsyncTest()
        {
            using (var connection = fixture.CreateConnection())
            {
                await connection.OpenAsync();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        @"INSERT customers (id, code, name)
                          VALUES (4, N'4', N'Facebook')";
                    await command.ExecuteNonQueryAsync();
                }

                var result = await connection.Table<Customer>().CountAsync();
                Assert.Equal(1, result);
            }
        }

        [Fact]
        public void CountTest()
        {
            using (var connection = fixture.CreateConnection())
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        @"INSERT customers (id, code, name)
                          VALUES (5, N'5', N'Twitter')";
                    command.ExecuteNonQuery();
                }

                var result = connection.Table<Customer>().Count();
                Assert.Equal(1, result);
            }
        }

        [Fact]
        public void DeleteWithPredicateTest()
        {
            using (var connection = fixture.CreateConnection())
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        @"INSERT customers (id, code, name)
                          VALUES (1, N'1', N'Microsoft')";
                    command.ExecuteNonQuery();
                }

                var result = connection.Table<Customer>().Delete(x => x.Code == "1");
                Assert.Equal(1, result);
            }
        }

        [Fact]
        public void DeleteWithWhereTest()
        {
            using (var connection = fixture.CreateConnection())
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        @"INSERT customers (id, code, name)
                          VALUES (6, N'6', N'Amazon')";
                    command.ExecuteNonQuery();
                }

                var result = connection.Table<Customer>()
                    .Where(x => x.Code == "6")
                    .Delete();
                Assert.Equal(1, result);
            }
        }

        [Fact]
        public async Task InsertAsyncTest()
        {
            using (var connection = fixture.CreateConnection())
            {
                connection.Open();

                var expected = new Customer
                {
                    Id = 7,
                    Code = "7",
                    Name = "Salesforce",
                };
                var inserted = await connection.InsertAsync(expected);
                Assert.Equal(1, inserted);

                var actual = await connection.Table<Customer>()
                    .ToListAsync();
                Assert.Single(actual);
                Assert.Equal(expected.Name, actual[0].Name);
                Assert.Equal(expected.Id, actual[0].Id);
            }
        }

        [Fact]
        public async Task UpdateAsyncTest()
        {
            using (var connection = fixture.CreateConnection())
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        @"INSERT customers (id, code, name)
                          VALUES (8, N'8', N'Spotify')";
                    await command.ExecuteNonQueryAsync();
                }

                var expected = new Customer
                {
                    Id = 8,
                    Code = "8",
                    Name = "Shopify",
                };
                var updated = await connection.UpdateAsync(expected);
                Assert.Equal(1, updated);

                var actual = await connection.Table<Customer>()
                    .ToListAsync();
                Assert.Single(actual);
                Assert.Equal(expected.Name, actual[0].Name);
                Assert.Equal(expected.Id, actual[0].Id);
            }
        }

        [Fact]
        public void WhereTest()
        {
            using (var connection = fixture.CreateConnection())
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        @"INSERT customers (id, code, name)
                          VALUES (9, N'9', N'GitHub')";
                    command.ExecuteNonQuery();
                }

                var actual = connection.Table<Customer>()
                    .Where(x => x.Code == "9")
                    .ToList();
                Assert.Single(actual);
                Assert.Equal(9, actual[0].Id);
                Assert.Equal("GitHub", actual[0].Name);
            }
        }

        [Fact]
        public void WhereAndWhereTest()
        {
            using (var connection = fixture.CreateConnection())
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        @"INSERT customers (id, code, name)
                          VALUES (10, N'10', N'Netflix')";
                    command.ExecuteNonQuery();
                }

                var actual = connection.Table<Customer>()
                    .Where(x => x.Code == "10")
                    .Where(x => x.Name == "Netflix")
                    .ToList();
                Assert.Single(actual);
                Assert.Equal(10, actual[0].Id);
                Assert.Equal("Netflix", actual[0].Name);
            }
        }

        [Fact]
        public void InsertTest()
        {
            using (var connection = fixture.CreateConnection())
            {
                connection.Open();

                var expected = new Customer
                {
                    Id = 11,
                    Code = "11",
                    Name = "Slack",
                };
                var inserted = connection.Insert(expected);
                Assert.Equal(1, inserted);

                var actual = connection.Table<Customer>()
                    .ToList();
                Assert.Single(actual);
                Assert.Equal(expected.Name, actual[0].Name);
                Assert.Equal(expected.Id, actual[0].Id);
            }
        }

        [Fact]
        public void UpdateTest()
        {
            using (var connection = fixture.CreateConnection())
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        @"INSERT customers (id, code, name)
                          VALUES (12, N'12', N'SONY')";
                    command.ExecuteNonQuery();
                }

                var expected = new Customer
                {
                    Id = 12,
                    Code = "12",
                    Name = "SCE",
                };
                var updated = connection.Update(expected);
                Assert.Equal(1, updated);

                var actual = connection.Table<Customer>()
                    .ToList();
                Assert.Single(actual);
                Assert.Equal(expected.Name, actual[0].Name);
                Assert.Equal(expected.Id, actual[0].Id);
            }
        }

        [Fact]
        public void DeleteItemTest()
        {
            using (var connection = fixture.CreateConnection())
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        @"INSERT customers (id, code, name)
                          VALUES (13, N'13', N'NINTENDO')";
                    command.ExecuteNonQuery();
                }

                var customer = new Customer
                {
                    Id = 13,
                    Code = "13",
                    Name = "NINTENDO",
                };
                var result = connection.Delete(customer);
                Assert.Equal(1, result);
            }
        }

        [Fact]
        public async Task DeleteItemAsyncTest()
        {
            using (var connection = fixture.CreateConnection())
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        @"INSERT customers (id, code, name)
                          VALUES (14, N'14', N'SEGA')";
                    await command.ExecuteNonQueryAsync();
                }

                var customer = new Customer
                {
                    Id = 14,
                    Code = "14",
                    Name = "SEGA",
                };
                var result = await connection.DeleteAsync(customer);
                Assert.Equal(1, result);
            }
        }

        [Fact]
        public void ToArrayTest()
        {
            using (var connection = fixture.CreateConnection())
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        @"INSERT customers (id, code, name)
                          VALUES (15, N'15', N'Soundcloud')";
                    command.ExecuteNonQuery();
                }

                var result = connection.Table<Customer>().ToArray();
                Assert.Single(result);
                Assert.Equal(15, result[0].Id);
                Assert.Equal("15", result[0].Code);
                Assert.Equal("Soundcloud", result[0].Name);
            }
        }

        [Fact]
        public async Task ToArrayAsyncTest()
        {
            using (var connection = fixture.CreateConnection())
            {
                await connection.OpenAsync();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        @"INSERT customers (id, code, name)
                          VALUES (16, N'16', N'Cloudflare')";
                    await command.ExecuteNonQueryAsync();
                }

                var result = await connection.Table<Customer>().ToArrayAsync();
                Assert.Single(result);
                Assert.Equal(16, result[0].Id);
                Assert.Equal("16", result[0].Code);
                Assert.Equal("Cloudflare", result[0].Name);
            }
        }

        [Fact]
        public void CountWithExpressionTest()
        {
            using (var connection = fixture.CreateConnection())
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        @"INSERT customers (id, code, name)
                          VALUES (17, N'17', N'Alphabet')";
                    command.ExecuteNonQuery();
                }

                var result = connection.Table<Customer>().Count(x => x.Code == "17");
                Assert.Equal(1, result);
            }
        }

        [Fact]
        public async Task CountAsyncWithExpressionTest()
        {
            using (var connection = fixture.CreateConnection())
            {
                await connection.OpenAsync();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        @"INSERT customers (id, code, name)
                          VALUES (18, N'18', N'NIKE')";
                    await command.ExecuteNonQueryAsync();
                }

                var result = await connection.Table<Customer>().CountAsync(x => x.Code == "18");
                Assert.Equal(1, result);
            }
        }

        [Fact]
        public async Task DeleteAsyncWithPredicateTest()
        {
            using (var connection = fixture.CreateConnection())
            {
                await connection.OpenAsync();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        @"INSERT customers (id, code, name)
                          VALUES (19, N'19', N'SpaceX')";
                    await command.ExecuteNonQueryAsync();
                }

                var result = await connection.Table<Customer>().DeleteAsync(x => x.Code == "19");
                Assert.Equal(1, result);
            }
        }

        [Fact]
        public async Task DeleteAsyncWithWhereTest()
        {
            using (var connection = fixture.CreateConnection())
            {
                await connection.OpenAsync();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        @"INSERT customers (id, code, name)
                          VALUES (20, N'20', N'NASA')";
                    await command.ExecuteNonQueryAsync();
                }

                var result = await connection.Table<Customer>()
                    .Where(x => x.Code == "20")
                    .DeleteAsync();
                Assert.Equal(1, result);
            }
        }

        [Fact]
        public void OrderByTest()
        {
            using (var connection = fixture.CreateConnection())
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        @"INSERT customers (id, code, name)
                          VALUES (21, N'22', N'freee')
                               , (22, N'21', N'Moneyforward')";
                    command.ExecuteNonQuery();
                }

                var result = connection.Table<Customer>()
                    .OrderBy(x => x.Code)
                    .ToList();
                Assert.Equal(2, result.Count);
                Assert.Equal("21", result[0].Code);
                Assert.Equal("22", result[1].Code);
            }
        }

        [Fact]
        public void OrderByDescendingTest()
        {
            using (var connection = fixture.CreateConnection())
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        @"INSERT customers (id, code, name)
                          VALUES (23, N'23', N'OBC')
                               , (24, N'24', N'PCA')
                               , (25, N'25', N'弥生')";
                    command.ExecuteNonQuery();
                }

                var result = connection.Table<Customer>()
                    .OrderByDescending(x => x.Code)
                    .ToList();
                Assert.Equal(3, result.Count);
                Assert.Equal("25", result[0].Code);
                Assert.Equal("24", result[1].Code);
                Assert.Equal("23", result[2].Code);
            }
        }

        [Fact]
        public async Task OrderByAsyncTest()
        {
            using (var connection = fixture.CreateConnection())
            {
                await connection.OpenAsync();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        @"INSERT customers (id, code, name)
                          VALUES (21, N'22', N'freee')
                               , (22, N'21', N'Moneyforward')";
                    await command.ExecuteNonQueryAsync();
                }

                var result = await connection.Table<Customer>()
                    .OrderBy(x => x.Code)
                    .ToListAsync();
                Assert.Equal(2, result.Count);
                Assert.Equal("21", result[0].Code);
                Assert.Equal("22", result[1].Code);
            }
        }

        [Fact]
        public async Task OrderByDescendingAsyncTest()
        {
            using (var connection = fixture.CreateConnection())
            {
                await connection.OpenAsync();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        @"INSERT customers (id, code, name)
                          VALUES (23, N'23', N'OBC')
                               , (24, N'24', N'PCA')
                               , (25, N'25', N'弥生')";
                    await command.ExecuteNonQueryAsync();
                }

                var result = await connection.Table<Customer>()
                    .OrderByDescending(x => x.Code)
                    .ToListAsync();
                Assert.Equal(3, result.Count);
                Assert.Equal("25", result[0].Code);
                Assert.Equal("24", result[1].Code);
                Assert.Equal("23", result[2].Code);
            }
        }

        [Fact]
        public void TakeTest()
        {
            using (var connection = fixture.CreateConnection())
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        @"INSERT customers (id, code, name)
                          VALUES (23, N'23', N'OBC')
                               , (24, N'24', N'PCA')
                               , (25, N'25', N'弥生')";
                    command.ExecuteNonQuery();
                }

                var result = connection.Table<Customer>()
                    .Take(1)
                    .ToList();
                Assert.Single(result);
                Assert.Equal("23", result[0].Code);
            }
        }

        [Fact]
        public async Task TakeAsyncTest()
        {
            using (var connection = fixture.CreateConnection())
            {
                await connection.OpenAsync();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        @"INSERT customers (id, code, name)
                          VALUES (23, N'23', N'OBC')
                               , (24, N'24', N'PCA')
                               , (25, N'25', N'弥生')";
                    await command.ExecuteNonQueryAsync();
                }

                var result = await connection.Table<Customer>()
                    .Take(1)
                    .ToListAsync();
                Assert.Single(result);
                Assert.Equal("23", result[0].Code);
            }
        }

        [Fact]
        public void SkipTest()
        {
            using (var connection = fixture.CreateConnection())
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        @"INSERT customers (id, code, name)
                          VALUES (23, N'23', N'OBC')
                               , (24, N'24', N'PCA')
                               , (25, N'25', N'弥生')";
                    command.ExecuteNonQuery();
                }

                var result = connection.Table<Customer>()
                    .Skip(1)
                    .ToList();
                Assert.Equal(2, result.Count);
                Assert.Equal("24", result[0].Code);
                Assert.Equal("25", result[1].Code);
            }
        }

        [Fact]
        public async Task SkipAsyncTest()
        {
            using (var connection = fixture.CreateConnection())
            {
                await connection.OpenAsync();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        @"INSERT customers (id, code, name)
                          VALUES (23, N'23', N'OBC')
                               , (24, N'24', N'PCA')
                               , (25, N'25', N'弥生')";
                    await command.ExecuteNonQueryAsync();
                }

                var result = await connection.Table<Customer>()
                    .Skip(1)
                    .ToListAsync();
                Assert.Equal(2, result.Count);
                Assert.Equal("24", result[0].Code);
                Assert.Equal("25", result[1].Code);
            }
        }

        [Fact]
        public void TakeSkipTest()
        {
            using (var connection = fixture.CreateConnection())
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        @"INSERT customers (id, code, name)
                          VALUES (23, N'23', N'OBC')
                               , (24, N'24', N'PCA')
                               , (25, N'25', N'弥生')";
                    command.ExecuteNonQuery();
                }

                var result = connection.Table<Customer>()
                    .Skip(1)
                    .Take(1)
                    .ToList();
                Assert.Single(result);
                Assert.Equal("24", result[0].Code);
            }
        }

        [Fact]
        public async Task TakeSkipAsyncTest()
        {
            using (var connection = fixture.CreateConnection())
            {
                await connection.OpenAsync();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        @"INSERT customers (id, code, name)
                          VALUES (23, N'23', N'OBC')
                               , (24, N'24', N'PCA')
                               , (25, N'25', N'弥生')";
                    await command.ExecuteNonQueryAsync();
                }

                var result = await connection.Table<Customer>()
                    .Skip(1)
                    .Take(1)
                    .ToListAsync();
                Assert.Single(result);
                Assert.Equal("24", result[0].Code);
            }
        }

        [Fact]
        public async Task FirstAsyncTest()
        {
            using (var connection = fixture.CreateConnection())
            {
                await connection.OpenAsync();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        @"INSERT customers (id, code, name)
                          VALUES (23, N'23', N'OBC')
                               , (24, N'24', N'PCA')
                               , (25, N'25', N'弥生')";
                    await command.ExecuteNonQueryAsync();
                }

                var result = await connection.Table<Customer>()
                    .FirstAsync();
                Assert.Equal("23", result.Code);
            }
        }

        [Fact]
        public async Task FirstAsyncWithPredicateTest()
        {
            using (var connection = fixture.CreateConnection())
            {
                await connection.OpenAsync();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        @"INSERT customers (id, code, name)
                          VALUES (23, N'23', N'OBC')
                               , (24, N'24', N'PCA')
                               , (25, N'25', N'弥生')";
                    await command.ExecuteNonQueryAsync();
                }

                var result = await connection.Table<Customer>()
                    .FirstAsync(x => x.Code == "24");
                Assert.Equal("24", result.Code);
            }
        }

        [Fact]
        public async Task FirstOrDefaultAsyncTest()
        {
            using (var connection = fixture.CreateConnection())
            {
                await connection.OpenAsync();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        @"INSERT customers (id, code, name)
                          VALUES (23, N'23', N'OBC')
                               , (24, N'24', N'PCA')
                               , (25, N'25', N'弥生')";
                    await command.ExecuteNonQueryAsync();
                }

                var result = await connection.Table<Customer>()
                    .FirstOrDefaultAsync();
                Assert.Equal("23", result.Code);
            }
        }

        [Fact]
        public async Task FirstOrDefaultAsyncWithPredicateTest()
        {
            using (var connection = fixture.CreateConnection())
            {
                await connection.OpenAsync();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        @"INSERT customers (id, code, name)
                          VALUES (23, N'23', N'OBC')
                               , (24, N'24', N'PCA')
                               , (25, N'25', N'弥生')";
                    await command.ExecuteNonQueryAsync();
                }

                var result = await connection.Table<Customer>()
                    .FirstOrDefaultAsync(x => x.Code == "24");
                Assert.Equal("24", result.Code);
            }
        }

        [Fact]
        public void FirstTest()
        {
            using (var connection = fixture.CreateConnection())
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        @"INSERT customers (id, code, name)
                          VALUES (23, N'23', N'OBC')
                               , (24, N'24', N'PCA')
                               , (25, N'25', N'弥生')";
                    command.ExecuteNonQuery();
                }

                var result = connection.Table<Customer>()
                    .First();
                Assert.Equal("23", result.Code);
            }
        }

        [Fact]
        public void FirstWithPredicateTest()
        {
            using (var connection = fixture.CreateConnection())
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        @"INSERT customers (id, code, name)
                          VALUES (23, N'23', N'OBC')
                               , (24, N'24', N'PCA')
                               , (25, N'25', N'弥生')";
                    command.ExecuteNonQuery();
                }

                var result = connection.Table<Customer>()
                    .First(x => x.Code == "24");
                Assert.Equal("24", result.Code);
            }
        }

        [Fact]
        public void FirstOrDefaultTest()
        {
            using (var connection = fixture.CreateConnection())
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        @"INSERT customers (id, code, name)
                          VALUES (23, N'23', N'OBC')
                               , (24, N'24', N'PCA')
                               , (25, N'25', N'弥生')";
                    command.ExecuteNonQuery();
                }

                var result = connection.Table<Customer>()
                    .FirstOrDefault();
                Assert.Equal("23", result.Code);
            }
        }

        [Fact]
        public void FirstOrDefaultWithPredicateTest()
        {
            using (var connection = fixture.CreateConnection())
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        @"INSERT customers (id, code, name)
                          VALUES (23, N'23', N'OBC')
                               , (24, N'24', N'PCA')
                               , (25, N'25', N'弥生')";
                    command.ExecuteNonQuery();
                }

                var result = connection.Table<Customer>()
                    .FirstOrDefault(x => x.Code == "24");
                Assert.Equal("24", result.Code);
            }
        }

        [Fact]
        public void ElementAtTest()
        {
            using (var connection = fixture.CreateConnection())
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        @"INSERT customers (id, code, name)
                          VALUES (23, N'23', N'OBC')
                               , (24, N'24', N'PCA')
                               , (25, N'25', N'弥生')";
                    command.ExecuteNonQuery();
                }

                var result = connection.Table<Customer>()
                    .ElementAt(0);
                Assert.Equal("23", result.Code);
            }
        }

        [Fact]
        public async Task ElementAtAsyncTest()
        {
            using (var connection = fixture.CreateConnection())
            {
                await connection.OpenAsync();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        @"INSERT customers (id, code, name)
                          VALUES (23, N'23', N'OBC')
                               , (24, N'24', N'PCA')
                               , (25, N'25', N'弥生')";
                    await command.ExecuteNonQueryAsync();
                }

                var result = await connection.Table<Customer>()
                    .ElementAtAsync(1);
                Assert.Equal("24", result.Code);
            }
        }

        [Fact]
        public void GetEnumeratorTest()
        {
            using (var connection = fixture.CreateConnection())
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        @"INSERT customers (id, code, name)
                          VALUES (23, N'23', N'OBC')
                               , (24, N'24', N'PCA')
                               , (25, N'25', N'弥生')";
                    command.ExecuteNonQuery();
                }

                var result = connection.Table<Customer>()
                    .GetEnumerator();
                Assert.True(result.MoveNext());
                Assert.Equal("23", result.Current.Code);
                Assert.True(result.MoveNext());
                Assert.Equal("24", result.Current.Code);
                Assert.True(result.MoveNext());
                Assert.Equal("25", result.Current.Code);
                Assert.False(result.MoveNext());
            }
        }

        [Fact]
        public void ThenByTest()
        {
            using (var connection = fixture.CreateConnection())
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        @"INSERT customers (id, code, name)
                          VALUES (23, N'111', N'ccc')
                               , (24, N'111', N'aaa')
                               , (25, N'111', N'bbb')";
                    command.ExecuteNonQuery();
                }

                var result = connection.Table<Customer>()
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
            using (var connection = fixture.CreateConnection())
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        @"INSERT customers (id, code, name)
                          VALUES (23, N'111', N'ccc')
                               , (24, N'111', N'aaa')
                               , (25, N'111', N'bbb')";
                    command.ExecuteNonQuery();
                }

                var result = connection.Table<Customer>()
                    .OrderBy(x => x.Code)
                    .ThenByDescending(x => x.Name)
                    .ToList();
                Assert.Equal("ccc", result[0].Name);
                Assert.Equal("bbb", result[1].Name);
                Assert.Equal("aaa", result[2].Name);
            }
        }

        [Fact]
        public async Task ThenByAsyncTest()
        {
            using (var connection = fixture.CreateConnection())
            {
                await connection.OpenAsync();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        @"INSERT customers (id, code, name)
                          VALUES (23, N'111', N'ccc')
                               , (24, N'111', N'aaa')
                               , (25, N'111', N'bbb')";
                    await command.ExecuteNonQueryAsync();
                }

                var result = await connection.Table<Customer>()
                    .OrderBy(x => x.Code)
                    .ThenBy(x => x.Name)
                    .ToListAsync();
                Assert.Equal("aaa", result[0].Name);
                Assert.Equal("bbb", result[1].Name);
                Assert.Equal("ccc", result[2].Name);
            }
        }

        [Fact]
        public async Task ThenByDescendingAsyncTest()
        {
            using (var connection = fixture.CreateConnection())
            {
                await connection.OpenAsync();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        @"INSERT customers (id, code, name)
                          VALUES (23, N'111', N'ccc')
                               , (24, N'111', N'aaa')
                               , (25, N'111', N'bbb')";
                    await command.ExecuteNonQueryAsync();
                }

                var result = await connection.Table<Customer>()
                    .OrderBy(x => x.Code)
                    .ThenByDescending(x => x.Name)
                    .ToListAsync();
                Assert.Equal("ccc", result[0].Name);
                Assert.Equal("bbb", result[1].Name);
                Assert.Equal("aaa", result[2].Name);
            }
        }

        [Fact]
        public void FindTest()
        {
            using (var connection = fixture.CreateConnection())
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        @"INSERT customers (id, code, name)
                          VALUES (1, N'1', N'aaa')
                               , (2, N'2', N'bbb')
                               , (3, N'3', N'ccc')";
                    command.ExecuteNonQuery();
                }

                var result = connection.Find<Customer>(2);
                Assert.Equal(2, result.Id);
                Assert.Equal("2", result.Code);
                Assert.Equal("bbb", result.Name);
            }
        }

        [Fact]
        public async Task FindAsyncTest()
        {
            using (var connection = fixture.CreateConnection())
            {
                await connection.OpenAsync();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        @"INSERT customers (id, code, name)
                          VALUES (1, N'1', N'aaa')
                               , (2, N'2', N'bbb')
                               , (3, N'3', N'ccc')";
                    await command.ExecuteNonQueryAsync();
                }

                var result = await connection.FindAsync<Customer>(2);
                Assert.Equal(2, result.Id);
                Assert.Equal("2", result.Code);
                Assert.Equal("bbb", result.Name);
            }
        }

        [Fact]
        public void GetTest()
        {
            using (var connection = fixture.CreateConnection())
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        @"INSERT customers (id, code, name)
                          VALUES (1, N'1', N'aaa')
                               , (2, N'2', N'bbb')
                               , (3, N'3', N'ccc')";
                    command.ExecuteNonQuery();
                }

                var result = connection.Get<Customer>(2);
                Assert.Equal(2, result.Id);
                Assert.Equal("2", result.Code);
                Assert.Equal("bbb", result.Name);
            }
        }

        [Fact]
        public async Task GetAsyncTest()
        {
            using (var connection = fixture.CreateConnection())
            {
                await connection.OpenAsync();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        @"INSERT customers (id, code, name)
                          VALUES (1, N'1', N'aaa')
                               , (2, N'2', N'bbb')
                               , (3, N'3', N'ccc')";
                    await command.ExecuteNonQueryAsync();
                }

                var result = await connection.GetAsync<Customer>(2);
                Assert.Equal(2, result.Id);
                Assert.Equal("2", result.Code);
                Assert.Equal("bbb", result.Name);
            }
        }

        [Fact]
        public void GetWithPredicateTest()
        {
            using (var connection = fixture.CreateConnection())
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        @"INSERT customers (id, code, name)
                          VALUES (1, N'1', N'aaa')
                               , (2, N'2', N'bbb')
                               , (3, N'3', N'ccc')";
                    command.ExecuteNonQuery();
                }

                var result = connection.Get<Customer>(x => x.Code == "2");
                Assert.Equal(2, result.Id);
                Assert.Equal("2", result.Code);
                Assert.Equal("bbb", result.Name);
            }
        }

        [Fact]
        public async Task GetAsyncWithPredicateTest()
        {
            using (var connection = fixture.CreateConnection())
            {
                await connection.OpenAsync();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        @"INSERT customers (id, code, name)
                          VALUES (1, N'1', N'aaa')
                               , (2, N'2', N'bbb')
                               , (3, N'3', N'ccc')";
                    await command.ExecuteNonQueryAsync();
                }

                var result = await connection.GetAsync<Customer>(x => x.Code == "2");
                Assert.Equal(2, result.Id);
                Assert.Equal("2", result.Code);
                Assert.Equal("bbb", result.Name);
            }
        }

        [Fact]
        public void FindWithPredicateTest()
        {
            using (var connection = fixture.CreateConnection())
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        @"INSERT customers (id, code, name)
                          VALUES (1, N'1', N'aaa')
                               , (2, N'2', N'bbb')
                               , (3, N'3', N'ccc')";
                    command.ExecuteNonQuery();
                }

                var result = connection.Find<Customer>(x => x.Code == "2");
                Assert.Equal(2, result.Id);
                Assert.Equal("2", result.Code);
                Assert.Equal("bbb", result.Name);
            }
        }

        [Fact]
        public async Task FindAsyncWithPredicateTest()
        {
            using (var connection = fixture.CreateConnection())
            {
                await connection.OpenAsync();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        @"INSERT customers (id, code, name)
                          VALUES (1, N'1', N'aaa')
                               , (2, N'2', N'bbb')
                               , (3, N'3', N'ccc')";
                    await command.ExecuteNonQueryAsync();
                }

                var result = await connection.FindAsync<Customer>(x => x.Code == "2");
                Assert.Equal(2, result.Id);
                Assert.Equal("2", result.Code);
                Assert.Equal("bbb", result.Name);
            }
        }

        [Fact]
        public void DeleteWithPrimaryKeyTest()
        {
            using (var connection = fixture.CreateConnection())
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        @"INSERT customers (id, code, name)
                          VALUES (1, N'1', N'aaa')
                               , (2, N'2', N'bbb')
                               , (3, N'3', N'ccc')";
                    command.ExecuteNonQuery();
                }

                var deleteCount = connection.Delete<Customer>(2);
                Assert.Equal(1, deleteCount);
            }
        }

        [Fact]
        public async Task DeleteAsyncWithPrimaryKeyTest()
        {
            using (var connection = fixture.CreateConnection())
            {
                await connection.OpenAsync();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        @"INSERT customers (id, code, name)
                          VALUES (1, N'1', N'aaa')
                               , (2, N'2', N'bbb')
                               , (3, N'3', N'ccc')";
                    await command.ExecuteNonQueryAsync();
                }

                var deleteCount = await connection.DeleteAsync<Customer>(2);
                Assert.Equal(1, deleteCount);
            }
        }

        [Fact]
        public void InsertAllTest()
        {
            using (var connection = fixture.CreateConnection())
            {
                connection.Open();

                var insertedCount = connection.InsertAll(new List<Customer>()
                {
                    new Customer
                    {
                        Id = 1,
                        Code = "111",
                        Name = "foo",
                    },
                    new Customer
                    {
                        Id = 2,
                        Code = "222",
                        Name = "bar",
                    },
                });

                Assert.Equal(2, insertedCount);
            }
        }

        [Fact]
        public async Task InsertAllAsyncTest()
        {
            using (var connection = fixture.CreateConnection())
            {
                await connection.OpenAsync();

                var insertedCount = await connection.InsertAllAsync(new List<Customer>()
                {
                    new Customer
                    {
                        Id = 1,
                        Code = "111",
                        Name = "foo",
                    },
                    new Customer
                    {
                        Id = 2,
                        Code = "222",
                        Name = "bar",
                    },
                });

                Assert.Equal(2, insertedCount);
            }
        }

        [Fact]
        public void InsertAllWithTypeTest()
        {
            using (var connection = fixture.CreateConnection())
            {
                connection.Open();

                var insertedCount = connection.InsertAll(
                    new List<Customer>()
                    {
                        new Customer
                        {
                            Id = 1,
                            Code = "111",
                            Name = "foo",
                        },
                        new Customer
                        {
                            Id = 2,
                            Code = "222",
                            Name = "bar",
                        },
                    },
                    typeof(Customer));

                Assert.Equal(2, insertedCount);
            }
        }

        [Fact]
        public async Task InsertAllAsyncWithTypeTest()
        {
            using (var connection = fixture.CreateConnection())
            {
                await connection.OpenAsync();

                var insertedCount = await connection.InsertAllAsync(
                    new List<Customer>()
                    {
                        new Customer
                        {
                            Id = 1,
                            Code = "111",
                            Name = "foo",
                        },
                        new Customer
                        {
                            Id = 2,
                            Code = "222",
                            Name = "bar",
                        },
                    },
                    typeof(Customer));

                Assert.Equal(2, insertedCount);
            }
        }

        [Fact]
        public async Task UpdateAllAsyncTest()
        {
            using (var connection = fixture.CreateConnection())
            {
                await connection.OpenAsync();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        @"INSERT customers (id, code, name)
                          VALUES (1, N'1', N'aaa')
                               , (2, N'2', N'bbb')
                               , (3, N'3', N'ccc')";
                    await command.ExecuteNonQueryAsync();
                }

                var updatedCount = await connection.UpdateAllAsync(
                    new List<Customer>
                    {
                        new Customer
                        {
                            Id = 1,
                            Code = "1",
                            Name = "foo",
                        },
                        new Customer
                        {
                            Id = 2,
                            Code = "2",
                            Name = "bar",
                        },
                    });

                Assert.Equal(2, updatedCount);
            }
        }

        [Fact]
        public void UpdateAllTest()
        {
            using (var connection = fixture.CreateConnection())
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        @"INSERT customers (id, code, name)
                          VALUES (1, N'1', N'aaa')
                               , (2, N'2', N'bbb')
                               , (3, N'3', N'ccc')";
                    command.ExecuteNonQuery();
                }

                var updatedCount = connection.UpdateAll(
                    new List<Customer>
                    {
                        new Customer
                        {
                            Id = 1,
                            Code = "1",
                            Name = "foo",
                        },
                        new Customer
                        {
                            Id = 2,
                            Code = "2",
                            Name = "bar",
                        },
                    });

                Assert.Equal(2, updatedCount);
            }
        }

        [Fact]
        public void InsertOrReplaceWhenInsertTest()
        {
            using (var connection = fixture.CreateConnection())
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        @"INSERT customers (id, code, name)
                          VALUES (1, N'1', N'aaa')
                               , (2, N'2', N'bbb')
                               , (3, N'3', N'ccc')";
                    command.ExecuteNonQuery();
                }

                var insertedCount = connection.InsertOrReplace(
                    new Customer
                    {
                        Id = 4,
                        Code = "4",
                        Name = "hoge",
                    });

                Assert.Equal(1, insertedCount);
            }
        }

        [Fact]
        public void InsertOrReplaceWhenUpdateTest()
        {
            using (var connection = fixture.CreateConnection())
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        @"INSERT customers (id, code, name)
                          VALUES (1, N'1', N'aaa')
                               , (2, N'2', N'bbb')
                               , (3, N'3', N'ccc')";
                    command.ExecuteNonQuery();
                }

                var updatedCount = connection.InsertOrReplace(
                    new Customer
                    {
                        Id = 2,
                        Code = "2",
                        Name = "foo",
                    });

                Assert.Equal(1, updatedCount);
            }
        }

        [Fact]
        public async Task InsertOrReplaceAsyncWhenInsertTest()
        {
            using (var connection = fixture.CreateConnection())
            {
                await connection.OpenAsync();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        @"INSERT customers (id, code, name)
                          VALUES (1, N'1', N'aaa')
                               , (2, N'2', N'bbb')
                               , (3, N'3', N'ccc')";
                    await command.ExecuteNonQueryAsync();
                }

                var insertedCount = await connection.InsertOrReplaceAsync(
                    new Customer
                    {
                        Id = 4,
                        Code = "4",
                        Name = "hoge",
                    });

                Assert.Equal(1, insertedCount);
            }
        }

        [Fact]
        public async Task InsertOrReplaceAsyncWhenUpdateTest()
        {
            using (var connection = fixture.CreateConnection())
            {
                await connection.OpenAsync();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        @"INSERT customers (id, code, name)
                          VALUES (1, N'1', N'aaa')
                               , (2, N'2', N'bbb')
                               , (3, N'3', N'ccc')";
                    await command.ExecuteNonQueryAsync();
                }

                var updatedCount = await connection.InsertOrReplaceAsync(
                    new Customer
                    {
                        Id = 2,
                        Code = "2",
                        Name = "foo",
                    });

                Assert.Equal(1, updatedCount);
            }
        }

        [Fact]
        public void QueryTest()
        {
            using (var connection = fixture.CreateConnection())
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        @"INSERT customers (id, code, name)
                          VALUES (1, N'1', N'aaa')
                               , (2, N'2', N'bbb')
                               , (3, N'3', N'ccc')";
                    command.ExecuteNonQuery();
                }

                var customers = connection.Query<Customer>(
                    @"SELECT * FROM customers WHERE id > 1")
                    .ToList();

                Assert.Equal(2, customers.Count);
                Assert.Equal(2, customers[0].Id);
                Assert.Equal(3, customers[1].Id);
            }
        }

        [Fact]
        public void FindWithQueryTest()
        {
            using (var connection = fixture.CreateConnection())
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        @"INSERT customers (id, code, name)
                          VALUES (1, N'1', N'aaa')
                               , (2, N'2', N'bbb')
                               , (3, N'3', N'ccc')";
                    command.ExecuteNonQuery();
                }

                var customer = connection.FindWithQuery<Customer>(
                    @"SELECT * FROM customers WHERE id > 1");

                Assert.Equal(2, customer.Id);
            }
        }

        [Fact]
        public void FindWithQueryWithTableMappingTest()
        {
            using (var connection = fixture.CreateConnection())
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        @"INSERT customers (id, code, name)
                          VALUES (1, N'1', N'aaa')
                               , (2, N'2', N'bbb')
                               , (3, N'3', N'ccc')";
                    command.ExecuteNonQuery();
                }

                var customer = connection.FindWithQuery(
                    new TableMapping(typeof(Customer)),
                    @"SELECT * FROM customers WHERE id > 1");

                Assert.Equal(2, ((Customer)customer).Id);
            }
        }

        [Fact]
        public void QueryWithParamTest()
        {
            using (var connection = fixture.CreateConnection())
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        @"INSERT customers (id, code, name)
                          VALUES (1, N'1', N'aaa')
                               , (2, N'2', N'bbb')
                               , (3, N'3', N'ccc')";
                    command.ExecuteNonQuery();
                }

                var customers = connection.Query<Customer>(
                    @"SELECT * FROM customers WHERE id > @id",
                    new
                    {
                        id = 1
                    })
                    .ToList();

                Assert.Equal(2, customers.Count);
                Assert.Equal(2, customers[0].Id);
                Assert.Equal(3, customers[1].Id);
            }
        }

        [Fact]
        public void FindWithQueryWithParamTest()
        {
            using (var connection = fixture.CreateConnection())
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        @"INSERT customers (id, code, name)
                          VALUES (1, N'1', N'aaa')
                               , (2, N'2', N'bbb')
                               , (3, N'3', N'ccc')";
                    command.ExecuteNonQuery();
                }

                var customer = connection.FindWithQuery<Customer>(
                    @"SELECT * FROM customers WHERE id > @id",
                    new
                    {
                        id = 1
                    });

                Assert.Equal(2, customer.Id);
            }
        }

        [Fact]
        public void FindWithQueryWithTableMappingAndParamTest()
        {
            using (var connection = fixture.CreateConnection())
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        @"INSERT customers (id, code, name)
                          VALUES (1, N'1', N'aaa')
                               , (2, N'2', N'bbb')
                               , (3, N'3', N'ccc')";
                    command.ExecuteNonQuery();
                }

                var customer = connection.FindWithQuery(
                    new TableMapping(typeof(Customer)),
                    @"SELECT * FROM customers WHERE id > @id",
                    new
                    {
                        id = 1
                    });

                Assert.Equal(2, ((Customer)customer).Id);
            }
        }

        [Fact]
        public async Task QueryAsyncTest()
        {
            using (var connection = fixture.CreateConnection())
            {
                await connection.OpenAsync();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        @"INSERT customers (id, code, name)
                          VALUES (1, N'1', N'aaa')
                               , (2, N'2', N'bbb')
                               , (3, N'3', N'ccc')";
                    await command.ExecuteNonQueryAsync();
                }

                var customers = (
                    await connection.QueryAsync<Customer>(
                        @"SELECT * FROM customers WHERE id > 1")
                ).ToList();

                Assert.Equal(2, customers.Count);
                Assert.Equal(2, customers[0].Id);
                Assert.Equal(3, customers[1].Id);
            }
        }

        [Fact]
        public async Task FindWithQueryAsyncTest()
        {
            using (var connection = fixture.CreateConnection())
            {
                await connection.OpenAsync();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        @"INSERT customers (id, code, name)
                          VALUES (1, N'1', N'aaa')
                               , (2, N'2', N'bbb')
                               , (3, N'3', N'ccc')";
                    await command.ExecuteNonQueryAsync();
                }

                var customer = await connection.FindWithQueryAsync<Customer>(
                    @"SELECT * FROM customers WHERE id > 1");

                Assert.Equal(2, customer.Id);
            }
        }

        [Fact]
        public async Task FindWithQueryAsyncWithTableMappingTest()
        {
            using (var connection = fixture.CreateConnection())
            {
                await connection.OpenAsync();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        @"INSERT customers (id, code, name)
                          VALUES (1, N'1', N'aaa')
                               , (2, N'2', N'bbb')
                               , (3, N'3', N'ccc')";
                    await command.ExecuteNonQueryAsync();
                }

                var customer = await connection.FindWithQueryAsync(
                    new TableMapping(typeof(Customer)),
                    @"SELECT * FROM customers WHERE id > 1");

                Assert.Equal(2, ((Customer)customer).Id);
            }
        }

        [Fact]
        public async Task QueryAsyncWithParamTest()
        {
            using (var connection = fixture.CreateConnection())
            {
                await connection.OpenAsync();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        @"INSERT customers (id, code, name)
                          VALUES (1, N'1', N'aaa')
                               , (2, N'2', N'bbb')
                               , (3, N'3', N'ccc')";
                    await command.ExecuteNonQueryAsync();
                }

                var customers = (
                    await connection.QueryAsync<Customer>(
                        @"SELECT * FROM customers WHERE id > @id",
                        new
                        {
                            id = 1
                        })
                ).ToList();

                Assert.Equal(2, customers.Count);
                Assert.Equal(2, customers[0].Id);
                Assert.Equal(3, customers[1].Id);
            }
        }

        [Fact]
        public async Task FindWithQueryAsyncWithParamTest()
        {
            using (var connection = fixture.CreateConnection())
            {
                await connection.OpenAsync();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        @"INSERT customers (id, code, name)
                          VALUES (1, N'1', N'aaa')
                               , (2, N'2', N'bbb')
                               , (3, N'3', N'ccc')";
                    await command.ExecuteNonQueryAsync();
                }

                var customer = await connection.FindWithQueryAsync<Customer>(
                    @"SELECT * FROM customers WHERE id > @id",
                    new
                    {
                        id = 1
                    });
                Assert.Equal(2, customer.Id);
            }
        }

        [Fact]
        public async Task FindWithQueryAsyncWithTableMappingAndParamTest()
        {
            using (var connection = fixture.CreateConnection())
            {
                await connection.OpenAsync();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        @"INSERT customers (id, code, name)
                          VALUES (1, N'1', N'aaa')
                               , (2, N'2', N'bbb')
                               , (3, N'3', N'ccc')";
                    await command.ExecuteNonQueryAsync();
                }

                var customer = await connection.FindWithQueryAsync(
                    new TableMapping(typeof(Customer)),
                    @"SELECT * FROM customers WHERE id > @id",
                    new
                    {
                        id = 1
                    });
                Assert.Equal(2, ((Customer)customer).Id);
            }
        }

        [Fact]
        public void ExecuteTest()
        {
            using (var connection = fixture.CreateConnection())
            {
                connection.Open();

                var result = connection.Execute(
                    @"INSERT customers (id, code, name)
                      VALUES (@id, @code, @name)",
                    new
                    {
                        id = 1,
                        code = "1",
                        name = "aaa",
                    });

                Assert.Equal(1, result);
            }
        }

        [Fact]
        public async Task ExecuteAsyncTest()
        {
            using (var connection = fixture.CreateConnection())
            {
                await connection.OpenAsync();

                var result = await connection.ExecuteAsync(
                    @"INSERT customers (id, code, name)
                      VALUES (@id, @code, @name)",
                    new
                    {
                        id = 1,
                        code = "1",
                        name = "aaa",
                    });

                Assert.Equal(1, result);
            }
        }

        [Fact]
        public void QueryWithTableMappingTest()
        {
            using (var connection = fixture.CreateConnection())
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        @"INSERT customers (id, code, name)
                          VALUES (1, N'1', N'aaa')
                               , (2, N'2', N'bbb')
                               , (3, N'3', N'ccc')";
                    command.ExecuteNonQuery();
                }

                var customers = connection.Query(
                    new TableMapping(typeof(Customer)),
                    @"SELECT * FROM customers WHERE id > 1")
                    .ToList();

                Assert.Equal(2, customers.Count);
                Assert.Equal(2, ((Customer)customers[0]).Id);
                Assert.Equal(3, ((Customer)customers[1]).Id);
            }
        }

        [Fact]
        public void QueryWithTableMappingAndParamTest()
        {
            using (var connection = fixture.CreateConnection())
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        @"INSERT customers (id, code, name)
                          VALUES (1, N'1', N'aaa')
                               , (2, N'2', N'bbb')
                               , (3, N'3', N'ccc')";
                    command.ExecuteNonQuery();
                }

                var customers = connection.Query(
                    new TableMapping(typeof(Customer)),
                    @"SELECT * FROM customers WHERE id > @id",
                    new
                    {
                        id = 1
                    })
                    .ToList();

                Assert.Equal(2, customers.Count);
                Assert.Equal(2, ((Customer)customers[0]).Id);
                Assert.Equal(3, ((Customer)customers[1]).Id);
            }
        }

        [Fact]
        public async Task QueryAsyncWithTableMappingTest()
        {
            using (var connection = fixture.CreateConnection())
            {
                await connection.OpenAsync();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        @"INSERT customers (id, code, name)
                          VALUES (1, N'1', N'aaa')
                               , (2, N'2', N'bbb')
                               , (3, N'3', N'ccc')";
                    await command.ExecuteNonQueryAsync();
                }

                var customers = (
                    await connection.QueryAsync(
                        new TableMapping(typeof(Customer)),
                        @"SELECT * FROM customers WHERE id > 1")
                ).ToList();

                Assert.Equal(2, customers.Count);
                Assert.Equal(2, ((Customer)customers[0]).Id);
                Assert.Equal(3, ((Customer)customers[1]).Id);
            }
        }

        [Fact]
        public async Task QueryAsyncWithTableMappingAndParamTest()
        {
            using (var connection = fixture.CreateConnection())
            {
                await connection.OpenAsync();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        @"INSERT customers (id, code, name)
                          VALUES (1, N'1', N'aaa')
                               , (2, N'2', N'bbb')
                               , (3, N'3', N'ccc')";
                    await command.ExecuteNonQueryAsync();
                }

                var customers = (
                    await connection.QueryAsync(
                        new TableMapping(typeof(Customer)),
                        @"SELECT * FROM customers WHERE id > @id",
                        new
                        {
                            id = 1
                        })
                ).ToList();

                Assert.Equal(2, customers.Count);
                Assert.Equal(2, ((Customer)customers[0]).Id);
                Assert.Equal(3, ((Customer)customers[1]).Id);
            }
        }

        [Fact]
        public void ExecuteScalarTest()
        {
            using (var connection = fixture.CreateConnection())
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        @"INSERT customers (id, code, name)
                          VALUES (1, N'1', N'aaa')
                               , (2, N'2', N'bbb')
                               , (3, N'3', N'ccc')";
                    command.ExecuteNonQuery();
                }

                var result = connection.ExecuteScalar<int>(
                    @"SELECT COUNT(*) FROM customers WHERE id > @id",
                    new
                    {
                        id = 1
                    });
                Assert.Equal(2, result);
            }
        }

        [Fact]
        public async Task ExecuteScalarAsyncTest()
        {
            using (var connection = fixture.CreateConnection())
            {
                await connection.OpenAsync();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        @"INSERT customers (id, code, name)
                          VALUES (1, N'1', N'aaa')
                               , (2, N'2', N'bbb')
                               , (3, N'3', N'ccc')";
                    await command.ExecuteNonQueryAsync();
                }

                var result = await connection.ExecuteScalarAsync<int>(
                    @"SELECT COUNT(*) FROM customers WHERE id > @id",
                    new
                    {
                        id = 1
                    });
                Assert.Equal(2, result);
            }
        }
    }

    [Table("customers")]
    public class Customer
    {
        [PrimaryKey]
        [Column("id")]
        public int Id { get; set; }

        [NotNull]
        [Column("code")]
        public string Code { get; set; }

        [NotNull]
        [Column("name")]
        public string Name { get; set; }
    }
}
