using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.Common;
using System.Data.SqlClient;
using System.Threading.Tasks;
using System.Transactions;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Kuery.Tests
{
    [TestClass]
    public class SqlHelperTest
    {
        const string DbName = "kuery_test";

        static DbConnection CreateConnection(string database = DbName)
        {
            var css = ConfigurationManager.ConnectionStrings["Default"];
            var csb = new SqlConnectionStringBuilder(css.ConnectionString);
            csb.InitialCatalog = database ?? DbName;
            return new SqlConnection(csb.ToString());
        }

        [ClassInitialize]
        public static void ClassInitialize(TestContext testContext)
        {
            using (var connection = CreateConnection("master"))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                       $@"IF DB_ID (N'{DbName}') IS NOT NULL
                            DROP DATABASE [{DbName}]";
                    command.ExecuteNonQuery();
                }
                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        $@"IF DB_ID (N'{DbName}') IS NULL
                            CREATE DATABASE [{DbName}]";
                    command.ExecuteNonQuery();
                }
            }

            using (var connection = CreateConnection())
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
                        @"IF OBJECT_ID (N'dbo.customers') IS NULL
                            CREATE TABLE customers (
                              id INTEGER PRIMARY KEY NOT NULL,
                              code NVARCHAR(50) NOT NULL,
                              name NVARCHAR(50) NOT NULL
                            )";
                    command.ExecuteNonQuery();
                }
            }
        }

        TransactionScope transactionScope;

        [TestInitialize]
        public void TestInitialize()
        {
            transactionScope = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled);
        }

        [TestCleanup]
        public void TestCleanup()
        {
            transactionScope?.Dispose();
        }

        public TestContext TestContext { get; set; }

        [TestMethod]
        public void ToListTest()
        {
            using (var connection = CreateConnection())
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
                Assert.AreEqual(1, result.Count);
                Assert.AreEqual(2, result[0].Id);
                Assert.AreEqual("2", result[0].Code);
                Assert.AreEqual("Google", result[0].Name);
            }
        }

        [TestMethod]
        public async Task ToListAsyncTest()
        {
            using (var connection = CreateConnection())
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
                Assert.AreEqual(1, result.Count);
                Assert.AreEqual(3, result[0].Id);
                Assert.AreEqual("3", result[0].Code);
                Assert.AreEqual("Apple", result[0].Name);
            }
        }

        [TestMethod]
        public async Task CountAsyncTest()
        {
            using (var connection = CreateConnection())
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
                Assert.AreEqual(1, result);
            }
        }

        [TestMethod]
        public void CountTest()
        {
            using (var connection = CreateConnection())
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
                Assert.AreEqual(1, result);
            }
        }

        [TestMethod]
        public void DeleteWithPredicateTest()
        {
            using (var connection = CreateConnection())
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
                Assert.AreEqual(1, result);
            }
        }

        [TestMethod]
        public void DeleteWithWhereTest()
        {
            using (var connection = CreateConnection())
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
                Assert.AreEqual(1, result);
            }
        }

        [TestMethod]
        public async Task InsertAsyncTest()
        {
            using (var connection = CreateConnection())
            {
                connection.Open();

                var expected = new Customer
                {
                    Id = 7,
                    Code = "7",
                    Name = "Salesforce",
                };
                var inserted = await connection.InsertAsync(expected);
                Assert.AreEqual(1, inserted);

                var actual = await connection.Table<Customer>()
                    .ToListAsync();
                Assert.AreEqual(1, actual.Count);
                Assert.AreEqual(expected.Name, actual[0].Name);
                Assert.AreEqual(expected.Id, actual[0].Id);
            }
        }

        [TestMethod]
        public async Task UpdateAsyncTest()
        {
            using (var connection = CreateConnection())
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
                Assert.AreEqual(1, updated);

                var actual = await connection.Table<Customer>()
                    .ToListAsync();
                Assert.AreEqual(1, actual.Count);
                Assert.AreEqual(expected.Name, actual[0].Name);
                Assert.AreEqual(expected.Id, actual[0].Id);
            }
        }

        [TestMethod]
        public void WhereTest()
        {
            using (var connection = CreateConnection())
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
                Assert.AreEqual(1, actual.Count);
                Assert.AreEqual(9, actual[0].Id);
                Assert.AreEqual("GitHub", actual[0].Name);
            }
        }

        [TestMethod]
        public void WhereAndWhereTest()
        {
            using (var connection = CreateConnection())
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
                Assert.AreEqual(1, actual.Count);
                Assert.AreEqual(10, actual[0].Id);
                Assert.AreEqual("Netflix", actual[0].Name);
            }
        }

        [TestMethod]
        public void InsertTest()
        {
            using (var connection = CreateConnection())
            {
                connection.Open();

                var expected = new Customer
                {
                    Id = 11,
                    Code = "11",
                    Name = "Slack",
                };
                var inserted = connection.Insert(expected);
                Assert.AreEqual(1, inserted);

                var actual = connection.Table<Customer>()
                    .ToList();
                Assert.AreEqual(1, actual.Count);
                Assert.AreEqual(expected.Name, actual[0].Name);
                Assert.AreEqual(expected.Id, actual[0].Id);
            }
        }

        [TestMethod]
        public void UpdateTest()
        {
            using (var connection = CreateConnection())
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
                Assert.AreEqual(1, updated);

                var actual = connection.Table<Customer>()
                    .ToList();
                Assert.AreEqual(1, actual.Count);
                Assert.AreEqual(expected.Name, actual[0].Name);
                Assert.AreEqual(expected.Id, actual[0].Id);
            }
        }

        [TestMethod]
        public void DeleteItemTest()
        {
            using (var connection = CreateConnection())
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
                Assert.AreEqual(1, result);
            }
        }

        [TestMethod]
        public async Task DeleteItemAsyncTest()
        {
            using (var connection = CreateConnection())
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
                Assert.AreEqual(1, result);
            }
        }

        [TestMethod]
        public void ToArrayTest()
        {
            using (var connection = CreateConnection())
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
                Assert.AreEqual(1, result.Length);
                Assert.AreEqual(15, result[0].Id);
                Assert.AreEqual("15", result[0].Code);
                Assert.AreEqual("Soundcloud", result[0].Name);
            }
        }

        [TestMethod]
        public async Task ToArrayAsyncTest()
        {
            using (var connection = CreateConnection())
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
                Assert.AreEqual(1, result.Length);
                Assert.AreEqual(16, result[0].Id);
                Assert.AreEqual("16", result[0].Code);
                Assert.AreEqual("Cloudflare", result[0].Name);
            }
        }

        [TestMethod]
        public void CountWithExpressionTest()
        {
            using (var connection = CreateConnection())
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
                Assert.AreEqual(1, result);
            }
        }

        [TestMethod]
        public async Task CountAsyncWithExpressionTest()
        {
            using (var connection = CreateConnection())
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
                Assert.AreEqual(1, result);
            }
        }

        [TestMethod]
        public async Task DeleteAsyncWithPredicateTest()
        {
            using (var connection = CreateConnection())
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
                Assert.AreEqual(1, result);
            }
        }

        [TestMethod]
        public async Task DeleteAsyncWithWhereTest()
        {
            using (var connection = CreateConnection())
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
                Assert.AreEqual(1, result);
            }
        }

        [TestMethod]
        public void OrderByTest()
        {
            using (var connection = CreateConnection())
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
                Assert.AreEqual(2, result.Count);
                Assert.AreEqual("21", result[0].Code);
                Assert.AreEqual("22", result[1].Code);
            }
        }

        [TestMethod]
        public void OrderByDescendingTest()
        {
            using (var connection = CreateConnection())
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
                Assert.AreEqual(3, result.Count);
                Assert.AreEqual("25", result[0].Code);
                Assert.AreEqual("24", result[1].Code);
                Assert.AreEqual("23", result[2].Code);
            }
        }

        [TestMethod]
        public async Task OrderByAsyncTest()
        {
            using (var connection = CreateConnection())
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
                Assert.AreEqual(2, result.Count);
                Assert.AreEqual("21", result[0].Code);
                Assert.AreEqual("22", result[1].Code);
            }
        }

        [TestMethod]
        public async Task OrderByDescendingAsyncTest()
        {
            using (var connection = CreateConnection())
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
                Assert.AreEqual(3, result.Count);
                Assert.AreEqual("25", result[0].Code);
                Assert.AreEqual("24", result[1].Code);
                Assert.AreEqual("23", result[2].Code);
            }
        }

        [TestMethod]
        public void TakeTest()
        {
            using (var connection = CreateConnection())
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
                Assert.AreEqual(1, result.Count);
                Assert.AreEqual("23", result[0].Code);
            }
        }

        [TestMethod]
        public async Task TakeAsyncTest()
        {
            using (var connection = CreateConnection())
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
                Assert.AreEqual(1, result.Count);
                Assert.AreEqual("23", result[0].Code);
            }
        }

        [TestMethod]
        public void SkipTest()
        {
            using (var connection = CreateConnection())
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
                Assert.AreEqual(2, result.Count);
                Assert.AreEqual("24", result[0].Code);
                Assert.AreEqual("25", result[1].Code);
            }
        }

        [TestMethod]
        public async Task SkipAsyncTest()
        {
            using (var connection = CreateConnection())
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
                Assert.AreEqual(2, result.Count);
                Assert.AreEqual("24", result[0].Code);
                Assert.AreEqual("25", result[1].Code);
            }
        }

        [TestMethod]
        public void TakeSkipTest()
        {
            using (var connection = CreateConnection())
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
                Assert.AreEqual(1, result.Count);
                Assert.AreEqual("24", result[0].Code);
            }
        }

        [TestMethod]
        public async Task TakeSkipAsyncTest()
        {
            using (var connection = CreateConnection())
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
                Assert.AreEqual(1, result.Count);
                Assert.AreEqual("24", result[0].Code);
            }
        }

        [TestMethod]
        public async Task FirstAsyncTest()
        {
            using (var connection = CreateConnection())
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
                Assert.AreEqual("23", result.Code);
            }
        }

        [TestMethod]
        public async Task FirstAsyncWithPredicateTest()
        {
            using (var connection = CreateConnection())
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
                Assert.AreEqual("24", result.Code);
            }
        }

        [TestMethod]
        public async Task FirstOrDefaultAsyncTest()
        {
            using (var connection = CreateConnection())
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
                Assert.AreEqual("23", result.Code);
            }
        }

        [TestMethod]
        public async Task FirstOrDefaultAsyncWithPredicateTest()
        {
            using (var connection = CreateConnection())
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
                Assert.AreEqual("24", result.Code);
            }
        }

        [TestMethod]
        public void FirstTest()
        {
            using (var connection = CreateConnection())
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
                Assert.AreEqual("23", result.Code);
            }
        }

        [TestMethod]
        public void FirstWithPredicateTest()
        {
            using (var connection = CreateConnection())
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
                Assert.AreEqual("24", result.Code);
            }
        }

        [TestMethod]
        public void FirstOrDefaultTest()
        {
            using (var connection = CreateConnection())
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
                Assert.AreEqual("23", result.Code);
            }
        }

        [TestMethod]
        public void FirstOrDefaultWithPredicateTest()
        {
            using (var connection = CreateConnection())
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
                Assert.AreEqual("24", result.Code);
            }
        }

        [TestMethod]
        public void ElementAtTest()
        {
            using (var connection = CreateConnection())
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
                Assert.AreEqual("23", result.Code);
            }
        }

        [TestMethod]
        public async Task ElementAtAsyncTest()
        {
            using (var connection = CreateConnection())
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
                Assert.AreEqual("24", result.Code);
            }
        }

        [TestMethod]
        public void GetEnumeratorTest()
        {
            using (var connection = CreateConnection())
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
                Assert.IsTrue(result.MoveNext());
                Assert.AreEqual("23", result.Current.Code);
                Assert.IsTrue(result.MoveNext());
                Assert.AreEqual("24", result.Current.Code);
                Assert.IsTrue(result.MoveNext());
                Assert.AreEqual("25", result.Current.Code);
                Assert.IsFalse(result.MoveNext());
            }
        }

        [TestMethod]
        public void ThenByTest()
        {
            using (var connection = CreateConnection())
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
                Assert.AreEqual("aaa", result[0].Name);
                Assert.AreEqual("bbb", result[1].Name);
                Assert.AreEqual("ccc", result[2].Name);
            }
        }

        [TestMethod]
        public void ThenByDescendingTest()
        {
            using (var connection = CreateConnection())
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
                Assert.AreEqual("ccc", result[0].Name);
                Assert.AreEqual("bbb", result[1].Name);
                Assert.AreEqual("aaa", result[2].Name);
            }
        }

        [TestMethod]
        public async Task ThenByAsyncTest()
        {
            using (var connection = CreateConnection())
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
                Assert.AreEqual("aaa", result[0].Name);
                Assert.AreEqual("bbb", result[1].Name);
                Assert.AreEqual("ccc", result[2].Name);
            }
        }

        [TestMethod]
        public async Task ThenByDescendingAsyncTest()
        {
            using (var connection = CreateConnection())
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
                Assert.AreEqual("ccc", result[0].Name);
                Assert.AreEqual("bbb", result[1].Name);
                Assert.AreEqual("aaa", result[2].Name);
            }
        }

        [TestMethod]
        public void FindTest()
        {
            using (var connection = CreateConnection())
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
                Assert.AreEqual(2, result.Id);
                Assert.AreEqual("2", result.Code);
                Assert.AreEqual("bbb", result.Name);
            }
        }

        [TestMethod]
        public async Task FindAsyncTest()
        {
            using (var connection = CreateConnection())
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
                Assert.AreEqual(2, result.Id);
                Assert.AreEqual("2", result.Code);
                Assert.AreEqual("bbb", result.Name);
            }
        }

        [TestMethod]
        public void GetTest()
        {
            using (var connection = CreateConnection())
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
                Assert.AreEqual(2, result.Id);
                Assert.AreEqual("2", result.Code);
                Assert.AreEqual("bbb", result.Name);
            }
        }

        [TestMethod]
        public async Task GetAsyncTest()
        {
            using (var connection = CreateConnection())
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
                Assert.AreEqual(2, result.Id);
                Assert.AreEqual("2", result.Code);
                Assert.AreEqual("bbb", result.Name);
            }
        }

        [TestMethod]
        public void GetWithPredicateTest()
        {
            using (var connection = CreateConnection())
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
                Assert.AreEqual(2, result.Id);
                Assert.AreEqual("2", result.Code);
                Assert.AreEqual("bbb", result.Name);
            }
        }

        [TestMethod]
        public async Task GetAsyncWithPredicateTest()
        {
            using (var connection = CreateConnection())
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
                Assert.AreEqual(2, result.Id);
                Assert.AreEqual("2", result.Code);
                Assert.AreEqual("bbb", result.Name);
            }
        }

        [TestMethod]
        public void FindWithPredicateTest()
        {
            using (var connection = CreateConnection())
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
                Assert.AreEqual(2, result.Id);
                Assert.AreEqual("2", result.Code);
                Assert.AreEqual("bbb", result.Name);
            }
        }

        [TestMethod]
        public async Task FindAsyncWithPredicateTest()
        {
            using (var connection = CreateConnection())
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
                Assert.AreEqual(2, result.Id);
                Assert.AreEqual("2", result.Code);
                Assert.AreEqual("bbb", result.Name);
            }
        }

        [TestMethod]
        public void DeleteWithPrimaryKeyTest()
        {
            using (var connection = CreateConnection())
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
                Assert.AreEqual(1, deleteCount);
            }
        }

        [TestMethod]
        public async Task DeleteAsyncWithPrimaryKeyTest()
        {
            using (var connection = CreateConnection())
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
                Assert.AreEqual(1, deleteCount);
            }
        }

        [TestMethod]
        public void InsertAllTest()
        {
            using (var connection = CreateConnection())
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

                Assert.AreEqual(2, insertedCount);
            }
        }

        [TestMethod]
        public async Task InsertAllAsyncTest()
        {
            using (var connection = CreateConnection())
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

                Assert.AreEqual(2, insertedCount);
            }
        }

        [TestMethod]
        public void InsertAllWithTypeTest()
        {
            using (var connection = CreateConnection())
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

                Assert.AreEqual(2, insertedCount);
            }
        }

        [TestMethod]
        public async Task InsertAllAsyncWithTypeTest()
        {
            using (var connection = CreateConnection())
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

                Assert.AreEqual(2, insertedCount);
            }
        }

        [TestMethod]
        public async Task UpdateAllAsyncTest()
        {
            using (var connection = CreateConnection())
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

                Assert.AreEqual(2, updatedCount);
            }
        }

        [TestMethod]
        public void UpdateAllTest()
        {
            using (var connection = CreateConnection())
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

                Assert.AreEqual(2, updatedCount);
            }
        }

        [TestMethod]
        public void InsertOrReplaceWhenInsertTest()
        {
            using (var connection = CreateConnection())
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

                Assert.AreEqual(1, insertedCount);
            }
        }

        [TestMethod]
        public void InsertOrReplaceWhenUpdateTest()
        {
            using (var connection = CreateConnection())
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

                Assert.AreEqual(1, updatedCount);
            }
        }

        [TestMethod]
        public async Task InsertOrReplaceAsyncWhenInsertTest()
        {
            using (var connection = CreateConnection())
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

                Assert.AreEqual(1, insertedCount);
            }
        }

        [TestMethod]
        public async Task InsertOrReplaceAsyncWhenUpdateTest()
        {
            using (var connection = CreateConnection())
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

                Assert.AreEqual(1, updatedCount);
            }
        }

        [TestMethod]
        public void QueryTest()
        {
            using (var connection = CreateConnection())
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

                Assert.AreEqual(2, customers.Count);
                Assert.AreEqual(2, customers[0].Id);
                Assert.AreEqual(3, customers[1].Id);
            }
        }

        [TestMethod]
        public void FindWithQueryTest()
        {
            using (var connection = CreateConnection())
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

                Assert.AreEqual(2, customer.Id);
            }
        }

        [TestMethod]
        public void FindWithQueryWithTableMappingTest()
        {
            using (var connection = CreateConnection())
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

                Assert.AreEqual(2, ((Customer)customer).Id);
            }
        }

        [TestMethod]
        public void QueryWithParamTest()
        {
            using (var connection = CreateConnection())
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

                Assert.AreEqual(2, customers.Count);
                Assert.AreEqual(2, customers[0].Id);
                Assert.AreEqual(3, customers[1].Id);
            }
        }

        [TestMethod]
        public void FindWithQueryWithParamTest()
        {
            using (var connection = CreateConnection())
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

                Assert.AreEqual(2, customer.Id);
            }
        }

        [TestMethod]
        public void FindWithQueryWithTableMappingAndParamTest()
        {
            using (var connection = CreateConnection())
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

                Assert.AreEqual(2, ((Customer)customer).Id);
            }
        }

        [TestMethod]
        public async Task QueryAsyncTest()
        {
            using (var connection = CreateConnection())
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

                Assert.AreEqual(2, customers.Count);
                Assert.AreEqual(2, customers[0].Id);
                Assert.AreEqual(3, customers[1].Id);
            }
        }

        [TestMethod]
        public async Task FindWithQueryAsyncTest()
        {
            using (var connection = CreateConnection())
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

                Assert.AreEqual(2, customer.Id);
            }
        }

        [TestMethod]
        public async Task FindWithQueryAsyncWithTableMappingTest()
        {
            using (var connection = CreateConnection())
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

                Assert.AreEqual(2, ((Customer)customer).Id);
            }
        }

        [TestMethod]
        public async Task QueryAsyncWithParamTest()
        {
            using (var connection = CreateConnection())
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

                Assert.AreEqual(2, customers.Count);
                Assert.AreEqual(2, customers[0].Id);
                Assert.AreEqual(3, customers[1].Id);
            }
        }

        [TestMethod]
        public async Task FindWithQueryAsyncWithParamTest()
        {
            using (var connection = CreateConnection())
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
                Assert.AreEqual(2, customer.Id);
            }
        }

        [TestMethod]
        public async Task FindWithQueryAsyncWithTableMappingAndParamTest()
        {
            using (var connection = CreateConnection())
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
                Assert.AreEqual(2, ((Customer)customer).Id);
            }
        }

        [TestMethod]
        public void ExecuteTest()
        {
            using (var connection = CreateConnection())
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

                Assert.AreEqual(1, result);
            }
        }

        [TestMethod]
        public async Task ExecuteAsyncTest()
        {
            using (var connection = CreateConnection())
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

                Assert.AreEqual(1, result);
            }
        }

        [TestMethod]
        public void QueryWithTableMappingTest()
        {
            using (var connection = CreateConnection())
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

                Assert.AreEqual(2, customers.Count);
                Assert.AreEqual(2, ((Customer)customers[0]).Id);
                Assert.AreEqual(3, ((Customer)customers[1]).Id);
            }
        }

        [TestMethod]
        public void QueryWithTableMappingAndParamTest()
        {
            using (var connection = CreateConnection())
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

                Assert.AreEqual(2, customers.Count);
                Assert.AreEqual(2, ((Customer)customers[0]).Id);
                Assert.AreEqual(3, ((Customer)customers[1]).Id);
            }
        }

        [TestMethod]
        public async Task QueryAsyncWithTableMappingTest()
        {
            using (var connection = CreateConnection())
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

                Assert.AreEqual(2, customers.Count);
                Assert.AreEqual(2, ((Customer)customers[0]).Id);
                Assert.AreEqual(3, ((Customer)customers[1]).Id);
            }
        }

        [TestMethod]
        public async Task QueryAsyncWithTableMappingAndParamTest()
        {
            using (var connection = CreateConnection())
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

                Assert.AreEqual(2, customers.Count);
                Assert.AreEqual(2, ((Customer)customers[0]).Id);
                Assert.AreEqual(3, ((Customer)customers[1]).Id);
            }
        }

        [TestMethod]
        public void ExecuteScalarTest()
        {
            using (var connection = CreateConnection())
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
                Assert.AreEqual(2, result);
            }
        }

        [TestMethod]
        public async Task ExecuteScalarAsyncTest()
        {
            using (var connection = CreateConnection())
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
                Assert.AreEqual(2, result);
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
