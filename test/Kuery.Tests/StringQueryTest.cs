using System;
using System.Transactions;
using Xunit;

namespace Kuery.Tests
{
    public class StringQueryTest : IClassFixture<SqliteFixture>, IDisposable
    {
        readonly SqliteFixture fixture;

        readonly TransactionScope ts;

        public StringQueryTest(SqliteFixture fixture)
        {
            this.fixture = fixture;

            using (var connection = fixture.CreateConnection())
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = @"
                        if object_id (N'Product') is null
                            create table Product (
                                Name nvarchar(100) primary key not null
                            );";
                    command.ExecuteNonQuery();
                }
            }

            ts = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled);

            using (var connection = fixture.CreateConnection())
            {
                connection.Open();
                var products = new Product[]
                {
                    new Product { Name = "Foo" },
                    new Product { Name = "Bar" },
                    new Product { Name = "Foobar" },
                };
                connection.InsertAll(products);
            }
        }

        public void Dispose()
        {
            ts?.Dispose();
        }

        public class Product
        {
            public string Name { get; set; }
        }

        [Fact]
        public void StringEquals()
        {
            using (var connection = fixture.CreateConnection())
            {
                connection.Open();

                var fs = connection.Table<Product>().Where(x => x.Name == "Foo").ToList();
                Assert.Single(fs);
            }
        }

        [Fact]
        public void StartsWith()
        {
            using (var connection = fixture.CreateConnection())
            {
                connection.Open();

                var fs = connection.Table<Product>().Where(x => x.Name.StartsWith("F")).ToList();
                Assert.Equal(2, fs.Count);

                var lfs = connection.Table<Product>().Where(x => x.Name.StartsWith("f")).ToList();
                Assert.Empty(lfs);


                var lfs2 = connection.Table<Product>().Where(x => x.Name.StartsWith("f", StringComparison.OrdinalIgnoreCase)).ToList();
                Assert.Equal(2, lfs2.Count);


                var bs = connection.Table<Product>().Where(x => x.Name.StartsWith("B")).ToList();
                Assert.Single(bs);
            }
        }

        [Fact]
        public void EndsWith()
        {
            using (var connection = fixture.CreateConnection())
            {
                connection.Open();

                var fs = connection.Table<Product>().Where(x => x.Name.EndsWith("ar")).ToList();
                Assert.Equal(2, fs.Count);

                var lfs = connection.Table<Product>().Where(x => x.Name.EndsWith("Ar")).ToList();
                Assert.Empty(lfs);

                var bs = connection.Table<Product>().Where(x => x.Name.EndsWith("o")).ToList();
                Assert.Single(bs);
            }
        }

        [Fact]
        public void Contains()
        {
            using (var connection = fixture.CreateConnection())
            {
                connection.Open();

                var fs = connection.Table<Product>().Where(x => x.Name.Contains("o")).ToList();
                Assert.Equal(2, fs.Count);

                var lfs = connection.Table<Product>().Where(x => x.Name.Contains("O")).ToList();
                Assert.Empty(lfs);

                var lfsu = connection.Table<Product>().Where(x => x.Name.ToUpper().Contains("O")).ToList();
                Assert.Equal(2, lfsu.Count);

                var bs = connection.Table<Product>().Where(x => x.Name.Contains("a")).ToList();
                Assert.Equal(2, bs.Count);

                var zs = connection.Table<Product>().Where(x => x.Name.Contains("z")).ToList();
                Assert.Empty(zs);
            }
        }
    }
}
