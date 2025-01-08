using System;
using System.Data.Common;
using System.Linq;
using Xunit;
using Xunit.Abstractions;

namespace Kuery.Tests.Sqlite
{
    public class QueryProviderTest : IClassFixture<SqliteFixture>, IDisposable
    {
        private readonly SqliteFixture fixture;

        private readonly DbQueryProvider provider;

        private readonly ITestOutputHelper output;

        private readonly DbConnection connection;

        public QueryProviderTest(SqliteFixture fixture, ITestOutputHelper output)
        {
            this.fixture = fixture;
            this.output = output;
            connection = this.fixture.CreateConnection();
            provider = new DbQueryProvider(connection);
        }

        public void Dispose()
        {
            connection.Dispose();
        }

        [Fact]
        public void Where()
        {
            var query = new Query<Product>(provider)
                .Where(x => x.Name == "foo");

            var sql = query.ToString();

            output.WriteLine(sql);

            Assert.NotNull(sql);
        }

        [Fact]
        public void Select()
        {
            var query = new Query<Product>(provider)
                .Select(x => new
                {
                    x.Id,
                    x.Name,
                });

            var sql = query.ToString();

            output.WriteLine(sql);

            Assert.NotNull(sql);
        }

        [Fact]
        public void WhereSelect()
        {
            var query = new Query<Product>(provider)
                .Where(x => x.Name == "foo")
                .Select(x => new
                {
                    x.Id,
                    x.Name,
                });

            var sql = query.ToString();

            output.WriteLine(sql);

            Assert.NotNull(sql);
        }
    }
}
