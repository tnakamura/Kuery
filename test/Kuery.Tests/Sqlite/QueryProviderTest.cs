using System;
using System.Data.Common;
using System.Linq;
using Xunit;
using Xunit.Abstractions;

namespace Kuery.Tests.Sqlite
{
    public class QueryProviderTest : IClassFixture<SqliteFixture>, IDisposable
    {
        [Table("books")]
        class Book
        {
            [PrimaryKey]
            [Column("id")]
            public string Id { get; set; }

            [NotNull]
            [Column("code")]
            public string Code { get; set; }

            [NotNull]
            [Column("title")]
            public string Title { get; set; }

            [NotNull]
            [Column("summary")]
            public string Summary { get; set; }

            [NotNull]
            [Column("price")]
            public int Price { get; set; }
        }

        private readonly SqliteFixture fixture;

        private readonly DbConnection connection;

        private readonly ITestOutputHelper output;

        private readonly DbQueryProvider provider;

        public QueryProviderTest(ITestOutputHelper output, SqliteFixture fixture)
        {
            this.output = output;
            this.fixture = fixture;
            connection = fixture.CreateConnection();
            provider = new DbQueryProvider(connection);

            connection.Open();
            using (var command = connection.CreateCommand())
            {
                command.CommandText = @"
                    create table if not exists [Books] (
                        [Id] nvarchar(50) not null primary key,
                        [Code] nvarchar(50) not null,
                        [Title] nvarchar(100) not null,
                        [Summary] nvarchar(100) not null,
                        [Price] integer not null
                    );";
                command.ExecuteNonQuery();
            }
        }

        public void Dispose()
        {
            using (var command = connection.CreateCommand())
            {
                command.CommandText = $@"
                    drop table if exists Books;";
                command.ExecuteNonQuery();
            }
            connection.Dispose();
        }

        [Fact]
        public void WhereTest()
        {
            var query = new Query<Book>(provider)
                .Where(x => x.Price >= 1000);

            output.WriteLine(query.ToString());
        }

        [Fact]
        public void WhereOrderByTest()
        {
            var query = new Query<Book>(provider)
                .Where(x => x.Price >= 1000)
                .OrderBy(x => x.Code);

            output.WriteLine(query.ToString());
        }
    }
}
