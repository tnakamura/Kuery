using System;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using Kuery.Query;
using Kuery.Query.Internal;
using Kuery.Query.SqlExpressions;
using Xunit;

namespace Kuery.Tests
{
    // Run the same behavioral contract against every provider through the existing test scripts.
    public abstract class QueryPipelineContract
    {
        readonly Func<IDbConnection> _open;
        protected QueryPipelineContract(Func<IDbConnection> open) { _open = open; }

        IDbConnection Open()
        {
            var connection = _open();
            try
            {
                var dialect = SqlDialectFactory.Create(connection);
                string Q(string identifier) => dialect.EscapeIdentifier(identifier);
                connection.Execute("drop table if exists " + Q("kuery_pipeline_contract"));
                var dateType = dialect.Kind == SqlDialectKind.SqlServer ? "datetime2" : dialect.Kind == SqlDialectKind.MySql ? "datetime" : dialect.Kind == SqlDialectKind.Sqlite ? "text" : "timestamp";
                var boolType = dialect.Kind == SqlDialectKind.PostgreSql ? "boolean" : dialect.Kind == SqlDialectKind.SqlServer ? "bit" : "integer";
                connection.Execute("create table " + Q("kuery_pipeline_contract") + " (" + Q("Id") + " integer primary key, "
                    + Q("ParentId") + " integer null, " + Q("Name") + " varchar(80) null, " + Q("Value") + " integer null, "
                    + Q("Amount") + " decimal(18, 2) not null, " + Q("Created") + " " + dateType + " not null, " + Q("Enabled") + " " + boolType + " not null)");
                connection.Insert(new ContractRow { Id = 1, Amount = 1.5m, Created = new DateTime(2020, 1, 1), Enabled = true });
                connection.Insert(new ContractRow { Id = 2, ParentId = 1, Name = "Alpha", Value = 2, Amount = 2.5m, Created = new DateTime(2020, 2, 1) });
                connection.Insert(new ContractRow { Id = 3, ParentId = 1, Name = "Beta", Value = 2, Amount = 3.5m, Created = new DateTime(2021, 1, 1), Enabled = true });
                connection.Insert(new ContractRow { Id = 4, ParentId = 2, Name = "Alpha", Value = 4, Amount = 4.5m, Created = new DateTime(2021, 2, 1) });
                return connection;
            }
            catch { connection.Dispose(); throw; }
        }

        [Fact]
        public void ComputedProjectionPagingAndReordering()
        {
            using var connection = Open();
            var query = connection.Query<ContractRow>();
            Assert.Equal(new[] { 4, 2 }, query.OrderBy(x => x.Id).Take(2).OrderByDescending(x => x.Id).Select(x => x.Id * 2));
            Assert.Equal(new[] { "BETA", "ALPHA" }, query.Select(x => new { Number = x.Id + 1, Text = x.Name.ToUpper() }).Where(x => x.Number > 3).OrderBy(x => x.Number).Select(x => x.Text));
            Assert.Empty(query.Skip(1).Take(0));
            Assert.Equal(1, query.OrderBy(x => x.Id).Take(2).Skip(1).Count());
            Assert.Equal(4, query.OrderBy(x => x.Id).Take(4).Last().Id);
            Assert.Equal(new[] { true, false, true, false }, query.OrderBy(x => x.Id).Select(x => x.Enabled));
            Assert.Equal(new[] { false, true, true, true }, query.OrderBy(x => x.Id).Select(x => x.Id > 1));
            Assert.Equal(2, query.Count(x => x.Enabled));
        }

        [Fact]
        public void NullAndCollectionSemantics()
        {
            using var connection = Open();
            var query = connection.Query<ContractRow>();
            Assert.Equal(new[] { 2, 3, 4 }, query.Where(x => x.Value != x.ParentId).OrderBy(x => x.Id).Select(x => x.Id));
            Assert.Equal(new[] { 2, 3, 4 }, query.Where(x => !(x.Value == x.ParentId)).OrderBy(x => x.Id).Select(x => x.Id));
            string name = null;
            var filtered = query.Where(x => x.Name == name);
            Assert.Equal(1, filtered.Single().Id);
            name = "Alpha";
            Assert.Equal(2, filtered.Count());
            var names = new[] { null, "Alpha" };
            Assert.Equal(new[] { 1, 2, 4 }, query.Where(x => names.Contains(x.Name)).OrderBy(x => x.Id).Select(x => x.Id));
            var nullableNames = query.Where(x => x.Id <= 2).Select(x => x.Name);
            Assert.Equal(new[] { 1, 2, 4 }, query.Where(x => nullableNames.Contains(x.Name)).OrderBy(x => x.Id).Select(x => x.Id));
        }

        [Fact]
        public void IndependentJoinInputsAndLeftJoinEntities()
        {
            using var connection = Open();
            var query = connection.Query<ContractRow>();
            var joined = query.Where(x => x.Id < 3).Join(query.OrderBy(x => x.Id).Take(3), parent => parent.Id, child => child.ParentId,
                (parent, child) => new { Parent = parent.Id, Child = child.Id });
            Assert.Equal(new[] { 2, 3 }, joined.OrderBy(x => x.Child).Select(x => x.Child));
            var left = (from parent in query
                        join child in query on parent.Id equals child.ParentId into children
                        from child in children.DefaultIfEmpty()
                        select new { parent.Id, Child = child, IsMissing = child == null }).OrderBy(x => x.Id).ToList();
            Assert.Equal(5, left.Count);
            Assert.Equal(2, left.Count(x => x.Child == null));
            Assert.All(left.Where(x => x.Child == null), x => Assert.True(x.IsMissing));
        }

        [Fact]
        public void GroupingSetOperationsAndAggregates()
        {
            using var connection = Open();
            var query = connection.Query<ContractRow>();
            var groups = query.GroupBy(x => x.Created.Year).Where(g => g.Count() > 1)
                .Select(g => new { Year = g.Key, Sum = g.Sum(x => x.Id * 2), Count = g.Count(x => x.Enabled) }).Where(g => g.Sum > 7).ToList();
            Assert.Single(groups);
            Assert.Equal(2021, groups[0].Year);
            Assert.Equal(14, groups[0].Sum);
            Assert.Equal(1, groups[0].Count);
            Assert.Equal(3, query.Select(x => x.Name).Distinct().Count());
            var first = query.OrderBy(x => x.Id).Take(2).Select(x => x.Id);
            var last = query.OrderByDescending(x => x.Id).Take(2).Select(x => x.Id);
            Assert.Equal(new[] { 2, 3 }, first.Union(last).OrderBy(x => x).Skip(1).Take(2));
            Assert.Equal(10, first.Concat(last).Sum());
            Assert.Equal(new[] { 1 }, first.Except(last.Union(first.Where(x => x > 1))));
            Assert.Equal(2.5, query.Select(x => x.Id).Average());
            Assert.Equal(4L, query.LongCount());
        }

        [Fact]
        public async Task CorrelationAndAsyncTerminals()
        {
            using var connection = Open();
            var query = connection.Query<ContractRow>();
            Assert.Equal(new[] { 1, 2 }, query.Where(parent => query.Any(child => child.ParentId == parent.Id)).OrderBy(x => x.Id).Select(x => x.Id));
            Assert.Equal(new[] { 2, 1, 0, 0 }, query.OrderBy(x => x.Id).Select(parent => query.Count(child => child.ParentId == parent.Id)));
            Assert.False(query.Take(0).Any());
            Assert.True(query.OrderBy(x => x.Id).Take(2).All(x => x.Id < 3));
            Assert.Equal(2, await query.Take(2).CountAsync());
            Assert.Equal(2, await query.OrderBy(x => x.Id).Select(x => x.Id * 2).FirstAsync());
            Assert.Equal(0, await query.Where(x => x.Id < 0).Select(x => x.Id).FirstOrDefaultAsync());
        }

        [Fact]
        public async Task UpdateAndDeleteUseTheSharedScalarTranslator()
        {
            using var connection = Open();
            var query = connection.Query<ContractRow>();
            Assert.Equal(2, await query.Where(x => x.Id <= 2).ExecuteUpdateAsync(s => s.SetProperty(x => x.Amount, x => x.Amount + 2).SetProperty(x => x.Name, x => (x.Name ?? "none").ToUpper())));
            Assert.Equal("NONE", query.First(x => x.Id == 1).Name);
            Assert.Equal(3.5m, query.First(x => x.Id == 1).Amount);
            Assert.Equal(2, await query.Where(x => x.Id > 2).ExecuteDeleteAsync());
            Assert.Throws<InvalidOperationException>(() => query.ExecuteDelete());
            Assert.Throws<NotSupportedException>(() => query.OrderBy(x => x.Id).Where(x => x.Id > 0).ExecuteDelete());
        }

        [Table("kuery_pipeline_contract")]
        public class ContractRow
        {
            [PrimaryKey]
            public int Id { get; set; }
            public int? ParentId { get; set; }
            public string Name { get; set; }
            public int? Value { get; set; }
            public decimal Amount { get; set; }
            public DateTime Created { get; set; }
            public bool Enabled { get; set; }
        }
    }
}

namespace Kuery.Tests.Sqlite
{
    public class QueryableProviderTest : Kuery.Tests.QueryPipelineContract
    {
        public QueryableProviderTest() : base(() =>
        {
            var connection = new Microsoft.Data.Sqlite.SqliteConnection("Data Source=:memory:");
            connection.Open();
            return connection;
        }) { }
    }
}

namespace Kuery.Tests.Npgsql
{
    public class QueryableProviderTest : Kuery.Tests.QueryPipelineContract, IClassFixture<NpgsqlFixture>
    {
        public QueryableProviderTest(NpgsqlFixture fixture) : base(() => fixture.OpenNewConnection()) { }
    }
}

namespace Kuery.Tests.SqlClient
{
    public class QueryableProviderTest : Kuery.Tests.QueryPipelineContract, IClassFixture<SqlClientFixture>
    {
        public QueryableProviderTest(SqlClientFixture fixture) : base(() => fixture.OpenNewConnection()) { }
    }
}

namespace Kuery.Tests.MySql
{
    public class QueryableProviderTest : Kuery.Tests.QueryPipelineContract, IClassFixture<MySqlFixture>
    {
        public QueryableProviderTest(MySqlFixture fixture) : base(() => fixture.OpenNewConnection()) { }
    }
}
