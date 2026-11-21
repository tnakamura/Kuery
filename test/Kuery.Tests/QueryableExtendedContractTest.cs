using System;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Kuery.Tests
{
    public abstract partial class QueryPipelineContract
    {
        [Fact]
        public async Task ComposesParameterizedSqlWithFilteringPagingAndAggregates()
        {
            using var connection = Open();
            var dialect = Kuery.Query.SqlDialectFactory.Create(connection);
            var table = dialect.EscapeIdentifier("kuery_pipeline_contract");
            var id = dialect.EscapeIdentifier("Id");
            var name = dialect.EscapeIdentifier("Name");
            var format = $"select {id}, {name} from {table} where {id} > {{0}}";
            var source = connection.FromSql<SqlContractRow>(FormattableStringFactory.Create(format, 1));

            var query = source.Where(x => x.Id < 4).OrderByDescending(x => x.Id).Take(1);
            var row = Assert.Single(await query.ToArrayAsync(TestContext.Current.CancellationToken));
            Assert.Equal(3, row.Id);
            Assert.Equal("Beta", row.Name);
            Assert.Equal(9, source.Sum(x => x.Id));
            Assert.Equal(new[] { 2, 3, 4 }, source.Join(connection.Query<ContractRow>(), a => a.Id, b => b.Id, (a, b) => a.Id).Order());

            var filteredByName = $"select {id}, {name} from {table} where {name} = {{0}}";
            var captured = connection.FromSql<SqlContractRow>(FormattableStringFactory.Create(filteredByName, "O'Brien")).TagWith("composition test");
            Assert.Empty(captured);
            Assert.DoesNotContain("O'Brien", captured.ToQueryString());
        }

        public class SqlContractRow
        {
            public int Id { get; set; }
            public string Name { get; set; }
        }

        [Fact]
        public void CorrelatedSelectManySupportsInnerAndLeftJoins()
        {
            using var connection = Open();
            var query = connection.Query<ContractRow>();
            var parents = query.OrderBy(x => x.Id).Take(3);
            var inner = parents.SelectMany(parent => query.Where(child => child.ParentId == parent.Id),
                (parent, child) => new { Parent = parent.Id, Child = child.Id });
            Assert.Equal(new[] { 12, 13, 24 }, inner.OrderBy(x => x.Child).Select(x => x.Parent * 10 + x.Child));
            var left = parents.SelectMany(parent => query.Where(child => child.ParentId == parent.Id).DefaultIfEmpty(),
                (parent, child) => new { Parent = parent.Id, Child = child == null ? 0 : child.Id });
            Assert.Equal(new[] { 12, 13, 24, 30 }, left.OrderBy(x => x.Parent).ThenBy(x => x.Child).Select(x => x.Parent * 10 + x.Child));
        }

        [Fact]
        public void PerParentPagingUsesLateralOrReportsProviderLimit()
        {
            using var connection = Open();
            var rows = connection.Query<ContractRow>();
            var query = rows.SelectMany(parent => rows.Where(child => child.ParentId == parent.Id)
                    .OrderByDescending(child => child.Id).Take(1).DefaultIfEmpty(),
                (parent, child) => new { Parent = parent.Id, Child = child == null ? 0 : child.Id });
            if (Kuery.Query.SqlDialectFactory.Create(connection).Kind == Kuery.Query.SqlDialectKind.Sqlite)
            {
                var error = Assert.Throws<NotSupportedException>(() => query.ToArray());
                Assert.Contains("LATERAL", error.Message);
                return;
            }
            Assert.Equal(new[] { 13, 24, 30, 40 }, query.OrderBy(x => x.Parent).Select(x => x.Parent * 10 + x.Child));
        }

        [Fact]
        public async Task GroupingMaterializesCollectionsAndOrderedTopElements()
        {
            using var connection = Open();
            var query = connection.Query<ContractRow>();
            var cancellationToken = TestContext.Current.CancellationToken;
            var expected = query.ToList().OrderByDescending(x => x.Id)
                .GroupBy(x => new { x.Value, x.ParentId }, x => x.Id)
                .OrderBy(g => g.Min()).Select(g => g.ToArray()).ToArray();
            var groups = query.OrderByDescending(x => x.Id)
                .GroupBy(x => new { x.Value, x.ParentId }, x => x.Id);
            var actual = (await groups.ToArrayAsync(cancellationToken)).OrderBy(g => g.Min()).Select(g => g.ToArray()).ToArray();
            Assert.Equal(expected, actual);

            var first = query.GroupBy(x => x.ParentId)
                .Select(g => g.OrderByDescending(x => x.Id).First().Id).Order().ToArray();
            Assert.Equal(new[] { 1, 3, 4 }, first);

            var top = query.GroupBy(x => x.ParentId).Select(g => new
            {
                g.Key,
                Items = g.OrderByDescending(x => x.Id).Take(2).ToList()
            }).ToList();
            Assert.Equal(new[] { 3, 2 }, top.Single(g => g.Key == 1).Items.Select(x => x.Id));
            Assert.Equal(3, top.Count);
            var emptyLists = query.GroupBy(x => x.ParentId, x => x.Id).Select(g => g.OrderBy(x => x).Take(0).ToList()).ToList();
            Assert.Equal(3, emptyLists.Count);
            Assert.All(emptyLists, items => Assert.Empty(items));
            Assert.Empty(query.Take(0).GroupBy(x => x.ParentId).ToList());
            Assert.Throws<NotSupportedException>(() => query.GroupBy(x => x.ParentId)
                .Select(g => new { First = g.OrderBy(x => x.Id).First(), Count = g.Count() }).ToList());
        }

        [Fact]
        public void FinalGroupJoinPreservesDuplicateOuterRowsAndEmptyGroups()
        {
            using var connection = Open();
            var query = connection.Query<ContractRow>();
            var outer = query.Concat(query.Where(x => x.Id == 1)).OrderBy(x => x.Id);
            var groups = outer.GroupJoin(query.OrderByDescending(x => x.Id), parent => (int?)parent.Id, child => child.ParentId,
                (parent, children) => new { parent.Id, Children = children.ToArray() }).ToList();
            Assert.Equal(new[] { 1, 1, 2, 3, 4 }, groups.Select(x => x.Id));
            Assert.Equal(new[] { 3, 2 }, groups[0].Children.Select(x => x.Id));
            Assert.Equal(new[] { 3, 2 }, groups[1].Children.Select(x => x.Id));
            Assert.Equal(4, Assert.Single(groups[2].Children).Id);
            Assert.Empty(groups[3].Children);
            Assert.Empty(groups[4].Children);
        }

        [Fact]
        public void OrderAndMinMaxByPreserveSequenceSemantics()
        {
            using var connection = Open();
            var query = connection.Query<ContractRow>();

            Assert.Equal(new[] { 1, 2, 3, 4 }, query.Select(x => x.Id).Order());
            Assert.Equal(new[] { 4, 3, 2, 1 }, query.Select(x => x.Id).OrderDescending());
            Assert.Equal(2, query.OrderBy(x => x.Id).MinBy(x => x.Value).Id);
            Assert.Equal(3, query.OrderByDescending(x => x.Id).MinBy(x => x.Value).Id);
            Assert.Equal(4, query.MaxBy(x => x.Value).Id);
            Assert.Equal(3, query.OrderBy(x => x.Id).Take(3).MaxBy(x => x.Id).Id);
            Assert.Null(query.Take(0).MinBy(x => x.Id));
            Assert.Throws<InvalidOperationException>(() => query.Take(0).Select(x => x.Id).MaxBy(x => x));
        }

        [Fact]
        public async Task AsyncExtensionsExecuteTerminalsAndReleaseTheReaderAfterAnEarlyExit()
        {
            using var connection = Open();
            var query = connection.Query<ContractRow>();
            var cancellationToken = TestContext.Current.CancellationToken;

            Assert.True(await query.AnyAsync(x => x.Id == 3, cancellationToken));
            Assert.False(await query.AllAsync(x => x.Id < 4, cancellationToken));
            Assert.True(await query.Select(x => x.Value).ContainsAsync(2, cancellationToken));
            Assert.Equal(3L, await query.LongCountAsync(x => x.Id > 1, cancellationToken));
            Assert.Equal(10, await query.SumAsync(x => x.Id, cancellationToken));
            Assert.Equal(10, await query.Select(x => x.Id).SumAsync(cancellationToken));
            Assert.Equal(2.5, await query.Select(x => x.Id).AverageAsync(cancellationToken));
            Assert.Equal(2, await query.MinAsync(x => x.Value, cancellationToken));
            Assert.Equal(4, await query.Select(x => x.Id).MaxAsync(cancellationToken));
            Assert.Equal(4, (await query.MaxByAsync(x => x.Value, cancellationToken)).Id);
            Assert.Equal(1, (await query.SingleAsync(x => x.Id == 1, cancellationToken)).Id);
            Assert.Equal(4, (await query.OrderBy(x => x.Id).LastAsync(cancellationToken)).Id);
            Assert.Equal(3, (await query.OrderBy(x => x.Id).ElementAtAsync(2, cancellationToken)).Id);
            Assert.Null(await query.OrderBy(x => x.Id).ElementAtOrDefaultAsync(8, cancellationToken));
            Assert.Equal(new[] { 1, 2, 3, 4 }, await query.Select(x => x.Id).Order().ToArrayAsync(cancellationToken));
            Assert.Equal(3, (await query.Select(x => x.Value).ToHashSetAsync(cancellationToken)).Count);
            Assert.Equal(4, (await query.ToDictionaryAsync(x => x.Id, cancellationToken)).Count);

            await foreach (var row in query.OrderBy(x => x.Id).AsAsyncEnumerable(cancellationToken))
            {
                Assert.Equal(1, row.Id);
                break;
            }
            Assert.Equal(4, await query.CountAsync(cancellationToken));

            using var canceled = new CancellationTokenSource();
            canceled.Cancel();
            await using var enumerator = query.AsAsyncEnumerable(canceled.Token).GetAsyncEnumerator();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => enumerator.MoveNextAsync().AsTask());
        }
    }
}
