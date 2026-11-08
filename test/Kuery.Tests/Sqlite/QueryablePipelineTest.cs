using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Kuery.Linq;
using Microsoft.Data.Sqlite;
using Xunit;

namespace Kuery.Tests.Sqlite
{
    public class QueryablePipelineTest : IDisposable
    {
        readonly SqliteConnection _connection;
        readonly PipelineRow[] _rows =
        {
            new PipelineRow { Id = 1, Amount = 1.5, Created = new DateTime(2020, 1, 1) },
            new PipelineRow { Id = 2, ParentId = 1, Name = "Alpha", Value = 1, Amount = 2.5, Created = new DateTime(2020, 2, 1) },
            new PipelineRow { Id = 3, ParentId = 1, Name = "beta", Value = 1, Amount = 3.5, Created = new DateTime(2021, 1, 1) },
            new PipelineRow { Id = 4, ParentId = 2, Name = "Alpha", Value = 3, Amount = 4.5, Created = new DateTime(2021, 3, 1) },
            new PipelineRow { Id = 5, ParentId = 2, Name = "", Amount = 5.5, Created = new DateTime(2022, 1, 1) },
            new PipelineRow { Id = 6, ParentId = 3, Name = "tail  ", Value = -2, Amount = 6.5, Created = new DateTime(2022, 2, 1) },
        };

        public QueryablePipelineTest()
        {
            _connection = new SqliteConnection("Data Source=:memory:");
            _connection.Open();
            _connection.Execute("create table pipeline_rows (Id integer primary key, ParentId integer null, Name text null, Value integer null, Amount real not null, Created text not null)");
            foreach (var row in _rows) _connection.Insert(row);
        }

        public void Dispose() => _connection.Dispose();
        IQueryable<PipelineRow> Query => _connection.Query<PipelineRow>();

        [Fact]
        public void PagingAndFilteringPreserveOperatorOrder()
        {
            Assert.Empty(Query.OrderBy(x => x.Id).Take(2).Where(x => x.Id > 3));
            Assert.Equal(new[] { 3 }, Query.OrderBy(x => x.Id).Take(3).Skip(2).Select(x => x.Id));
            Assert.Equal(new[] { 4, 5 }, Query.OrderBy(x => x.Id).Skip(1).Skip(2).Take(2).Take(4).Select(x => x.Id));
            Assert.Equal(2, Query.Take(2).Count());
            Assert.Equal(1, Query.OrderBy(x => x.Id).Take(3).Count(x => x.Id > 2));
            Assert.Equal(3, Query.OrderBy(x => x.Id).Take(3).Last().Id);
            Assert.Empty(Query.Take(0));
            Assert.Empty(Query.Skip(2).Take(-1));
            Assert.Equal(6, Query.Skip(-10).Count());
        }

        [Fact]
        public void RandomOperatorSequencesMatchLinqToObjects()
        {
            var random = new Random(841);
            for (int iteration = 0; iteration < 80; iteration++)
            {
                IQueryable<PipelineRow> actual = Query.OrderBy(x => x.Id);
                IQueryable<PipelineRow> expected = _rows.AsQueryable().OrderBy(x => x.Id);
                for (int step = 0; step < 5; step++)
                {
                    var count = random.Next(-1, 8);
                    var kind = random.Next(5);
                    Func<IQueryable<PipelineRow>, IQueryable<PipelineRow>> operation = kind switch
                    {
                        0 => q => q.Skip(count),
                        1 => q => q.Take(count),
                        2 => q => q.Where(x => x.Id > count),
                        3 => q => q.OrderByDescending(x => x.Id),
                        _ => q => q.OrderBy(x => x.Id),
                    };
                    actual = operation(actual);
                    expected = operation(expected);
                }
                Assert.Equal(expected.Select(x => x.Id).ToArray(), actual.Select(x => x.Id).ToArray());
                Assert.Equal(expected.Count(), actual.Count());
            }
        }

        [Fact]
        public void RepeatedOrderByReplacesOrderingAndPagingKeepsItsBoundary()
        {
            Assert.Equal(new[] { 6, 5, 4 }, Query.OrderBy(x => x.Name).OrderByDescending(x => x.Id).Take(3).Select(x => x.Id));
            Assert.Equal(new[] { 3, 2, 1 }, Query.OrderBy(x => x.Id).Take(3).OrderByDescending(x => x.Id).Select(x => x.Id));
        }

        [Fact]
        public void ComputedAndConstructorProjectionsCanBeComposed()
        {
            var result = Query.Select(x => new { Number = x.Id * 2, Text = (x.Name ?? "none").ToUpper() })
                .Where(x => x.Number > 5).OrderBy(x => x.Text.Length).ThenBy(x => x.Number)
                .Select(x => new PipelineDto(x.Number + 1, x.Text)).ToList();
            var expected = _rows.Select(x => new { Number = x.Id * 2, Text = (x.Name ?? "none").ToUpper() })
                .Where(x => x.Number > 5).OrderBy(x => x.Text.Length).ThenBy(x => x.Number)
                .Select(x => new PipelineDto(x.Number + 1, x.Text)).ToList();
            Assert.Equal(expected.Select(x => (x.Number, x.Text)), result.Select(x => (x.Number, x.Text)));
            Assert.Equal(_rows.Sum(x => x.Id * 2), Query.Select(x => x.Id * 2).Sum());
            Assert.Equal(3.5, Query.Select(x => x.Id).Average());
            Assert.Equal(_rows.Select(x => new { Value = x.Id + 1 }).Where(x => x.Value < 4).Select(x => x.Value),
                Query.Select(x => new ProjectionDto { Value = x.Id + 1 }).Where(x => x.Value < 4).Select(x => x.Value));
        }

        [Fact]
        public void DistinctAndAggregateOperateOnTheProjectedSequence()
        {
            Assert.Equal(5, Query.Select(x => x.Name).Distinct().Count());
            Assert.Equal(_rows.Select(x => x.Value).Distinct().Sum(), Query.Select(x => x.Value).Distinct().Sum());
            Assert.Equal(new[] { "Alpha", "beta", "tail  " }, Query.Select(x => x.Name).Distinct().Where(x => x != null && x != "").OrderBy(x => x));
            Assert.Equal(3, Query.Select(x => new { x.ParentId, x.Value }).Distinct().Take(3).Count());
        }

        [Fact]
        public void NullComparisonsAndTheirNegationsMatchCSharp()
        {
            Assert.Equal(_rows.Where(x => x.Value != x.ParentId).Select(x => x.Id), Query.Where(x => x.Value != x.ParentId).Select(x => x.Id));
            Assert.Equal(_rows.Where(x => !(x.Value == x.ParentId)).Select(x => x.Id), Query.Where(x => !(x.Value == x.ParentId)).Select(x => x.Id));
            Assert.Equal(_rows.Where(x => !(x.Value != x.ParentId)).Select(x => x.Id), Query.Where(x => !(x.Value != x.ParentId)).Select(x => x.Id));
            Assert.Equal(_rows.Where(x => !(x.Value > 0)).Select(x => x.Id), Query.Where(x => !(x.Value > 0)).Select(x => x.Id));
            string name = null;
            var query = Query.Where(x => x.Name == name);
            Assert.Equal(1, query.Single().Id);
            name = "Alpha";
            Assert.Equal(new[] { 2, 4 }, query.OrderBy(x => x.Id).Select(x => x.Id));
            Assert.Equal(_rows.Select(x => x.Value == x.ParentId), Query.OrderBy(x => x.Id).Select(x => x.Value == x.ParentId));
        }

        [Fact]
        public void ContainsHandlesNullEmptyAndChangingCollections()
        {
            var names = new List<string> { null, "Alpha" };
            var query = Query.Where(x => names.Contains(x.Name)).OrderBy(x => x.Id);
            Assert.Equal(new[] { 1, 2, 4 }, query.Select(x => x.Id));
            names.Clear();
            Assert.Empty(query);
            names.Add("beta");
            Assert.Equal(3, query.Single().Id);
            names.Add(null);
            Assert.Equal(new[] { 2, 4, 5, 6 }, Query.Where(x => !names.Contains(x.Name)).OrderBy(x => x.Id).Select(x => x.Id));
            var nullableNames = Query.Where(x => x.Id < 3).Select(x => x.Name);
            Assert.Equal(new[] { 1, 2, 4 }, Query.Where(x => nullableNames.Contains(x.Name)).OrderBy(x => x.Id).Select(x => x.Id));
        }

        [Fact]
        public void SelfJoinUsesIndependentFilteredAndPagedSources()
        {
            var query = Query.Where(x => x.Id < 4).Join(Query.OrderBy(x => x.Id).Take(4),
                parent => parent.Id, child => child.ParentId, (parent, child) => new { Parent = parent.Name, Child = child.Id })
                .Where(x => x.Child > 2).OrderBy(x => x.Child);
            Assert.Equal(new[] { 3, 4 }, query.Select(x => x.Child));
            Assert.Equal(2, query.Count());
            Assert.Equal(4, query.Last().Child);
        }

        [Fact]
        public void LeftJoinCanMaterializeAnAbsentEntity()
        {
            var result = (from parent in Query.OrderBy(x => x.Id)
                          join child in Query.Where(x => x.Id <= 3) on parent.Id equals child.ParentId into children
                          from child in children.DefaultIfEmpty()
                          select new { parent.Id, Child = child, Number = child == null ? -1 : child.Id }).ToList();
            Assert.Equal(7, result.Count);
            Assert.Equal(5, result.Count(x => x.Child == null));
            Assert.All(result.Where(x => x.Child == null), x => Assert.Equal(-1, x.Number));
        }

        [Fact]
        public void ComputedGroupKeysHavingAndOuterCompositionWork()
        {
            var actual = Query.GroupBy(x => x.Created.Year)
                .Where(g => g.Count() > 1).Select(g => new { Year = g.Key, Total = g.Sum(x => x.Id + 1), Count = g.Count(x => x.Value != null) })
                .Where(x => x.Total > 5).OrderBy(x => x.Year).ToList();
            var expected = _rows.GroupBy(x => x.Created.Year)
                .Where(g => g.Count() > 1).Select(g => new { Year = g.Key, Total = g.Sum(x => x.Id + 1), Count = g.Count(x => x.Value != null) })
                .Where(x => x.Total > 5).OrderBy(x => x.Year).ToList();
            Assert.Equal(expected, actual);
            Assert.Equal(3, Query.GroupBy(x => x.Created.Year).Select(g => new { g.Key, Count = g.Count() }).Count());
            Assert.Equal(3, Query.GroupBy(x => x.Created.Year).Count());
            Assert.True(Query.GroupBy(x => x.Created.Year).Any(g => g.Count() == 2));
            Assert.True(Query.GroupBy(x => x.Created.Year).All(g => g.Count() == 2));
        }

        [Fact]
        public void SetOperationsPreserveGroupingPagingAndProjection()
        {
            var first = Query.OrderBy(x => x.Id).Take(2).Select(x => x.Id * 2);
            var second = Query.OrderByDescending(x => x.Id).Take(2).Select(x => x.Id * 2);
            Assert.Equal(new[] { 4, 10, 12 }, first.Union(second).Where(x => x > 2).OrderBy(x => x));
            Assert.Equal(4, first.Concat(second).Count());
            Assert.Equal(new[] { 2 }, first.Union(second).Except(first.Where(x => x > 2).Union(second)).ToArray());
            Assert.Equal(28, first.Concat(second).Sum());
        }

        [Fact]
        public void SetOperandsAlignMembersByTheirTargetRatherThanInitializerOrder()
        {
            var left = Query.Where(x => x.Id == 1).Select(x => new PairDto { First = x.Id, Second = x.Id + 10 });
            var right = Query.Where(x => x.Id == 2).Select(x => new PairDto { Second = x.Id + 20, First = x.Id * 10 });
            var result = left.Concat(right).OrderBy(x => x.First).ToList();
            Assert.Equal(new[] { (1, 11), (20, 22) }, result.Select(x => (x.First, x.Second)));
            Assert.Throws<NotSupportedException>(() => left.Union(Query.Select(x => new PairDto { First = x.Id })).ToList());
        }

        [Fact]
        public void BoxingAndConstantObjectConstructionKeepTheirClrTypes()
        {
            Assert.IsType<int>(Query.OrderBy(x => x.Id).Select(x => (object)x.Id).First());
            var row = Query.Select(x => new PairDto { First = 42, Second = 43 }).First();
            Assert.Equal(42, row.First);
            Assert.Equal(43, row.Second);
            Assert.Equal(_rows.Select(x => x.Id / 2m), Query.OrderBy(x => x.Id).Select(x => x.Id / 2m));
        }

        [Fact]
        public void MaterializationPreservesMappedScalarTypesAndTextEnums()
        {
            _connection.Execute("create table typed_row (Id integer primary key, Status text, Token text, Optional integer, Duration text, Stamp text, Data blob)");
            var token = Guid.NewGuid();
            var stamp = new DateTimeOffset(2020, 2, 3, 4, 5, 6, TimeSpan.FromHours(9));
            _connection.Insert(new TypedRow { Id = 1, Status = PipelineStatus.Ready, Token = token, Duration = TimeSpan.FromSeconds(3), Stamp = stamp, Data = new byte[] { 0, 1 } });
            var rows = _connection.Query<TypedRow>();
            var row = rows.Where(x => x.Status == PipelineStatus.Ready).Single();
            Assert.Equal(token, row.Token);
            Assert.Null(row.Optional);
            Assert.Equal(TimeSpan.FromSeconds(3), row.Duration);
            Assert.Equal(stamp, row.Stamp);
            Assert.Equal(new byte[] { 0, 1 }, row.Data);
            Assert.Equal(PipelineStatus.Ready, rows.Select(x => x.Status).Single());
            PipelineStatus status = PipelineStatus.Ready;
            var query = rows.Where(x => x.Status.Equals(status));
            Assert.Single(query);
            status = PipelineStatus.Done;
            Assert.Empty(query);
        }

        [Fact]
        public void CorrelatedMutationUsesTheOuterAlias()
        {
            var children = Query;
            Assert.Equal(3, Query.Where(parent => children.Any(child => child.ParentId == parent.Id))
                .ExecuteUpdate(s => s.SetProperty(x => x.Name, "has children")));
            Assert.Equal(new[] { 1, 2, 3 }, Query.Where(x => x.Name == "has children").OrderBy(x => x.Id).Select(x => x.Id));
        }

        [Fact]
        public void LeftJoinPresenceDoesNotDependOnAnyMappedColumnBeingNonNull()
        {
            _connection.Execute("create table keyless_row (Name text null)");
            _connection.Execute("insert into keyless_row (Name) values (null)");
            var inner = _connection.Query<KeylessRow>();
            var result = (from row in Query.Where(x => x.Id <= 2).OrderBy(x => x.Id)
                          join value in inner on row.Id equals 1 into values
                          from value in values.DefaultIfEmpty()
                          select new { row.Id, Value = value }).ToList();
            Assert.NotNull(result[0].Value);
            Assert.Null(result[0].Value.Name);
            Assert.Null(result[1].Value);
        }

        [Fact]
        public void ExistsAndAllRespectPagingAndCorrelation()
        {
            Assert.False(Query.OrderBy(x => x.Id).Take(2).Any(x => x.Id > 3));
            Assert.True(Query.OrderBy(x => x.Id).Take(2).All(x => x.Id < 3));
            Assert.True(Query.Where(x => x.Id < 0).All(x => x.Value > 0));
            var children = Query.Where(x => x.Value != null);
            Assert.Equal(new[] { 1, 2, 3 }, Query.Where(parent => children.Any(child => child.ParentId == parent.Id)).OrderBy(x => x.Id).Select(x => x.Id));
            Assert.Equal(new[] { 2, 1, 1, 0, 0, 0 }, Query.OrderBy(x => x.Id).Select(parent => children.Count(child => child.ParentId == parent.Id)));
        }

        [Fact]
        public async Task AsyncTerminalOperatorsMatchSyncAndHonorCancellation()
        {
            var query = Query.OrderBy(x => x.Id).Take(3).Select(x => x.Id + 1);
            Assert.Equal(query.ToList(), await query.ToListAsync());
            Assert.Equal(query.Count(), await query.CountAsync());
            Assert.Equal(query.First(), await query.FirstAsync());
            Assert.Equal(0, await query.Where(x => x < 0).FirstOrDefaultAsync());
            await Assert.ThrowsAsync<InvalidOperationException>(() => query.Where(x => x < 0).FirstAsync());
            using var cancellation = new CancellationTokenSource();
            cancellation.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => query.ToListAsync(cancellation.Token));
        }

        [Fact]
        public void ScalarTerminalsHandleEmptyAndDuplicateResults()
        {
            Assert.Null(Query.Where(x => x.Id < 0).Select(x => x.Name).FirstOrDefault());
            Assert.Equal(0, Query.Where(x => x.Id < 0).Select(x => x.Id).SingleOrDefault());
            Assert.Throws<InvalidOperationException>(() => Query.Select(x => x.Name).Single());
            Assert.Equal(0, Query.Where(x => x.Id < 0).Select(x => x.Id).Sum());
            Assert.Null(Query.Where(x => x.Id < 0).Select(x => x.Value).Min());
            Assert.Throws<InvalidOperationException>(() => Query.Where(x => x.Id < 0).Select(x => x.Id).Min());
            Assert.Equal(0, Query.OrderBy(x => x.Id).Select(x => x.Id).ElementAtOrDefault(-1));
            Assert.Throws<ArgumentOutOfRangeException>(() => Query.OrderBy(x => x.Id).Select(x => x.Id).ElementAt(20));
            Assert.Throws<ArgumentOutOfRangeException>(() => Query.OrderBy(x => x.Id).Select(x => x.Id).ElementAt(-1));
            Assert.Equal(6L, Query.LongCount());
        }

        [Fact]
        public void UnsupportedExpressionsAndMixedRootsAreRejected()
        {
            Assert.Throws<NotSupportedException>(() => Query.Select(x => UserFunction(x.Id)).ToList());
            Assert.Equal(6, Query.AsEnumerable().Select(x => UserFunction(x.Id)).Count());
            Assert.Throws<NotSupportedException>(() => Query.Where((x, index) => index > 0).ToList());
            using var other = new SqliteConnection("Data Source=:memory:");
            Assert.Throws<NotSupportedException>(() => Query.Join(other.Query<PipelineRow>(), x => x.Id, x => x.Id, (x, y) => x).ToList());
            Assert.Throws<NotSupportedException>(() => Query.Union(_rows.AsQueryable()).ToList());
            Assert.Throws<NotSupportedException>(() => Query.Select(x => x.Value).GroupBy(x => x).ToList());
            Assert.Throws<NotSupportedException>(() => Query.Select(x => new PipelineDto(x.Id, x.Name)).Where(x => x.Number > 1).ToList());
            var entity = _rows[0];
            Assert.Throws<NotSupportedException>(() => Query.Where(x => x == entity).ToList());
            Assert.Throws<NotSupportedException>(() => Query.SelectMany(parent => Query.Where(child => child.ParentId == parent.Id)).ToList());
            Assert.Throws<NotSupportedException>(() => Query.Select(x => x.Id).Distinct(EqualityComparer<int>.Default).ToList());
            Assert.Throws<NotSupportedException>(() => Query.OrderBy(x => x.Name, StringComparer.OrdinalIgnoreCase).ToList());
        }

        [Fact]
        public async Task UpdatesAndDeletesShareTranslationAndRetainGuards()
        {
            int increment = 2;
            Assert.Equal(2, Query.Where(x => x.Id <= 2).ExecuteUpdate(s => s.SetProperty(x => x.Amount, x => x.Amount + increment).SetProperty(x => x.Name, x => (x.Name ?? "none").ToUpper())));
            increment = 3;
            Assert.Equal(2, await Query.Where(x => x.Id <= 2).ExecuteUpdateAsync(s => s.SetProperty(x => x.Amount, x => x.Amount + increment)));
            Assert.Equal(6.5, Query.First(x => x.Id == 1).Amount);
            Assert.Equal("NONE", Query.First(x => x.Id == 1).Name);
            Assert.Equal(2, await Query.Where(x => x.Amount > 6 && x.Id < 3).ExecuteDeleteAsync());
            Assert.Throws<InvalidOperationException>(() => Query.ExecuteDelete());
            Assert.Throws<NotSupportedException>(() => Query.Take(1).Where(x => x.Id > 0).ExecuteDelete());
        }

        [Fact]
        public void PlansAreReusedWithoutRetainingParameterValues()
        {
            int minimum = 1;
            var query = Query.Where(x => x.Id > minimum).Select(x => x.Name);
            var executor = new KueryLinqExecutor();
            var first = executor.Prepare(new KueryQueryContext(_connection), query.Expression, typeof(IEnumerable<string>), false, out var firstPlan);
            minimum = 4;
            var second = executor.Prepare(new KueryQueryContext(_connection), query.Expression, typeof(IEnumerable<string>), false, out var secondPlan);
            Assert.Same(firstPlan, secondPlan);
            Assert.Equal(first.CommandText, second.CommandText);
            Assert.Equal(1, first.Parameters.Single().Value);
            Assert.Equal(4, second.Parameters.Single().Value);
            Assert.DoesNotContain("select *", first.CommandText);
            Assert.Contains(".\"Name\"", first.CommandText);
        }

        [Fact]
        public void FixedSqlTemplatesContainSlotMetadataWithoutValues()
        {
            int minimum = 1;
            var query = Query.Where(x => x.Id > minimum).Select(x => x.Id);
            var executor = new KueryLinqExecutor();
            var first = executor.Prepare(new KueryQueryContext(_connection), query.Expression, typeof(IEnumerable<int>), false, out var plan);
            Assert.NotNull(plan.CommandTemplate);
            Assert.All(plan.CommandTemplate.Parameters, p => Assert.Null(p.Value));
            minimum = 4;
            var second = executor.Prepare(new KueryQueryContext(_connection), query.Expression, typeof(IEnumerable<int>), false, out var reused);
            Assert.Same(plan, reused);
            Assert.Equal(first.CommandText, second.CommandText);
            Assert.Equal(4, second.Parameters.Single().Value);
            Assert.All(plan.CommandTemplate.Parameters, p => Assert.Null(p.Value));
        }

        [Fact]
        public void CachedPlansDoNotKeepConnectionsOrCapturedObjectsAlive()
        {
            var references = CompileWithoutRetainingInputs();
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
            Assert.False(references.Connection.IsAlive);
            Assert.False(references.Payload.IsAlive);
            GC.KeepAlive(references.Plan);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        static (WeakReference Connection, WeakReference Payload, RelationalQueryPlan Plan) CompileWithoutRetainingInputs()
        {
            using var connection = new SqliteConnection("Data Source=:memory:");
            var payload = new CapturePayload { Minimum = 2 };
            var query = connection.Query<PipelineRow>().Where(x => x.Id >= payload.Minimum)
                .Select(x => new LeakProjection { Id = x.Id });
            new KueryLinqExecutor().Prepare(new KueryQueryContext(connection), query.Expression,
                typeof(IEnumerable<LeakProjection>), false, out var plan);
            return (new WeakReference(connection), new WeakReference(payload), plan);
        }

        [Fact]
        public async Task SharedPlansKeepDifferentConnectionsAndConcurrentValuesSeparate()
        {
            var results = await Task.WhenAll(Enumerable.Range(1, 12).Select(value => Task.Run(() =>
            {
                using var connection = new SqliteConnection("Data Source=:memory:");
                connection.Open();
                connection.Execute("create table pipeline_rows (Id integer primary key, ParentId integer, Name text, Value integer, Amount real, Created text)");
                connection.Insert(new PipelineRow { Id = value, Name = "row" + value, Created = new DateTime(2020, 1, 1) });
                return connection.Query<PipelineRow>().Where(x => x.Id == value).Select(x => x.Name).Single();
            })));
            Assert.Equal(Enumerable.Range(1, 12).Select(x => "row" + x), results);
        }

        [Fact]
        public void PlanCacheEvictsLeastRecentlyUsedEntriesAtItsBound()
        {
            string prefix = Guid.NewGuid().ToString();
            var original = new RelationalQueryPlan();
            Assert.Same(original, QueryPlanCache.GetOrAdd(prefix, () => original));
            for (int i = 0; i < 257; i++) QueryPlanCache.GetOrAdd(prefix + i, () => new RelationalQueryPlan());
            Assert.True(QueryPlanCache.Count <= 256);
            Assert.NotSame(original, QueryPlanCache.GetOrAdd(prefix, () => new RelationalQueryPlan()));
        }

        static int UserFunction(int value) => value + 1;

        [Table("pipeline_rows")]
        public class PipelineRow
        {
            [PrimaryKey]
            public int Id { get; set; }
            public int? ParentId { get; set; }
            public string Name { get; set; }
            public int? Value { get; set; }
            public double Amount { get; set; }
            public DateTime Created { get; set; }
        }

        public class PipelineDto
        {
            public PipelineDto(int number, string text) { Number = number; Text = text; }
            public int Number { get; }
            public string Text { get; }
        }

        public class ProjectionDto
        {
            public int Value { get; set; }
        }

        public class PairDto
        {
            public int First { get; set; }
            public int Second { get; set; }
        }

        public class LeakProjection
        {
            public int Id { get; set; }
        }

        sealed class CapturePayload
        {
            public int Minimum { get; set; }
        }

        [Table("keyless_row")]
        public class KeylessRow
        {
            public string Name { get; set; }
        }

        [StoreAsText]
        public enum PipelineStatus { Ready = 1, Done = 2 }

        [Table("typed_row")]
        public class TypedRow
        {
            [PrimaryKey]
            public int Id { get; set; }
            public PipelineStatus Status { get; set; }
            public Guid Token { get; set; }
            public bool? Optional { get; set; }
            public TimeSpan Duration { get; set; }
            public DateTimeOffset Stamp { get; set; }
            public byte[] Data { get; set; }
        }
    }
}
