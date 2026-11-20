using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Kuery.Query;
using Kuery.Query.Internal;
using Kuery.Query.SqlExpressions;
using Microsoft.Data.Sqlite;
using Xunit;
using static Kuery.Tests.QueryPipelineContract;

namespace Kuery.Tests.Sqlite
{
    public class QueryableOperatorInfrastructureTest
    {
        [Theory]
        [InlineData((int)SqlDialectKind.Sqlite, false)]
        [InlineData((int)SqlDialectKind.PostgreSql, true)]
        [InlineData((int)SqlDialectKind.SqlServer, true)]
        [InlineData((int)SqlDialectKind.MySql, false)]
        public void FullJoinUsesTheDialectAndPreservesTheCachedTree(int dialectKind, bool native)
        {
            var kind = (SqlDialectKind)dialectKind;
            using var connection = new SqliteConnection("Data Source=:memory:");
            var rows = connection.Query<ContractRow>();
            var expression = rows.Where(x => x.Id > 2).FullJoin(rows.Where(x => x.Id < 8), x => x.ParentId, x => x.ParentId,
                (left, right) => new { Left = left == null ? -1 : left.Id, Right = right == null ? -1 : right.Id }).Expression;
            var normalizer = new ExpressionTreeFuncletizer(connection);
            var plan = new QueryCompilationContext().CreateQueryExecutor(normalizer.ExtractParameters(expression));
            ISqlDialect dialect = kind == SqlDialectKind.Sqlite ? new SqliteDialect() : kind == SqlDialectKind.PostgreSql ? new PostgreSqlDialect()
                : kind == SqlDialectKind.SqlServer ? new SqlServerDialect() : new MySqlDialect();
            var first = new QuerySqlGenerator(dialect, normalizer.Values).GetCommand(plan.Query);
            var second = new QuerySqlGenerator(dialect, normalizer.Values).GetCommand(plan.Query);
            Assert.Equal(first.CommandText, second.CommandText);
            if (native) Assert.Contains(" full outer join ", first.CommandText);
            else
            {
                Assert.Contains(" union all ", first.CommandText);
                Assert.DoesNotContain(" full outer join ", first.CommandText);
                Assert.Contains(" is null", first.CommandText);
            }
            Assert.Contains(first.Parameters, p => Equals(p.Value, 2));
            Assert.Contains(first.Parameters, p => Equals(p.Value, 8));
            Assert.DoesNotContain("select *", new QuerySqlGenerator(new SqlServerDialect(), normalizer.Values).GetCommand(plan.Query).CommandText);
        }

        [Fact]
        public void EntityIdentityParametersAreFreshAndDoNotRetainCapturedEntities()
        {
            using var connection = new SqliteConnection("Data Source=:memory:");
            var rows = connection.Query<ContractRow>();
            var item = new ContractRow { Id = 2 };
            var query = rows.Where(x => rows.Contains(item));
            var compiler = new QueryCompiler();
            var first = compiler.CompileQuery(new QueryContext(connection), query.Expression, typeof(IEnumerable<ContractRow>), false, out var plan);
            item.Id = 4;
            var second = compiler.CompileQuery(new QueryContext(connection), query.Expression, typeof(IEnumerable<ContractRow>), false, out var reused);
            Assert.Same(plan, reused);
            Assert.Contains(first.Parameters, p => Equals(p.Value, 2));
            Assert.Contains(second.Parameters, p => Equals(p.Value, 4));
            if (plan.CommandTemplate != null) Assert.All(plan.CommandTemplate.Parameters, p => Assert.Null(p.Value));
            var references = CompileEntityIdentity();
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
            Assert.False(references.Item.IsAlive);
            Assert.False(references.Connection.IsAlive);
            GC.KeepAlive(references.Plan);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        static (WeakReference Item, WeakReference Connection, CompiledQuery Plan) CompileEntityIdentity()
        {
            using var connection = new SqliteConnection("Data Source=:memory:");
            var item = new ContractRow { Id = 3, Name = new string('x', 10000) };
            var rows = connection.Query<ContractRow>();
            var query = rows.Where(x => rows.Contains(item));
            new QueryCompiler().CompileQuery(new QueryContext(connection), query.Expression, typeof(IEnumerable<ContractRow>), false, out var plan);
            return (new WeakReference(item), new WeakReference(connection), plan);
        }

        [Fact]
        public async Task NewOperatorsKeepConcurrentConnectionsAndValuesSeparate()
        {
            var results = await Task.WhenAll(Enumerable.Range(1, 12).Select(id => Task.Run(() =>
            {
                using var connection = new SqliteConnection("Data Source=:memory:");
                connection.Open();
                connection.Execute("create table kuery_pipeline_contract (Id integer primary key)");
                connection.Insert(new IdentityRow { Id = id });
                var query = connection.Query<IdentityRow>();
                return query.Contains(new IdentityRow { Id = id }) && !query.Contains(new IdentityRow { Id = id + 1 })
                    && query.Select(x => x.Id).DefaultIfEmpty().OrderBy(x => x).Reverse().Single() == id;
            })));
            Assert.All(results, Assert.True);
        }

        [Fact]
        public void NewOperatorsRejectMixedRootsAndUnsupportedComparers()
        {
            using var first = new SqliteConnection("Data Source=:memory:");
            using var second = new SqliteConnection("Data Source=:memory:");
            var rows = first.Query<ContractRow>();
            Assert.Throws<NotSupportedException>(() => rows.FullJoin(second.Query<ContractRow>(), x => x.Id, x => x.Id, (a, b) => a).ToList());
            Assert.Throws<NotSupportedException>(() => rows.LeftJoin(second.Query<ContractRow>(), x => x.Id, x => x.Id, (a, b) => a).ToList());
            Assert.Throws<NotSupportedException>(() => rows.RightJoin(second.Query<ContractRow>(), x => x.Id, x => x.Id, (a, b) => a).ToList());
            Assert.Throws<NotSupportedException>(() => rows.Select(x => x.Id).Contains(1, EqualityComparer<int>.Default));
            Assert.Throws<NotSupportedException>(() => first.Query<KeylessRow>().Contains(new KeylessRow()));
        }

        [Fact]
        public void TphSupportsAbstractRootsAndValidatesMetadata()
        {
            using var connection = new SqliteConnection("Data Source=:memory:");
            connection.Open();
            connection.Execute("create table abstract_tph (Id integer, Kind text, Value integer)");
            connection.Execute("insert into abstract_tph values (1, 'leaf', 9)");
            Assert.Equal(9, Assert.IsType<AbstractLeaf>(connection.Query<AbstractRoot>().Single()).Value);
            Assert.Equal(1, connection.Query<AbstractRoot>().OfType<AbstractLeaf>().Single().Id);
            Assert.Throws<NotSupportedException>(() => connection.Query<InvalidRoot>().ToList());
        }

        [Table("kuery_pipeline_contract")]
        public class IdentityRow { [PrimaryKey] public int Id { get; set; } }
        public class KeylessRow { public int Id { get; set; } }
        [Table("abstract_tph"), Discriminator("Kind"), DerivedType(typeof(AbstractLeaf), "leaf")]
        public abstract class AbstractRoot { public int Id { get; set; } }
        public class AbstractLeaf : AbstractRoot { public int Value { get; set; } }
        [Discriminator("Kind"), DerivedType(typeof(InvalidLeaf), "same"), DerivedType(typeof(InvalidOtherLeaf), "same")]
        public abstract class InvalidRoot { }
        public class InvalidLeaf : InvalidRoot { }
        public class InvalidOtherLeaf : InvalidRoot { }
    }
}
