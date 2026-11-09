using System;
using System.Linq;
using System.Linq.Expressions;
using Kuery.Query;
using Kuery.Query.Internal;
using Kuery.Query.SqlExpressions;
using Microsoft.Data.Sqlite;
using Xunit;

namespace Kuery.Tests.Sqlite
{
    public class QueryableSqlGenerationTest
    {
        static ISqlDialect Dialect(int kind) => kind switch
        {
            0 => new SqliteDialect(),
            1 => new PostgreSqlDialect(),
            2 => new SqlServerDialect(),
            _ => new MySqlDialect(),
        };

        static RelationalCommand Generate(Expression expression, SqliteConnection connection, ISqlDialect dialect)
        {
            var normalized = new ExpressionTreeFuncletizer(connection);
            var plan = new QueryCompilationContext().CreateQueryExecutor(normalized.Visit(expression));
            return new QuerySqlGenerator(dialect, normalized.Values).GetCommand(plan.Query);
        }

        [Theory]
        [InlineData(0)]
        [InlineData(1)]
        [InlineData(2)]
        [InlineData(3)]
        public void PagingAndProjectionUseScopedAliasesAndTypedParameters(int kind)
        {
            using var connection = new SqliteConnection();
            var dialect = Dialect(kind);
            string userValue = "' OR 1=1 --";
            var query = connection.Query<QueryPipelineContract.ContractRow>().OrderBy(x => x.Id).Take(2)
                .Where(x => x.Name == userValue).OrderByDescending(x => x.Id).Skip(1).Take(0)
                .Select(x => new { Result = x.Id > 1, Text = x.Name.ToUpper(), Year = x.Created.Year });
            var command = Generate(query.Expression, connection, dialect);
            Assert.DoesNotContain(userValue, command.CommandText);
            Assert.Contains(command.Parameters, p => Equals(p.Value, userValue) && p.Type == typeof(string));
            Assert.Contains("from (select", command.CommandText);
            Assert.Contains(dialect.EscapeIdentifier("t1") + "." + dialect.EscapeIdentifier("c0"), command.CommandText);
            Assert.Contains("upper(", command.CommandText);
            Assert.DoesNotContain("select *", command.CommandText);
            Assert.Equal(command.Parameters.Count, command.Parameters.Select(p => p.Name).Distinct().Count());
            if (kind == 2)
            {
                Assert.Contains("case when", command.CommandText);
                Assert.Contains("cast(1 as bit)", command.CommandText);
                Assert.Contains("fetch next (case when", command.CommandText);
                Assert.Contains("datepart(year", command.CommandText);
            }
            else
            {
                Assert.Contains(" limit ", command.CommandText);
                Assert.Contains(" offset ", command.CommandText);
                if (kind == 3)
                {
                    Assert.DoesNotContain("limit (case", command.CommandText);
                    Assert.DoesNotContain("offset (case", command.CommandText);
                    Assert.Contains(command.Parameters, p => Equals(p.Value, 0));
                }
            }
        }

        [Theory]
        [InlineData(0)]
        [InlineData(1)]
        [InlineData(2)]
        [InlineData(3)]
        public void AggregatesAndExistenceAreScalarQueries(int kind)
        {
            using var connection = new SqliteConnection();
            var query = connection.Query<QueryPipelineContract.ContractRow>();
            var dialect = Dialect(kind);
            var count = Expression.Call(typeof(Queryable), nameof(Queryable.LongCount), new[] { typeof(QueryPipelineContract.ContractRow) }, query.Expression);
            Assert.Contains(kind == 2 ? "count_big(*)" : "count(*)", Generate(count, connection, dialect).CommandText);
            var any = Expression.Call(typeof(Queryable), nameof(Queryable.Any), new[] { typeof(QueryPipelineContract.ContractRow) }, query.Take(1).Expression);
            var sql = Generate(any, connection, dialect).CommandText;
            Assert.Contains("exists (select", sql);
            Assert.DoesNotContain("count(", sql);
            Assert.Contains("1", sql);
        }

        [Theory]
        [InlineData(0, "\"a\"\"b\"", "a\"b")]
        [InlineData(1, "\"a\"\"b\"", "a\"b")]
        [InlineData(2, "[a]]b]", "a]b")]
        [InlineData(3, "`a``b`", "a`b")]
        public void IdentifierDelimitersAreEscaped(int kind, string expected, string identifier)
        {
            Assert.Equal(expected, Dialect(kind).EscapeIdentifier(identifier));
        }

        [Fact]
        public void RuntimeCollectionExpansionDoesNotChangeTheCachedPlan()
        {
            using var connection = new SqliteConnection();
            string[] names = { null, "one" };
            var query = connection.Query<QueryPipelineContract.ContractRow>().Where(x => names.Contains(x.Name));
            var executor = new KueryLinqExecutor();
            var resultType = typeof(System.Collections.Generic.IEnumerable<QueryPipelineContract.ContractRow>);
            var first = executor.Prepare(new QueryContext(connection), query.Expression, resultType, false, out var firstPlan);
            names = Array.Empty<string>();
            var second = executor.Prepare(new QueryContext(connection), query.Expression, resultType, false, out var secondPlan);
            Assert.Same(firstPlan, secondPlan);
            Assert.Contains(" is null", first.CommandText);
            Assert.Contains("(1 = 0)", second.CommandText);
            Assert.Empty(second.Parameters);
        }
    }

}
