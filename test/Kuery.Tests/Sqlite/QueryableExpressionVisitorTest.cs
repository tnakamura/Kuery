using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Kuery.Query;
using Kuery.Query.Internal;
using Kuery.Query.SqlExpressions;
using Microsoft.Data.Sqlite;
using Xunit;

namespace Kuery.Tests.Sqlite
{
    public class QueryableExpressionVisitorTest : IDisposable
    {
        readonly SqliteConnection _connection = new SqliteConnection("Data Source=:memory:");

        public QueryableExpressionVisitorTest()
        {
            _connection.Open();
            _connection.Execute("create table visitor_rows (Id integer primary key, ParentId integer null)");
            for (int id = 1; id <= 4; id++)
                _connection.Insert(new VisitorRow { Id = id, ParentId = id == 1 ? null : (int?)(id / 2) });
        }

        public void Dispose() => _connection.Dispose();

        [Theory]
        [InlineData(0)]
        [InlineData(1)]
        [InlineData(2)]
        public void SqlVisitorRewritesNestedQueriesWithoutChangingTheCachedTree(int kind)
        {
            int minimum = 2;
            var rows = _connection.Query<VisitorRow>();
            IQueryable<int> query;
            if (kind == 0)
                query = rows.Where(parent => rows.Any(child => child.ParentId == parent.Id && child.Id > minimum)).Select(x => x.Id);
            else if (kind == 1)
                query = rows.Where(x => x.Id >= minimum).Join(rows, child => child.ParentId, parent => parent.Id, (child, parent) => child.Id);
            else
                query = rows.Where(x => x.Id >= minimum).Select(x => x.Id).Intersect(rows.Select(x => x.Id));
            query = query.OrderBy(x => x);

            var funcletizer = new ExpressionTreeFuncletizer(_connection);
            funcletizer.ExtractParameters(query.Expression);
            var originalSql = new QueryCompiler().CompileQuery(new QueryContext(_connection), query.Expression,
                typeof(IEnumerable<int>), false, out var original);
            var expectedOriginal = kind == 0 ? new[] { 1, 2 } : new[] { 2, 3, 4 };
            Assert.Equal(expectedOriginal, Execute(originalSql));

            var rewritten = new CompiledQuery
            {
                ShapedQuery = new ShapedQueryExpression
                {
                    QueryExpression = (SelectExpression)new ReplaceParameterVisitor().Visit(original.Query),
                    ShaperExpression = original.ShapedQuery.ShaperExpression,
                },
            };
            var rewrittenSql = new QuerySqlGenerator(new SqliteDialect(), funcletizer.Values).GetCommand(rewritten.Query);
            Assert.Equal(kind == 0 ? new[] { 2 } : new[] { 3, 4 }, Execute(rewrittenSql));
            Assert.Equal(originalSql.CommandText, new QuerySqlGenerator(new SqliteDialect(), funcletizer.Values).GetCommand(original.Query).CommandText);
            Assert.Equal(expectedOriginal, Execute(originalSql));
        }

        [Fact]
        public void PreprocessorInlinesInvokedLambdasBeforeSqlTranslation()
        {
            Expression<Func<VisitorRow, bool>> predicate = x => x.Id % 2 == 0;
            var parameter = Expression.Parameter(typeof(VisitorRow), "row");
            var invoked = Expression.Lambda<Func<VisitorRow, bool>>(Expression.Invoke(predicate, parameter), parameter);
            var result = _connection.Query<VisitorRow>().Where(invoked).OrderBy(x => x.Id).Select(x => x.Id).ToList();
            Assert.Equal(new[] { 2, 4 }, result);

            Func<VisitorRow, bool> clientPredicate = x => x.Id > 2;
            var clientInvocation = Expression.Lambda<Func<VisitorRow, bool>>(
                Expression.Invoke(Expression.Constant(clientPredicate), parameter), parameter);
            Assert.Throws<NotSupportedException>(() => _connection.Query<VisitorRow>().Where(clientInvocation).ToList());
        }

        int[] Execute(RelationalCommand generated)
        {
            using var command = _connection.CreateCommand();
            command.CommandText = generated.CommandText;
            foreach (var parameter in generated.Parameters)
                command.Parameters.AddWithValue(parameter.Name, parameter.Value ?? DBNull.Value);
            var result = new List<int>();
            using var reader = command.ExecuteReader();
            while (reader.Read()) result.Add(reader.GetInt32(0));
            return result.ToArray();
        }

        private sealed class ReplaceParameterVisitor : SqlExpressionVisitor
        {
            protected override Expression VisitSqlParameter(SqlParameterExpression parameter)
                => parameter.Index == 0 ? (Expression)new SqlConstantExpression(3, typeof(int)) : parameter;
        }

        [Table("visitor_rows")]
        public class VisitorRow
        {
            [PrimaryKey]
            public int Id { get; set; }
            public int? ParentId { get; set; }
        }
    }
}
