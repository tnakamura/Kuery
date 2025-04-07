using System;
using System.Data.Common;
using System.Linq.Expressions;
using System.Reflection;
using Kuery.Linq.Expressions;

namespace Kuery.Linq
{
    internal class DbQueryProvider : QueryProvider
    {
        private readonly DbConnection connection;

        public DbQueryProvider(DbConnection connection)
        {
            this.connection = connection;
        }

        /// <inheritdoc/>
        internal override string GetQueryText(Expression expression)
        {
            return Translate(expression).CommandText;
        }

        /// <inheritdoc/>
        internal override object Execute(Expression expression)
        {
            var result = Translate(expression);
            var projector = result.Projector.Compile();
            var cmd = connection.CreateCommand();
            cmd.CommandText = result.CommandText;
            var reader = cmd.ExecuteReader();
            var elementType = TypeSystem.GetElementType(expression.Type);
            return Activator.CreateInstance(
                type: typeof(ProjectionReader<>).MakeGenericType(elementType),
                bindingAttr: BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic,
                binder: null,
                args: new object[]
                {
                    reader,
                    projector,
                },
                culture: null);
        }

        private TranslateResult Translate(Expression expression)
        {
            expression = Evaluator.PartialEval(expression);
            var projection = (ProjectionExpression)new QueryBinder().Bind(expression);
            var commandText = new QueryFormatter().Format(projection.Source);
            var projector = new ProjectionBuilder().Build(projection.Projector);
            return new TranslateResult
            {
                CommandText = commandText,
                Projector = projector,
            };
        }
    }
}
