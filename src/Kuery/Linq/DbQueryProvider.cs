using System;
using System.Data.Common;
using System.Linq.Expressions;
using System.Reflection;

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
            var cmd = connection.CreateCommand();
            cmd.CommandText = result.CommandText;
            var reader = cmd.ExecuteReader();
            var elementType = TypeSystem.GetElementType(expression.Type);

            if (result.Projector != null)
            {
                var projector = result.Projector.Compile();
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
            else
            {
                return Activator.CreateInstance(
                    type: typeof(ObjectReader<>).MakeGenericType(elementType),
                    bindingAttr: BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic,
                    binder: null,
                    args: new object[]
                    {
                        reader,
                    },
                    culture: null);
            }
        }

        private TranslateResult Translate(Expression expression)
        {
            expression = Evaluator.PartialEval(expression);
            return new QueryTranslator().Translate(expression);
        }
    }
}
