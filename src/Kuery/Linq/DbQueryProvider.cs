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
            return Translate(expression);
        }

        /// <inheritdoc/>
        internal override object Execute(Expression expression)
        {
            var cmd = connection.CreateCommand();
            cmd.CommandText = Translate(expression);
            var reader = cmd.ExecuteReader();
            var elementType = TypeSystem.GetElementType(expression.Type);

            return Activator.CreateInstance(
                type: typeof(ObjectReader<>).MakeGenericType(elementType),
                bindingAttr: BindingFlags.Instance | BindingFlags.NonPublic,
                binder: null,
                args: new object[]
                {
                    reader,
                },
                culture: null);
        }

        private string Translate(Expression expression)
        {
            return new QueryTranslator().Translate(expression);
        }
    }
}
