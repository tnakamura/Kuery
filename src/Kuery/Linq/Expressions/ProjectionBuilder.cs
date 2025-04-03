using System.Linq.Expressions;
using System.Reflection;

namespace Kuery.Linq.Expressions
{
    internal class ProjectionBuilder : DbExpressionVisitor
    {
        private ParameterExpression row;
        private static MethodInfo miGetValue;

        static ProjectionBuilder()
        {
            miGetValue = typeof(ProjectionRow)
                .GetMethod(
                    name: nameof(ProjectionRow.GetValue),
                    bindingAttr: BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance);
        }

        internal LambdaExpression Build(Expression expression)
        {
            row = Expression.Parameter(typeof(ProjectionRow), nameof(row));
            var body = Visit(expression);
            return Expression.Lambda(body, row);
        }

        /// <inheritdoc/>
        protected override Expression VisitColumn(ColumnExpression column)
        {
            return Expression.Convert(
                expression: Expression.Call(
                    instance: row,
                    method: miGetValue,
                    arguments: Expression.Constant(column.Ordinal)),
                type: column.Type);
        }
    }
}
