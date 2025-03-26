using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace Kuery.Linq
{
    internal class ColumnProjection
    {
        internal string Columns;
        internal Expression Selector;
    }

    internal class ColumnProjector : ExpressionVisitor
    {
        private StringBuilder sb;
        private int iColumn;
        private ParameterExpression row;
        private static MethodInfo miGetValue;

        static ColumnProjector()
        {
            miGetValue = typeof(ProjectionRow).GetMethod(
                name: nameof(ProjectionRow.GetValue),
                bindingAttr: BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
        }

        internal ColumnProjector() { }

        internal ColumnProjection ProjectColumns(
            Expression expression,
            ParameterExpression row)
        {
            sb = new StringBuilder();
            this.row = row;
            var selector = Visit(expression);
            return new ColumnProjection
            {
                Columns = sb.ToString(),
                Selector = selector,
            };
        }

        /// <inheritdoc/>
        protected override Expression VisitMember(MemberExpression node)
        {
            if (node.Expression != null &&
                node.Expression.NodeType == ExpressionType.Parameter)
            {
                if (sb.Length > 0)
                {
                    sb.Append(", ");
                }
                sb.Append(node.Member.Name);
                return Expression.Convert(
                    expression: Expression.Call(
                        instance: row,
                        method: miGetValue,
                        arguments: Expression.Constant(iColumn++)),
                    node.Type);
            }
            else
            {
                return base.VisitMember(node);
            }
        }
    }
}
