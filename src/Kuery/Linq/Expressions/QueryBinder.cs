using System.Collections.Generic;
using System.Linq.Expressions;

namespace Kuery.Linq.Expressions
{
    internal class QueryBinder : ExpressionVisitor
    {
        private ColumnProjector columnProjector;

        private Dictionary<ParameterExpression, Expression> map;

        private int aliasCount;

        internal QueryBinder()
        {
            columnProjector = new ColumnProjector(/*CanBeColumn*/);
        }

        private bool CanBeColumn(Expression expression)
        {
            return expression.NodeType == (ExpressionType)DbExpressionType.Column;
        }

        internal Expression Bind(Expression expression)
        {
            map = new Dictionary<ParameterExpression, Expression>();
            return Visit(expression);
        }

        private static Expression StripQuotes(Expression e)
        {
            while (e.NodeType == ExpressionType.Quote)
            {
                e = ((UnaryExpression)e).Operand;
            }
            return e;
        }

        private string GetNextAlias()
        {
            return $"t{aliasCount++}";
        }
    }
}
