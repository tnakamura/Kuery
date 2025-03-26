using System;
using System.Linq;
using System.Linq.Expressions;
using System.Text;

namespace Kuery.Linq
{
    internal class TranslateResult
    {
        internal string CommandText;
        internal LambdaExpression Projector;
    }

    internal class QueryTranslator : ExpressionVisitor
    {
        private StringBuilder sb;
        private ParameterExpression row;
        ColumnProjection projection;

        internal QueryTranslator()
        {
        }

        internal TranslateResult Translate(Expression expression)
        {
            sb = new StringBuilder();
            row = Expression.Parameter(typeof(ProjectionRow), nameof(row));
            Visit(expression);
            return new TranslateResult
            {
                CommandText = sb.ToString(),
                Projector = projection != null
                    ? Expression.Lambda(projection.Selector, row)
                    : null,
            };
        }

        private static Expression StripQuotes(Expression e)
        {
            while (e.NodeType == ExpressionType.Quote)
            {
                e = ((UnaryExpression)e).Operand;
            }
            return e;
        }

        /// <inheritdoc/>
        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            if (node.Method.DeclaringType == typeof(Queryable))
            {
                if (node.Method.Name == nameof(Queryable.Where))
                {
                    sb.Append("SELECT * FROM (");
                    Visit(node.Arguments[0]);
                    sb.Append(") AS T WHERE ");
                    var lambda = (LambdaExpression)StripQuotes(node.Arguments[1]);
                    Visit(lambda.Body);
                    return node;
                }
                else if (node.Method.Name == nameof(Queryable.Select))
                {
                    var lambda = (LambdaExpression)StripQuotes(node.Arguments[1]);
                    var projection = new ColumnProjector()
                        .ProjectColumns(lambda.Body, row);
                    sb.Append("SELECT ");
                    sb.Append(projection.Columns);
                    sb.Append(" FROM (");
                    Visit(node.Arguments[0]);
                    sb.Append(") AS T");
                    this.projection = projection;
                    return node;
                }
            }
            throw new NotSupportedException(
                $"The method '{node.Method.Name}' is not supported");
        }

        /// <inheritdoc/>
        protected override Expression VisitUnary(UnaryExpression node)
        {
            switch (node.NodeType)
            {
                case ExpressionType.Not:
                    sb.Append(" NOT ");
                    Visit(node.Operand);
                    break;
                default:
                    throw new NotSupportedException(
                        $"The unary operator '{node.NodeType}' is not supported");
            }
            return node;
        }

        /// <inheritdoc/>
        protected override Expression VisitBinary(BinaryExpression node)
        {
            sb.Append("(");

            Visit(node.Left);

            switch (node.NodeType)
            {
                case ExpressionType.And:
                    sb.Append(" AND ");
                    break;
                case ExpressionType.Or:
                    sb.Append(" OR ");
                    break;
                case ExpressionType.Equal:
                    sb.Append(" = ");
                    break;
                case ExpressionType.NotEqual:
                    sb.Append(" <> ");
                    break;
                case ExpressionType.LessThan:
                    sb.Append(" < ");
                    break;
                case ExpressionType.LessThanOrEqual:
                    sb.Append(" <= ");
                    break;
                case ExpressionType.GreaterThan:
                    sb.Append(" > ");
                    break;
                case ExpressionType.GreaterThanOrEqual:
                    sb.Append(" >= ");
                    break;
                default:
                    throw new NotSupportedException(
                        $"The binary operator '{node.NodeType}' is not supported");
            }

            Visit(node.Right);

            sb.Append(")");

            return node;
        }

        /// <inheritdoc/>
        protected override Expression VisitConstant(ConstantExpression node)
        {
            var q = node.Value as IQueryable;

            if (q != null)
            {
                // assume constant nodes w/ IQueryables are table references
                sb.Append("SELECT * FROM ");
                sb.Append(q.ElementType.Name);
            }
            else if (node.Value == null)
            {
                sb.Append("NULL");
            }
            else
            {
                switch (Type.GetTypeCode(node.Value.GetType()))
                {
                    case TypeCode.Boolean:
                        sb.Append((bool)node.Value ? 1 : 0);
                        break;
                    case TypeCode.String:
                        sb.Append("'");
                        sb.Append(node.Value);
                        sb.Append("'");
                        break;
                    case TypeCode.Object:
                        throw new NotSupportedException(
                            $"The constant for '{node.Value}' is not supported");
                    default:
                        sb.Append(node.Value);
                        break;
                }
            }

            return node;
        }

        /// <inheritdoc/>
        protected override Expression VisitMember(MemberExpression node)
        {
            if (node.Expression != null &&
                node.Expression.NodeType == ExpressionType.Parameter)
            {
                sb.Append(node.Member.Name);
                return node;
            }
            throw new NotSupportedException(
                $"The member '{node.Member.Name}' is not supported");
        }
    }
}
