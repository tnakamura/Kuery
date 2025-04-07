using System;
using System.Collections.Generic;
using System.Text;
using System.Linq.Expressions;
using Kuery.Linq.Expressions;

namespace Kuery.Linq
{
    internal class QueryFormatter : DbExpressionVisitor
    {
        private StringBuilder sb;
        private int indent = 2;
        private int depth;

        internal QueryFormatter()
        {
        }


        internal string Format(Expression expression)
        {
            sb = new StringBuilder();
            Visit(expression);
            return sb.ToString();
        }

        protected enum Indentation
        {
            Same,
            Inner,
            Outer,
        }

        internal int IndentationWidth { get; set; }

        private void AppendNewLine(Indentation style)
        {
            sb.AppendLine();

            if (style == Indentation.Inner)
            {
                depth++;
            }
            else if (style == Indentation.Outer)
            {
                depth--;
            }

            for (int i = 0, n = depth * indent; i < n; i++)
            {
                sb.Append(" ");
            }
        }

        /// <inheritdoc/>
        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
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
            if (node.Value == null)
            {
                sb.Append("NULL");
            }
            else
            {
                switch (Type.GetTypeCode(node.Value.GetType()))
                {
                    case TypeCode.Boolean:
                        sb.Append(((bool)node.Value) ? 1 : 0);
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
        protected override Expression VisitColumn(ColumnExpression column)
        {
            if (!string.IsNullOrEmpty(column.Alias))
            {
                sb.Append(column.Alias);
                sb.Append(".");
            }
            sb.Append(column.Name);
            return column;
        }

        /// <inheritdoc/>
        protected override Expression VisitSelect(SelectExpression select)
        {
            sb.Append("SELECT ");

            for (var i = 0; i < select.Columns.Count; i++)
            {
                var column = select.Columns[i];

                if (i > 0)
                {
                    sb.Append(", ");
                }

                var c = Visit(column.Expression) as ColumnExpression;

                if (c == null || c.Name != select.Columns[i].Name)
                {
                    sb.Append(" AS ");
                    sb.Append(column.Name);
                }
            }

            if (select.From != null)
            {
                AppendNewLine(Indentation.Same);
                sb.Append("FROM ");
                VisitSource(select.From);
            }

            if (select.Where != null)
            {
                AppendNewLine(Indentation.Same);
                sb.Append("WHERE ");
                Visit(select.Where);
            }

            return select;
        }

        /// <inheritdoc/>
        protected override Expression VisitSource(Expression source)
        {
            switch ((DbExpressionType)source.NodeType)
            {
                case DbExpressionType.Table:
                    var table = (TableExpression)source;
                    sb.Append(table.Name);
                    sb.Append(" AS ");
                    sb.Append(table.Alias);
                    break;
                case DbExpressionType.Select:
                    var select = (SelectExpression)source;
                    sb.Append("(");
                    AppendNewLine(Indentation.Inner);
                    Visit(select);
                    AppendNewLine(Indentation.Outer);
                    sb.Append(")");
                    sb.Append(" AS ");
                    sb.Append(select.Alias);
                    break;
                default:
                    throw new NotSupportedException(
                        $"Select source is not valid type");
            }
            return source;
        }
    }
}
