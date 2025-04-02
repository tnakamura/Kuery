using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Linq.Expressions;
using System.Text;

namespace Kuery.Linq.Expressions
{
    internal class DbExpressionVisitor : ExpressionVisitor
    {
        /// <inheritdoc/>
        public override Expression Visit(Expression node)
        {
            if (node == null)
            {
                return null;
            }

            switch ((DbExpressionType)node.NodeType)
            {
                case DbExpressionType.Table:
                    return VisitTable((TableExpression)node);
                case DbExpressionType.Column:
                    return VisitColumn((ColumnExpression)node);
                case DbExpressionType.Select:
                case DbExpressionType.Projection:
                default:
                    return base.Visit(node);
            }
        }

        protected virtual Expression VisitTable(TableExpression table)
        {
            return table;
        }

        protected virtual Expression VisitColumn(ColumnExpression column)
        {
            return column;
        }

        protected virtual Expression VisitSelect(SelectExpression select)
        {
            var from = VisitSource(select.From);
            var where = Visit(select.Where);
            var columns = VisitColumnDeclarations(select.Columns);

            if (from != select.From ||
                where != select.Where ||
                columns != select.Columns)
            {
                return new SelectExpression(
                    type: select.Type,
                    alias: select.Alias,
                    columns: columns,
                    from: from,
                    where: where);
            }

            return select;
        }

        protected virtual Expression VisitSource(Expression source)
        {
            return Visit(source);
        }

        protected virtual Expression VisitProjection(ProjectionExpression projection)
        {
            var source = (SelectExpression)Visit(projection.Source);
            var projector = Visit(projection.Projector);

            if (source != projection.Source || projector != projection.Projector)
            {
                return new ProjectionExpression(source, projector);
            }

            return projection;
        }

        protected ReadOnlyCollection<ColumnDeclaration> VisitColumnDeclarations(ReadOnlyCollection<ColumnDeclaration> columns)
        {
            List<ColumnDeclaration> alternate = null;

            for (var i = 0; i < columns.Count; i++)
            {
                var column = columns[i];
                var e = Visit(column.Expression);

                if (alternate == null && e != column.Expression)
                {
                    alternate = columns.Take(i).ToList();
                }

                if (alternate != null)
                {
                    alternate.Add(new ColumnDeclaration(column.Name, e));
                }
            }

            if (alternate != null)
            {
                return alternate.AsReadOnly();
            }

            return columns;
        }
    }
}
