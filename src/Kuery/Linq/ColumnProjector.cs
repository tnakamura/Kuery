using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using Kuery.Linq.Expressions;

namespace Kuery.Linq
{
    internal class ColumnProjector : DbExpressionVisitor
    {
        private Nominator nominator;
        private Dictionary<ColumnExpression, ColumnExpression> map;
        private List<ColumnDeclaration> columns;
        private HashSet<string> columnNames;
        private HashSet<Expression> candidates;
        private string existingAlias;
        private string newAlias;
        private int iColumn;

        internal ColumnProjector(Func<Expression, bool> fnCanBeColumn)
        {
            nominator = new Nominator(fnCanBeColumn);
        }

        internal ProjectedColumns ProjectColumns(
            Expression expression,
            string newAlias,
            string existingAlias)
        {
            map = new Dictionary<ColumnExpression, ColumnExpression>();
            columns = new List<ColumnDeclaration>();
            columnNames = new HashSet<string>();
            this.newAlias = newAlias;
            this.existingAlias = existingAlias;
            candidates = nominator.Nominate(expression);
            return new ProjectedColumns(
                projector: Visit(expression),
                columns: columns.AsReadOnly());
        }

        /// <inheritdoc/>
        public override Expression Visit(Expression node)
        {
            if (candidates.Contains(node))
            {
                if (node.NodeType == (ExpressionType)DbExpressionType.Column)
                {
                    var column = (ColumnExpression)node;
                    if (map.TryGetValue(column, out var mapped))
                    {
                        return mapped;
                    }

                    if (existingAlias == column.Alias)
                    {
                        var ordinal = columns.Count;
                        var columnName = GetUniqueColumnName(column.Name);
                        columns.Add(new ColumnDeclaration(columnName, column));
                        mapped = new ColumnExpression(
                            type: column.Type,
                            alias: newAlias,
                            name: columnName,
                            ordinal: ordinal);
                        map[column] = mapped;
                        columnNames.Add(columnName);
                        return mapped;
                    }

                    return column;
                }
                else
                {
                    var columnName = GetNextColumnName();
                    var ordinal = columns.Count;
                    columns.Add(new ColumnDeclaration(columnName, node));
                    return new ColumnExpression(
                        type: node.Type,
                        alias: newAlias,
                        name: columnName,
                        ordinal: ordinal);
                }
            }
            else
            {
                return base.Visit(node);
            }
        }

        private bool IsColumnNameInUse(string name)
        {
            return columnNames.Contains(name);
        }

        private string GetUniqueColumnName(string name)
        {
            var baseName = name;
            var suffix = 1;

            while (IsColumnNameInUse(name))
            {
                name = baseName + (suffix++);
            }

            return name;
        }

        private string GetNextColumnName()
        {
            return GetUniqueColumnName("c" + (iColumn++));
        }

        class Nominator : DbExpressionVisitor
        {
            private Func<Expression, bool> fnCanBeColumn;
            private bool isBlocked;
            private HashSet<Expression> candidates;

            internal Nominator(Func<Expression, bool> fnCanBeColumn)
            {
                this.fnCanBeColumn = fnCanBeColumn;
            }

            internal HashSet<Expression> Nominate(Expression expression)
            {
                candidates = new HashSet<Expression>();
                isBlocked = false;
                Visit(expression);
                return candidates;
            }

            /// <inheritdoc/>
            public override Expression Visit(Expression node)
            {
                if (node != null)
                {
                    var saveIsBlocked = isBlocked;
                    isBlocked = false;
                    base.Visit(node);

                    if (!isBlocked)
                    {
                        if (fnCanBeColumn(node))
                        {
                            candidates.Add(node);
                        }
                        else
                        {
                            isBlocked = true;
                        }
                    }

                    isBlocked |= saveIsBlocked;
                }

                return node;
            }
        }
    }
}
