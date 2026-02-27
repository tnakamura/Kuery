using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;

namespace Kuery.Linq
{
    internal enum QueryTerminalKind
    {
        Sequence,
        Count,
        First,
        FirstOrDefault,
    }

    internal sealed class QueryOrdering
    {
        internal QueryOrdering(TableMapping.Column column, bool ascending)
        {
            Column = column ?? throw new ArgumentNullException(nameof(column));
            Ascending = ascending;
        }

        internal TableMapping.Column Column { get; }

        internal bool Ascending { get; }
    }

    internal sealed class SelectQueryModel
    {
        internal SelectQueryModel(TableMapping table)
        {
            Table = table ?? throw new ArgumentNullException(nameof(table));
            Orderings = new List<QueryOrdering>();
            Terminal = QueryTerminalKind.Sequence;
        }

        internal TableMapping Table { get; }

        internal Expression Predicate { get; private set; }

        internal List<QueryOrdering> Orderings { get; }

        internal int? Skip { get; set; }

        internal int? Take { get; set; }

        internal QueryTerminalKind Terminal { get; private set; }

        internal LambdaExpression Projection { get; private set; }

        internal IReadOnlyList<ProjectedColumn> ProjectedColumns { get; private set; }

        internal void SetProjection(LambdaExpression projection, IReadOnlyList<ProjectedColumn> columns)
        {
            Projection = projection ?? throw new ArgumentNullException(nameof(projection));
            ProjectedColumns = columns ?? throw new ArgumentNullException(nameof(columns));
        }

        internal void AddPredicate(Expression predicate)
        {
            if (predicate == null)
            {
                return;
            }

            Predicate = Predicate == null ? predicate : Expression.AndAlso(Predicate, predicate);
        }

        internal void AddOrdering(TableMapping.Column column, bool ascending)
        {
            Orderings.Add(new QueryOrdering(column, ascending));
        }

        internal void SetTerminal(QueryTerminalKind terminal)
        {
            Terminal = terminal;
        }
    }

    internal sealed class ProjectedColumn
    {
        internal ProjectedColumn(TableMapping.Column sourceColumn, string targetMemberName)
        {
            SourceColumn = sourceColumn ?? throw new ArgumentNullException(nameof(sourceColumn));
            TargetMemberName = targetMemberName ?? throw new ArgumentNullException(nameof(targetMemberName));
        }

        internal TableMapping.Column SourceColumn { get; }

        internal string TargetMemberName { get; }
    }
}
