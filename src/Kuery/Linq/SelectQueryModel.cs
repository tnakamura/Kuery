using System;
using System.Collections.Generic;
using System.Linq.Expressions;

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
}
