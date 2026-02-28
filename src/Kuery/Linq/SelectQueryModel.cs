using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;

namespace Kuery.Linq
{
    internal sealed class GroupBySelectItem
    {
        internal GroupBySelectItem(TableMapping.Column keyColumn, string targetMemberName)
        {
            IsKey = true;
            SourceColumn = keyColumn ?? throw new ArgumentNullException(nameof(keyColumn));
            TargetMemberName = targetMemberName ?? throw new ArgumentNullException(nameof(targetMemberName));
        }

        internal GroupBySelectItem(string aggregateFunction, TableMapping.Column sourceColumn, string targetMemberName)
        {
            IsKey = false;
            AggregateFunction = aggregateFunction ?? throw new ArgumentNullException(nameof(aggregateFunction));
            SourceColumn = sourceColumn;
            TargetMemberName = targetMemberName ?? throw new ArgumentNullException(nameof(targetMemberName));
        }

        internal bool IsKey { get; }

        internal string AggregateFunction { get; }

        internal TableMapping.Column SourceColumn { get; }

        internal string TargetMemberName { get; }
    }

    internal enum QueryTerminalKind
    {
        Sequence,
        Count,
        LongCount,
        Any,
        All,
        First,
        FirstOrDefault,
        Last,
        LastOrDefault,
        Single,
        SingleOrDefault,
        ElementAt,
        ElementAtOrDefault,
        Sum,
        Min,
        Max,
        Average,
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

    internal sealed class JoinClause
    {
        internal JoinClause(
            TableMapping innerTable,
            TableMapping.Column outerKeyColumn,
            TableMapping.Column innerKeyColumn,
            LambdaExpression resultSelector)
        {
            InnerTable = innerTable ?? throw new ArgumentNullException(nameof(innerTable));
            OuterKeyColumn = outerKeyColumn ?? throw new ArgumentNullException(nameof(outerKeyColumn));
            InnerKeyColumn = innerKeyColumn ?? throw new ArgumentNullException(nameof(innerKeyColumn));
            ResultSelector = resultSelector ?? throw new ArgumentNullException(nameof(resultSelector));
        }

        internal TableMapping InnerTable { get; }

        internal TableMapping.Column OuterKeyColumn { get; }

        internal TableMapping.Column InnerKeyColumn { get; }

        internal LambdaExpression ResultSelector { get; }
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

        internal JoinClause Join { get; private set; }

        internal void SetJoin(JoinClause join)
        {
            Join = join ?? throw new ArgumentNullException(nameof(join));
        }

        internal Expression Predicate { get; private set; }

        internal List<QueryOrdering> Orderings { get; }

        internal int? Skip { get; set; }

        internal int? Take { get; set; }

        internal bool IsDistinct { get; set; }

        internal QueryTerminalKind Terminal { get; private set; }

        internal LambdaExpression AggregateSelector { get; private set; }

        internal Expression AllPredicate { get; private set; }

        internal void SetAllPredicate(Expression predicate)
        {
            AllPredicate = predicate ?? throw new ArgumentNullException(nameof(predicate));
        }

        internal void SetAggregateSelector(LambdaExpression selector)
        {
            AggregateSelector = selector;
        }

        internal LambdaExpression Projection { get; private set; }

        internal IReadOnlyList<ProjectedColumn> ProjectedColumns { get; private set; }

        internal void SetProjection(LambdaExpression projection, IReadOnlyList<ProjectedColumn> columns)
        {
            Projection = projection ?? throw new ArgumentNullException(nameof(projection));
            ProjectedColumns = columns ?? throw new ArgumentNullException(nameof(columns));
        }

        internal List<TableMapping.Column> GroupByColumns { get; private set; }

        internal IReadOnlyList<GroupBySelectItem> GroupBySelectItems { get; private set; }

        internal ConstructorInfo GroupByResultConstructor { get; private set; }

        internal System.Collections.ObjectModel.ReadOnlyCollection<System.Reflection.MemberInfo> GroupByKeyMembers { get; set; }

        internal Expression HavingPredicate { get; private set; }

        internal ParameterExpression HavingGroupParameter { get; private set; }

        internal void AddHavingPredicate(Expression predicate, ParameterExpression groupParameter)
        {
            HavingGroupParameter = groupParameter;
            HavingPredicate = HavingPredicate == null ? predicate : Expression.AndAlso(HavingPredicate, predicate);
        }

        internal void AddGroupByColumn(TableMapping.Column column)
        {
            if (GroupByColumns == null)
            {
                GroupByColumns = new List<TableMapping.Column>();
            }
            GroupByColumns.Add(column ?? throw new ArgumentNullException(nameof(column)));
        }

        internal void SetGroupBySelect(IReadOnlyList<GroupBySelectItem> items, ConstructorInfo constructor)
        {
            GroupBySelectItems = items ?? throw new ArgumentNullException(nameof(items));
            GroupByResultConstructor = constructor ?? throw new ArgumentNullException(nameof(constructor));
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
