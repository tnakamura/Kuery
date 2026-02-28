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

    internal sealed class JoinKeyPair
    {
        internal JoinKeyPair(TableMapping outerKeyTable, TableMapping.Column outerKeyColumn, TableMapping.Column innerKeyColumn)
        {
            OuterKeyTable = outerKeyTable ?? throw new ArgumentNullException(nameof(outerKeyTable));
            OuterKeyColumn = outerKeyColumn ?? throw new ArgumentNullException(nameof(outerKeyColumn));
            InnerKeyColumn = innerKeyColumn ?? throw new ArgumentNullException(nameof(innerKeyColumn));
        }

        internal TableMapping OuterKeyTable { get; }

        internal TableMapping.Column OuterKeyColumn { get; }

        internal TableMapping.Column InnerKeyColumn { get; }
    }

    internal sealed class JoinClause
    {
        internal JoinClause(
            TableMapping innerTable,
            IReadOnlyList<JoinKeyPair> keyPairs,
            LambdaExpression resultSelector,
            bool isLeftJoin = false)
        {
            InnerTable = innerTable ?? throw new ArgumentNullException(nameof(innerTable));
            KeyPairs = keyPairs ?? throw new ArgumentNullException(nameof(keyPairs));
            ResultSelector = resultSelector ?? throw new ArgumentNullException(nameof(resultSelector));
            IsLeftJoin = isLeftJoin;
        }

        internal TableMapping InnerTable { get; }

        internal IReadOnlyList<JoinKeyPair> KeyPairs { get; }

        internal TableMapping.Column OuterKeyColumn => KeyPairs[0].OuterKeyColumn;

        internal TableMapping.Column InnerKeyColumn => KeyPairs[0].InnerKeyColumn;

        internal LambdaExpression ResultSelector { get; }

        internal bool IsLeftJoin { get; }
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

        internal List<JoinClause> Joins { get; private set; }

        internal JoinClause Join => Joins?.Count > 0 ? Joins[0] : null;

        internal void AddJoin(JoinClause join)
        {
            if (Joins == null) Joins = new List<JoinClause>();
            Joins.Add(join ?? throw new ArgumentNullException(nameof(join)));
        }

        internal Dictionary<string, TableMapping> JoinShape { get; private set; }

        internal void SetJoinShapeMember(string memberName, TableMapping table)
        {
            if (JoinShape == null) JoinShape = new Dictionary<string, TableMapping>();
            JoinShape[memberName] = table ?? throw new ArgumentNullException(nameof(table));
        }

        internal LambdaExpression JoinResultSelector { get; private set; }

        internal void SetJoinResultSelector(LambdaExpression selector)
        {
            JoinResultSelector = selector;
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
