using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace Kuery.Linq.Expressions
{
    internal class QueryBinder : ExpressionVisitor
    {
        private ColumnProjector columnProjector;

        private Dictionary<ParameterExpression, Expression> map;

        private int aliasCount;

        internal QueryBinder()
        {
            columnProjector = new ColumnProjector(CanBeColumn);
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

        private ProjectedColumns ProjectColumns(
            Expression expression,
            string newAlias,
            string existingAlias)
        {
            return columnProjector.ProjectColumns(
                expression: expression,
                newAlias: newAlias,
                existingAlias: existingAlias);
        }

        /// <inheritdoc/>
        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            if (node.Method.DeclaringType == typeof(Queryable) ||
                node.Method.DeclaringType == typeof(Enumerable))
            {
                switch (node.Method.Name)
                {
                    case nameof(Queryable.Where):
                        return BindWhere(
                            resultType: node.Type,
                            source: node.Arguments[0],
                            predicate: (LambdaExpression)StripQuotes(node.Arguments[1]));
                    case nameof(Queryable.Select):
                        return BindSelect(
                            resultType: node.Type,
                            source: node.Arguments[0],
                            selector: (LambdaExpression)StripQuotes(node.Arguments[1]));
                    default:
                        throw new NotSupportedException(
                            $"The method '{node.Method.Name}' is not supported");
                }
            }
            return base.VisitMethodCall(node);
        }

        private Expression BindWhere(
            Type resultType,
            Expression source,
            LambdaExpression predicate)
        {
            var projection = (ProjectionExpression)Visit(source);
            map[predicate.Parameters[0]] = projection.Projector;
            var where = Visit(predicate.Body);
            var alias = GetNextAlias();
            var pc = ProjectColumns(
                expression: projection.Projector,
                newAlias: alias,
                existingAlias: GetExistingAlias(projection.Source));
            return new ProjectionExpression(
                source: new SelectExpression(
                    type: resultType,
                    alias: alias,
                    columns: pc.Columns,
                    from: projection.Source,
                    where: where),
                projector: pc.Projector);
        }

        private Expression BindSelect(
            Type resultType,
            Expression source,
            LambdaExpression selector)
        {
            var projection = (ProjectionExpression)Visit(source);
            map[selector.Parameters[0]] = projection.Projector;
            var expression = Visit(selector.Body);
            var alias = GetNextAlias();
            var pc = ProjectColumns(
                expression: expression,
                newAlias: alias,
                existingAlias: GetExistingAlias(projection.Source));
            return new ProjectionExpression(
                source: new SelectExpression(
                    type: resultType,
                    alias: alias,
                    columns: pc.Columns,
                    from: projection.Source,
                    where: null),
                projector: pc.Projector);
        }

        private static string GetExistingAlias(Expression source)
        {
            switch ((DbExpressionType)source.NodeType)
            {
                case DbExpressionType.Select:
                    return ((SelectExpression)source).Alias;
                case DbExpressionType.Table:
                    return ((TableExpression)source).Alias;
                default:
                    throw new InvalidOperationException(
                        $"Invalid source node type '{source.NodeType}'");
            }
        }

        private bool IsTable(object value)
        {
            var q = value as IQueryable;
            return q != null &&
                q.Expression.NodeType == ExpressionType.Constant;
        }

        private string GetTableName(object table)
        {
            var tableQuery = (IQueryable)table;
            var rowType = tableQuery.ElementType;
            return rowType.Name;
        }

        private string GetColumnName(MemberInfo member)
        {
            return member.Name;
        }

        private Type GetColumnType(MemberInfo member)
        {
            if (member is FieldInfo fi)
            {
                return fi.FieldType;
            }
            var pi = (PropertyInfo)member;
            return pi.PropertyType;
        }

        private IEnumerable<MemberInfo> GetMappedMembers(Type rowType)
        {
            return rowType
                .GetFields(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)
                .Cast<MemberInfo>();
        }

        private ProjectionExpression GetTableProjection(object value)
        {
            var table = (IQueryable)value;
            var tableAlias = GetNextAlias();
            var selectAlias = GetNextAlias();
            var bindings = new List<MemberBinding>();
            var columns = new List<ColumnDeclaration>();

            foreach (var mi in GetMappedMembers(table.ElementType))
            {
                var columnName = GetColumnName(mi);
                var columnType = GetColumnType(mi);
                var ordinal = columns.Count;

                bindings.Add(
                    Expression.Bind(
                        member: mi,
                        expression: new ColumnExpression(
                            type: columnType,
                            alias: selectAlias,
                            name: columnName,
                            ordinal: ordinal)));

                columns.Add(
                    new ColumnDeclaration(
                        name: columnName,
                        expression: new ColumnExpression(
                            type: columnType,
                            alias: tableAlias,
                            name: columnName,
                            ordinal: ordinal)));
            }

            var projector = Expression.MemberInit(
                newExpression: Expression.New(table.ElementType),
                bindings: bindings);

            var resultType = typeof(IEnumerable<>).MakeGenericType(table.ElementType);

            return new ProjectionExpression(
                source: new SelectExpression(
                    type: resultType,
                    alias: selectAlias,
                    columns: columns,
                    from: new TableExpression(
                        type: resultType,
                        alias: tableAlias,
                        name: GetTableName(table)),
                    where: null),
                projector: projector);
        }

        /// <inheritdoc/>
        protected override Expression VisitConstant(ConstantExpression node)
        {
            if (IsTable(node.Value))
            {
                return GetTableProjection(node.Value);
            }
            return node;
        }

        /// <inheritdoc/>
        protected override Expression VisitParameter(ParameterExpression node)
        {
            if (map.TryGetValue(node, out var e))
            {
                return e;
            }
            return node;
        }

        /// <inheritdoc/>
        protected override Expression VisitMember(MemberExpression node)
        {
            var source = Visit(node.Expression);

            switch (source.NodeType)
            {
                case ExpressionType.MemberInit:
                    var min = (MemberInitExpression)source;
                    for (var i = 0; i < min.Bindings.Count; i++)
                    {
                        var assign = min.Bindings[i] as MemberAssignment;
                        if (assign != null &&
                            MembersMatch(assign.Member, node.Member))
                        {
                            return assign.Expression;
                        }
                    }
                    break;

                case ExpressionType.New:
                    var nex = (NewExpression)source;
                    if (nex.Members != null)
                    {
                        for (var i = 0; i < nex.Members.Count; i++)
                        {
                            if (MembersMatch(nex.Members[i], node.Member))
                            {
                                return nex.Arguments[i];
                            }
                        }
                    }
                    break;
            }

            if (source == node.Expression)
            {
                return node;
            }

            return MakeMemberAccess(source, node.Member);
        }

        private bool MembersMatch(MemberInfo a, MemberInfo b)
        {
            if (a == b)
            {
                return true;
            }

            if (a is MethodInfo && b is PropertyInfo)
            {
                return a == ((PropertyInfo)b).GetGetMethod();
            }
            else if (a is PropertyInfo && b is MethodInfo)
            {
                return ((PropertyInfo)a).GetGetMethod() == b;
            }

            return false;
        }

        private Expression MakeMemberAccess(Expression source, MemberInfo mi)
        {
            if (mi is FieldInfo fi)
            {
                return Expression.Field(source, fi);
            }
            var pi = (PropertyInfo)mi;
            return Expression.Property(source, pi);
        }
    }
}
