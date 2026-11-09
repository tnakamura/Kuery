using System;
using System.Linq;
using System.Linq.Expressions;
using Kuery.Query.Internal;
using Kuery.Query.SqlExpressions;

namespace Kuery.Query
{
    internal sealed class RelationalSqlTranslatingExpressionVisitor : ExpressionVisitor
    {
        readonly RelationalQueryableMethodTranslatingExpressionVisitor _queryableMethodTranslatingExpressionVisitor;
        readonly SqlExpressionFactory _sqlExpressionFactory = new SqlExpressionFactory();
        internal RelationalSqlTranslatingExpressionVisitor(RelationalQueryableMethodTranslatingExpressionVisitor compiler)
        {
            _queryableMethodTranslatingExpressionVisitor = compiler;
        }

        internal SqlExpression Translate(Expression expression)
        {
            if (Visit(expression) is SqlExpression translated) return translated;
            throw RelationalQueryableMethodTranslatingExpressionVisitor.Unsupported(expression);
        }

        protected override Expression VisitExtension(Expression node)
        {
            if (node is QueryParameterExpression parameter) return new SqlParameterExpression(parameter.Index, parameter.Type);
            if (node is SqlExpression) return node;
            throw RelationalQueryableMethodTranslatingExpressionVisitor.Unsupported(node);
        }

        protected override Expression VisitConstant(ConstantExpression node) => new SqlConstantExpression(node.Value, node.Type);
        protected override Expression VisitConditional(ConditionalExpression node)
            => _sqlExpressionFactory.Case(node.Type, Translate(node.Test), Translate(node.IfTrue), Translate(node.IfFalse));

        protected override Expression VisitBinary(BinaryExpression binary)
        {
            if (binary.NodeType == ExpressionType.Coalesce)
                return _sqlExpressionFactory.Function("Coalesce", binary.Type, Translate(binary.Left), Translate(binary.Right));
            if ((binary.NodeType == ExpressionType.Equal || binary.NodeType == ExpressionType.NotEqual)
                && (binary.Left is EntityProjectionExpression || binary.Right is EntityProjectionExpression))
            {
                var entity = (binary.Left as EntityProjectionExpression) ?? (EntityProjectionExpression)binary.Right;
                var other = ReferenceEquals(entity, binary.Left) ? binary.Right : binary.Left;
                if (other is EntityProjectionExpression) throw RelationalQueryableMethodTranslatingExpressionVisitor.Unsupported(binary);
                return _sqlExpressionFactory.Function(binary.NodeType == ExpressionType.Equal ? "EntityIsNull" : "EntityIsNotNull", typeof(bool), entity.Presence ?? RelationalQueryableMethodTranslatingExpressionVisitor.Integer(1), Translate(other));
            }
            if (binary.Method != null && binary.Method.DeclaringType != typeof(string) && binary.Method.DeclaringType != typeof(decimal)
                && binary.Method.DeclaringType != typeof(DateTime) && binary.Method.DeclaringType != typeof(DateTimeOffset)) throw RelationalQueryableMethodTranslatingExpressionVisitor.Unsupported(binary);
            return binary.NodeType == ExpressionType.Add && binary.Type == typeof(string)
                ? (SqlExpression)_sqlExpressionFactory.Function("String.Concat", binary.Type, Translate(binary.Left), Translate(binary.Right))
                : _sqlExpressionFactory.MakeBinary(binary.NodeType, binary.Type, Translate(binary.Left), Translate(binary.Right));

        }

        protected override Expression VisitUnary(UnaryExpression unary)
        {
            if (unary.NodeType == ExpressionType.Convert || unary.NodeType == ExpressionType.ConvertChecked)
            {
                // CLR-only boxing and enum conversions do not alter the database value.
                if (unary.Type == typeof(object) || unary.Type.IsAssignableFrom(unary.Operand.Type))
                    return _sqlExpressionFactory.MakeUnary(ExpressionType.Convert, unary.Type, Translate(unary.Operand), clrOnly: true);
                var sourceType = Nullable.GetUnderlyingType(unary.Operand.Type) ?? unary.Operand.Type;
                if (sourceType.IsEnum && sourceType.IsDefined(typeof(StoreAsTextAttribute), true))
                    return _sqlExpressionFactory.Function("EnumNumber", unary.Type, Translate(unary.Operand));
                return _sqlExpressionFactory.MakeUnary(ExpressionType.Convert, unary.Type, Translate(unary.Operand));
            }
            if (unary.NodeType == ExpressionType.Not || unary.NodeType == ExpressionType.OnesComplement)
                return _sqlExpressionFactory.MakeUnary(ExpressionType.Not, unary.Type, Translate(unary.Operand));
            if (unary.NodeType == ExpressionType.Negate || unary.NodeType == ExpressionType.NegateChecked)
                return _sqlExpressionFactory.MakeUnary(ExpressionType.Negate, unary.Type, Translate(unary.Operand));

            throw RelationalQueryableMethodTranslatingExpressionVisitor.Unsupported(unary);
        }

        protected override Expression VisitMember(MemberExpression member)
        {
            if (member.Member.DeclaringType == typeof(DateTime) && member.Expression == null
                && (member.Member.Name == "Now" || member.Member.Name == "UtcNow" || member.Member.Name == "Today"))
                return _sqlExpressionFactory.Function("DateTime." + member.Member.Name, member.Type);
            if (Nullable.GetUnderlyingType(member.Expression?.Type ?? typeof(object)) != null)
            {
                if (member.Member.Name == "HasValue") return _sqlExpressionFactory.MakeUnary(ExpressionType.NotEqual, typeof(bool), Translate(member.Expression));
                if (member.Member.Name == "Value") return _sqlExpressionFactory.MakeUnary(ExpressionType.Convert, member.Type, Translate(member.Expression), clrOnly: true);
            }
            if (member.Expression?.Type == typeof(string) && member.Member.Name == "Length")
                return _sqlExpressionFactory.Function("String.Length", typeof(int), Translate(member.Expression));
            if (member.Member.DeclaringType == typeof(DateTime))
            {
                var allowed = new[] { "Year", "Month", "Day", "Hour", "Minute", "Second", "Date", "DayOfWeek" };
                if (allowed.Contains(member.Member.Name)) return _sqlExpressionFactory.Function("DateTime." + member.Member.Name, member.Type, Translate(member.Expression));
            }

            throw RelationalQueryableMethodTranslatingExpressionVisitor.Unsupported(member);
        }

        protected override Expression VisitMethodCall(MethodCallExpression call)
        {
            var name = call.Method.Name;
            var declaringType = call.Method.DeclaringType;
            var source = call.Object ?? call.Arguments.FirstOrDefault();
            if (source is GroupByShaperExpression group)
            {
                if (name == "Count" || name == "LongCount" || name == "Sum" || name == "Min" || name == "Max" || name == "Average" || name == "Any")
                {
                    SqlExpression argument = null;
                    if (call.Arguments.Count == 2)
                    {
                        var body = RelationalQueryableMethodTranslatingExpressionVisitor.Bind(RelationalQueryableMethodTranslatingExpressionVisitor.Lambda(call.Arguments[1]), group.Element);
                        argument = Translate(body);
                        if (name == "Count" || name == "LongCount" || name == "Any")
                            argument = _sqlExpressionFactory.Case(typeof(int?), argument, new SqlConstantExpression(1, typeof(int?)), new SqlConstantExpression(null, typeof(int?)));
                    }
                    else if (name != "Count" && name != "LongCount" && name != "Any") argument = Translate(group.Element);
                    var aggregate = _sqlExpressionFactory.Function(name == "Any" ? "Count" : name, name == "Any" ? typeof(int) : call.Type,
                        argument == null ? Array.Empty<SqlExpression>() : new[] { argument });
                    if (name == "Any") return _sqlExpressionFactory.MakeBinary(ExpressionType.GreaterThan, typeof(bool), aggregate, RelationalQueryableMethodTranslatingExpressionVisitor.Integer(0));
                    if (name == "Sum") return _sqlExpressionFactory.Function("Coalesce", call.Type, aggregate, new SqlConstantExpression(0, call.Type));
                    return aggregate;
                }
            }
            if (name == "Contains" && source != null && source.Type != typeof(string)
                && (declaringType == typeof(Enumerable) || declaringType == typeof(Queryable)
                    || (call.Object != null && declaringType.Namespace == "System.Collections.Generic")))
            {
                if (call.Arguments.Count != (call.Object == null ? 2 : 1)) throw RelationalQueryableMethodTranslatingExpressionVisitor.Unsupported(call);
                var operand = Translate(call.Arguments[call.Object == null ? 1 : 0]);
                if (source is QueryRootExpression || (source is MethodCallExpression queryCall && queryCall.Method.DeclaringType == typeof(Queryable)))
                {
                    var subquery = _queryableMethodTranslatingExpressionVisitor.Translate(source);
                    _queryableMethodTranslatingExpressionVisitor.PushdownIntoSubquery(subquery, false);
                    subquery.QueryExpression.Predicate = RelationalQueryableMethodTranslatingExpressionVisitor.And(subquery.QueryExpression.Predicate, _sqlExpressionFactory.MakeBinary(ExpressionType.Equal, typeof(bool), Translate(subquery.ShaperExpression), operand));
                    subquery.QueryExpression.AddToProjection(RelationalQueryableMethodTranslatingExpressionVisitor.Integer(1));
                    return new ExistsExpression(subquery.QueryExpression);
                }
                return new InExpression(operand, Translate(source));
            }
            if (declaringType == typeof(Queryable) && (name == "Any" || name == "All" || name == "Count" || name == "LongCount" || name == "Sum" || name == "Min" || name == "Max" || name == "Average"))
            {
                var subquery = _queryableMethodTranslatingExpressionVisitor.Translate(call);
                subquery.QueryExpression.AddToProjection(Translate(subquery.ShaperExpression));
                return new ScalarSubqueryExpression(subquery.QueryExpression, call.Type);
            }
            if (declaringType == typeof(KueryFunctions) && name == nameof(KueryFunctions.Like) && call.Arguments.Count == 2)
                return new LikeExpression(Translate(call.Arguments[0]), Translate(call.Arguments[1]));
            if (name == "Equals")
            {
                if (call.Object != null && call.Arguments.Count == 1 && (declaringType == typeof(object) || declaringType == typeof(string)
                    || declaringType.IsPrimitive || declaringType.IsEnum || declaringType == typeof(decimal) || declaringType == typeof(DateTime)
                    || declaringType == typeof(DateTimeOffset) || declaringType == typeof(Guid)
                    || (declaringType.IsGenericType && declaringType.GetGenericTypeDefinition() == typeof(Nullable<>))))
                    return _sqlExpressionFactory.MakeBinary(ExpressionType.Equal, typeof(bool), Translate(call.Object), Translate(call.Arguments[0]));
                if (call.Object == null && call.Arguments.Count == 2 && (declaringType == typeof(object) || declaringType == typeof(string) || declaringType == typeof(Nullable)))
                    return _sqlExpressionFactory.MakeBinary(ExpressionType.Equal, typeof(bool), Translate(call.Arguments[0]), Translate(call.Arguments[1]));
            }
            if (declaringType == typeof(string))
            {
                var args = (call.Object == null ? call.Arguments : new[] { call.Object }.Concat(call.Arguments)).ToArray();
                var count = call.Arguments.Count;
                if ((name == "Contains" || name == "StartsWith" || name == "EndsWith") && call.Object != null
                    && (count == 1 || (count == 2 && call.Arguments[1].Type == typeof(StringComparison))))
                    return _sqlExpressionFactory.Function("String." + name, call.Type, args.Select(Translate).ToArray());
                if (((name == "ToLower" || name == "ToUpper" || name == "Trim" || name == "TrimStart" || name == "TrimEnd") && count == 0)
                    || (name == "Replace" && count == 2 && call.Arguments[0].Type == typeof(string))
                    || (name == "Substring" && (count == 1 || count == 2))
                    || (name == "IndexOf" && count == 1 && call.Arguments[0].Type == typeof(string))
                    || ((name == "IsNullOrEmpty" || name == "IsNullOrWhiteSpace") && count == 1)
                    || (name == "Concat" && count >= 2 && call.Arguments.All(a => a.Type == typeof(string))))
                    return _sqlExpressionFactory.Function("String." + name, call.Type, args.Select(Translate).ToArray());
            }
            if (declaringType == typeof(Math))
            {
                var allowed = new[] { "Abs", "Round", "Floor", "Ceiling", "Max", "Min", "Pow", "Sqrt", "Log", "Log10" };
                if (allowed.Contains(name) && call.Arguments.All(a => a.Type != typeof(MidpointRounding)))
                    return _sqlExpressionFactory.Function("Math." + name, call.Type, call.Arguments.Select(Translate).ToArray());
            }
            if (declaringType == typeof(DateTime) && call.Object != null && call.Arguments.Count == 1)
            {
                var allowed = new[] { "AddDays", "AddMonths", "AddYears", "AddHours", "AddMinutes", "AddSeconds" };
                if (allowed.Contains(name)) return _sqlExpressionFactory.Function("DateTime." + name, call.Type, Translate(call.Object), Translate(call.Arguments[0]));
            }
            if (declaringType == typeof(Convert) && call.Arguments.Count == 1
                && new[] { "ToInt16", "ToInt32", "ToInt64", "ToDouble", "ToSingle", "ToBoolean", "ToString", "ToDecimal", "ToByte" }.Contains(name))
                return _sqlExpressionFactory.MakeUnary(ExpressionType.Convert, call.Type, Translate(call.Arguments[0]));
            if (name == "ToString" && call.Arguments.Count == 0 && call.Object != null
                && (call.Object.Type.IsPrimitive || call.Object.Type == typeof(decimal) || call.Object.Type == typeof(string)))
                return _sqlExpressionFactory.MakeUnary(ExpressionType.Convert, typeof(string), Translate(call.Object));
            if (declaringType?.IsGenericType == true && declaringType.GetGenericTypeDefinition() == typeof(Nullable<>) && name == "GetValueOrDefault")
                return _sqlExpressionFactory.Function("Coalesce", call.Type, Translate(call.Object), call.Arguments.Count == 1 ? Translate(call.Arguments[0]) : new SqlConstantExpression(Activator.CreateInstance(call.Type), call.Type));
            throw RelationalQueryableMethodTranslatingExpressionVisitor.Unsupported(call);
        }


    }
}
