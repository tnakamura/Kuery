using System;
using System.Collections;
using System.Linq;
using System.Linq.Expressions;

namespace Kuery.Linq
{
    internal sealed class SqlScalarTranslator
    {
        readonly RelationalQueryCompiler _compiler;
        internal SqlScalarTranslator(RelationalQueryCompiler compiler) { _compiler = compiler; }

        internal SqlExpression Translate(Expression expression)
        {
            if (expression is SqlExpression sql) return sql;
            if (expression is ConstantExpression constant) return new SqlConstantExpression(constant.Value, constant.Type);
            if (expression is BinaryExpression binary)
            {
                if ((binary.NodeType == ExpressionType.Equal || binary.NodeType == ExpressionType.NotEqual)
                    && (binary.Left is EntityProjectionExpression || binary.Right is EntityProjectionExpression))
                {
                    var entity = (binary.Left as EntityProjectionExpression) ?? (EntityProjectionExpression)binary.Right;
                    var other = ReferenceEquals(entity, binary.Left) ? binary.Right : binary.Left;
                    if (other is EntityProjectionExpression) throw RelationalQueryCompiler.Unsupported(binary);
                    return Operation(binary.NodeType == ExpressionType.Equal ? "EntityIsNull" : "EntityIsNotNull", typeof(bool),
                        entity.Presence ?? RelationalQueryCompiler.Integer(1), Translate(other));
                }
                if (binary.Method != null && binary.Method.DeclaringType != typeof(string) && binary.Method.DeclaringType != typeof(decimal)
                    && binary.Method.DeclaringType != typeof(DateTime) && binary.Method.DeclaringType != typeof(DateTimeOffset)) throw RelationalQueryCompiler.Unsupported(expression);
                return Operation(binary.NodeType == ExpressionType.Add && binary.Type == typeof(string) ? "Concat" : binary.NodeType.ToString(), binary.Type,
                    Translate(binary.Left), Translate(binary.Right));
            }
            if (expression is UnaryExpression unary)
            {
                if (unary.NodeType == ExpressionType.Convert || unary.NodeType == ExpressionType.ConvertChecked)
                {
                    // CLR-only boxing and enum conversions do not alter the database value.
                    if (unary.Type == typeof(object) || unary.Type.IsAssignableFrom(unary.Operand.Type))
                        return Operation("ClrConvert", unary.Type, Translate(unary.Operand));
                    var sourceType = Nullable.GetUnderlyingType(unary.Operand.Type) ?? unary.Operand.Type;
                    if (sourceType.IsEnum && sourceType.IsDefined(typeof(StoreAsTextAttribute), true))
                        return Operation("EnumNumber", unary.Type, Translate(unary.Operand));
                    return Operation("Convert", unary.Type, Translate(unary.Operand));
                }
                if (unary.NodeType == ExpressionType.Not || unary.NodeType == ExpressionType.OnesComplement)
                    return Operation(unary.Type == typeof(bool) || unary.Type == typeof(bool?) ? "Not" : "BitNot", unary.Type, Translate(unary.Operand));
                if (unary.NodeType == ExpressionType.Negate || unary.NodeType == ExpressionType.NegateChecked)
                    return Operation("Negate", unary.Type, Translate(unary.Operand));
            }
            if (expression is ConditionalExpression conditional)
                return Operation("Case", conditional.Type, Translate(conditional.Test), Translate(conditional.IfTrue), Translate(conditional.IfFalse));
            if (expression is MemberExpression member)
            {
                if (member.Member.DeclaringType == typeof(DateTime) && member.Expression == null
                    && (member.Member.Name == "Now" || member.Member.Name == "UtcNow" || member.Member.Name == "Today"))
                    return Operation("DateTime." + member.Member.Name, member.Type);
                if (Nullable.GetUnderlyingType(member.Expression?.Type ?? typeof(object)) != null)
                {
                    if (member.Member.Name == "HasValue") return Operation("IsNotNull", typeof(bool), Translate(member.Expression));
                    if (member.Member.Name == "Value") return Operation("ClrConvert", member.Type, Translate(member.Expression));
                }
                if (member.Expression?.Type == typeof(string) && member.Member.Name == "Length")
                    return Operation("String.Length", typeof(int), Translate(member.Expression));
                if (member.Member.DeclaringType == typeof(DateTime))
                {
                    var allowed = new[] { "Year", "Month", "Day", "Hour", "Minute", "Second", "Date", "DayOfWeek" };
                    if (allowed.Contains(member.Member.Name)) return Operation("DateTime." + member.Member.Name, member.Type, Translate(member.Expression));
                }
            }
            if (expression is MethodCallExpression call) return Method(call);
            throw RelationalQueryCompiler.Unsupported(expression);
        }

        SqlExpression Method(MethodCallExpression call)
        {
            var name = call.Method.Name;
            var declaringType = call.Method.DeclaringType;
            var source = call.Object ?? call.Arguments.FirstOrDefault();
            if (source is GroupProjectionExpression group)
            {
                if (name == "Count" || name == "LongCount" || name == "Sum" || name == "Min" || name == "Max" || name == "Average" || name == "Any")
                {
                    SqlExpression argument = null;
                    if (call.Arguments.Count == 2)
                    {
                        var body = RelationalQueryCompiler.Bind(RelationalQueryCompiler.Lambda(call.Arguments[1]), group.Element);
                        argument = Translate(body);
                        if (name == "Count" || name == "LongCount" || name == "Any")
                            argument = Operation("Case", typeof(int?), argument, new SqlConstantExpression(1, typeof(int?)), new SqlConstantExpression(null, typeof(int?)));
                    }
                    else if (name != "Count" && name != "LongCount" && name != "Any") argument = Translate(group.Element);
                    var aggregate = Operation(name == "Any" ? "Count" : name, name == "Any" ? typeof(int) : call.Type,
                        argument == null ? Array.Empty<SqlExpression>() : new[] { argument });
                    if (name == "Any") return Operation("GreaterThan", typeof(bool), aggregate, RelationalQueryCompiler.Integer(0));
                    if (name == "Sum") return Operation("Coalesce", call.Type, aggregate, new SqlConstantExpression(0, call.Type));
                    return aggregate;
                }
            }
            if (name == "Contains" && source != null && source.Type != typeof(string)
                && (declaringType == typeof(Enumerable) || declaringType == typeof(Queryable)
                    || (call.Object != null && declaringType.Namespace == "System.Collections.Generic")))
            {
                if (call.Arguments.Count != (call.Object == null ? 2 : 1)) throw RelationalQueryCompiler.Unsupported(call);
                var operand = Translate(call.Arguments[call.Object == null ? 1 : 0]);
                if (source is QueryRootExpression || (source is MethodCallExpression queryCall && queryCall.Method.DeclaringType == typeof(Queryable)))
                {
                    var subquery = _compiler.Query(source);
                    _compiler.Wrap(subquery, false);
                    subquery.Query.Predicate = RelationalQueryCompiler.And(subquery.Query.Predicate, Operation("Equal", typeof(bool), Translate(subquery.Projection), operand));
                    subquery.Query.Projections.Add(RelationalQueryCompiler.Integer(1));
                    return new SqlSubqueryExpression("exists", subquery.Query, typeof(bool));
                }
                return Operation("Contains", typeof(bool), Translate(source), operand);
            }
            if (declaringType == typeof(Queryable) && (name == "Any" || name == "All" || name == "Count" || name == "LongCount" || name == "Sum" || name == "Min" || name == "Max" || name == "Average"))
            {
                var subquery = _compiler.Query(call);
                subquery.Query.Projections.Add(Translate(subquery.Projection));
                return new SqlSubqueryExpression("scalar", subquery.Query, call.Type);
            }
            if (declaringType == typeof(KueryFunctions) && name == nameof(KueryFunctions.Like) && call.Arguments.Count == 2)
                return Operation("Like", call.Type, call.Arguments.Select(Translate).ToArray());
            if (name == "Equals")
            {
                if (call.Object != null && call.Arguments.Count == 1 && (declaringType == typeof(object) || declaringType == typeof(string)
                    || declaringType.IsPrimitive || declaringType.IsEnum || declaringType == typeof(decimal) || declaringType == typeof(DateTime)
                    || declaringType == typeof(DateTimeOffset) || declaringType == typeof(Guid)
                    || (declaringType.IsGenericType && declaringType.GetGenericTypeDefinition() == typeof(Nullable<>))))
                    return Operation("Equal", typeof(bool), Translate(call.Object), Translate(call.Arguments[0]));
                if (call.Object == null && call.Arguments.Count == 2 && (declaringType == typeof(object) || declaringType == typeof(string) || declaringType == typeof(Nullable)))
                    return Operation("Equal", typeof(bool), Translate(call.Arguments[0]), Translate(call.Arguments[1]));
            }
            if (declaringType == typeof(string))
            {
                var args = (call.Object == null ? call.Arguments : new[] { call.Object }.Concat(call.Arguments)).ToArray();
                var count = call.Arguments.Count;
                if ((name == "Contains" || name == "StartsWith" || name == "EndsWith") && call.Object != null
                    && (count == 1 || (count == 2 && call.Arguments[1].Type == typeof(StringComparison))))
                    return Operation("String." + name, call.Type, args.Select(Translate).ToArray());
                if (((name == "ToLower" || name == "ToUpper" || name == "Trim" || name == "TrimStart" || name == "TrimEnd") && count == 0)
                    || (name == "Replace" && count == 2 && call.Arguments[0].Type == typeof(string))
                    || (name == "Substring" && (count == 1 || count == 2))
                    || (name == "IndexOf" && count == 1 && call.Arguments[0].Type == typeof(string))
                    || ((name == "IsNullOrEmpty" || name == "IsNullOrWhiteSpace") && count == 1)
                    || (name == "Concat" && count >= 2 && call.Arguments.All(a => a.Type == typeof(string))))
                    return Operation("String." + name, call.Type, args.Select(Translate).ToArray());
            }
            if (declaringType == typeof(Math))
            {
                var allowed = new[] { "Abs", "Round", "Floor", "Ceiling", "Max", "Min", "Pow", "Sqrt", "Log", "Log10" };
                if (allowed.Contains(name) && call.Arguments.All(a => a.Type != typeof(MidpointRounding)))
                    return Operation("Math." + name, call.Type, call.Arguments.Select(Translate).ToArray());
            }
            if (declaringType == typeof(DateTime) && call.Object != null && call.Arguments.Count == 1)
            {
                var allowed = new[] { "AddDays", "AddMonths", "AddYears", "AddHours", "AddMinutes", "AddSeconds" };
                if (allowed.Contains(name)) return Operation("DateTime." + name, call.Type, Translate(call.Object), Translate(call.Arguments[0]));
            }
            if (declaringType == typeof(Convert) && call.Arguments.Count == 1
                && new[] { "ToInt16", "ToInt32", "ToInt64", "ToDouble", "ToSingle", "ToBoolean", "ToString", "ToDecimal", "ToByte" }.Contains(name))
                return Operation("Convert", call.Type, Translate(call.Arguments[0]));
            if (name == "ToString" && call.Arguments.Count == 0 && call.Object != null
                && (call.Object.Type.IsPrimitive || call.Object.Type == typeof(decimal) || call.Object.Type == typeof(string)))
                return Operation("Convert", typeof(string), Translate(call.Object));
            if (declaringType?.IsGenericType == true && declaringType.GetGenericTypeDefinition() == typeof(Nullable<>) && name == "GetValueOrDefault")
                return Operation("Coalesce", call.Type, Translate(call.Object), call.Arguments.Count == 1 ? Translate(call.Arguments[0]) : new SqlConstantExpression(Activator.CreateInstance(call.Type), call.Type));
            throw RelationalQueryCompiler.Unsupported(call);
        }

        static SqlOperationExpression Operation(string name, Type type, params SqlExpression[] args)
            => new SqlOperationExpression(name, type, Nullable.GetUnderlyingType(type) != null || (!type.IsValueType && args.Any(a => a.IsNullable)), args);
    }
}
