using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading;

namespace Kuery.Linq
{
    internal sealed class BoundParameter
    {
        internal string Name;
        internal object Value;
        internal Type Type;
        internal Type InputType;
        internal int SourceIndex;
    }

    internal sealed class RelationalCommand
    {
        internal string CommandText;
        internal readonly List<BoundParameter> Parameters = new List<BoundParameter>();
    }

    internal sealed class RelationalSqlGenerator
    {
        static readonly HashSet<string> PredicateOperations = new HashSet<string>
        {
            "Equal", "SqlEqual", "NotEqual", "GreaterThan", "GreaterThanOrEqual", "LessThan", "LessThanOrEqual", "Not", "AndAlso", "OrElse",
            "IsNull", "IsNotNull", "EntityIsNull", "EntityIsNotNull", "Contains", "Like", "String.Contains", "String.StartsWith",
            "String.EndsWith", "String.IsNullOrEmpty", "String.IsNullOrWhiteSpace",
        };
        static readonly IReadOnlyDictionary<string, string> BinaryOperators = new Dictionary<string, string>
        {
            { "Add", "+" }, { "AddChecked", "+" }, { "Subtract", "-" }, { "SubtractChecked", "-" },
            { "Multiply", "*" }, { "MultiplyChecked", "*" }, { "Divide", "/" }, { "Modulo", "%" },
            { "And", "&" }, { "Or", "|" }, { "ExclusiveOr", "^" },
            { "GreaterThan", ">" }, { "GreaterThanOrEqual", ">=" }, { "LessThan", "<" }, { "LessThanOrEqual", "<=" },
        };
        readonly ISqlDialect _dialect;
        readonly IReadOnlyList<object> _values;
        readonly RelationalCommand _command = new RelationalCommand();
        readonly Dictionary<int, string> _parameters = new Dictionary<int, string>();
        int _derived;
        bool _runtimeShape;

        internal RelationalSqlGenerator(ISqlDialect dialect, IReadOnlyList<object> values) { _dialect = dialect; _values = values; }

        internal static RelationalCommand GenerateCached(RelationalQueryPlan plan, ISqlDialect dialect, IReadOnlyList<object> values)
        {
            var template = Volatile.Read(ref plan.CommandTemplate);
            if (template != null)
            {
                var bound = new RelationalCommand { CommandText = template.CommandText };
                foreach (var parameter in template.Parameters)
                    bound.Parameters.Add(new BoundParameter { Name = parameter.Name, Type = parameter.Type,
                        Value = ParameterValue(values[parameter.SourceIndex], parameter.InputType) });
                return bound;
            }
            var generator = new RelationalSqlGenerator(dialect, values);
            var command = generator.Generate(plan);
            if (!generator._runtimeShape)
            {
                template = new RelationalCommand { CommandText = command.CommandText };
                foreach (var parameter in command.Parameters)
                    template.Parameters.Add(new BoundParameter { Name = parameter.Name, Type = parameter.Type,
                        InputType = parameter.InputType, SourceIndex = parameter.SourceIndex });
                // Templates contain only slot metadata, never the first execution's values.
                Interlocked.CompareExchange(ref plan.CommandTemplate, template, null);
            }
            return command;
        }
        internal RelationalCommand Generate(RelationalQueryPlan plan)
        {
            if (plan.Mutation == null) _command.CommandText = Select(plan.Query);
            else
            {
                var table = _dialect.EscapeIdentifier(plan.MutationTable.TableName);
                var alias = _dialect.EscapeIdentifier(plan.Query.From.Alias);
                if (plan.Mutation == "delete")
                    _command.CommandText = (_dialect.Kind == SqlDialectKind.SqlServer || _dialect.Kind == SqlDialectKind.MySql)
                        ? "delete " + alias + " from " + table + " " + alias : "delete from " + table + " as " + alias;
                else
                {
                    _command.CommandText = "update " + (_dialect.Kind == SqlDialectKind.SqlServer ? alias : table + " as " + alias)
                        + " set " + string.Join(", ", plan.Setters.Select(s => _dialect.EscapeIdentifier(s.Key) + " = " + Value(s.Value)));
                    if (_dialect.Kind == SqlDialectKind.SqlServer) _command.CommandText += " from " + table + " " + alias;
                }
                _command.CommandText += " where " + Predicate(plan.Query.Predicate);
            }
            return _command;
        }

        string Select(SqlSelectExpression query)
        {
            var sql = new StringBuilder("select ");
            if (query.Distinct) sql.Append("distinct ");
            if (_dialect.Kind == SqlDialectKind.SqlServer && query.Limit != null && query.Offset == null)
                sql.Append("top (").Append(Value(query.Limit)).Append(") ");
            sql.Append(query.Projections.Count == 0 ? "1" : string.Join(", ", query.Projections.Select((p, i) => Value(p) + " as " + _dialect.EscapeIdentifier("c" + i))));
            if (query.From != null) sql.Append(" from ").Append(Source(query.From));
            foreach (var join in query.Joins)
            {
                sql.Append(' ').Append(join.Kind).Append(" join ").Append(Source(join.Source));
                if (join.Predicate != null) sql.Append(" on ").Append(Predicate(join.Predicate));
            }
            var predicate = query.Predicate == null ? null : Predicate(query.Predicate);
            if (_dialect.Kind == SqlDialectKind.SqlServer && query.Offset != null && query.Limit != null)
                predicate = (predicate == null ? "" : "(" + predicate + ") and ") + "(" + Value(query.Limit) + " > 0)";
            if (predicate != null) sql.Append(" where ").Append(predicate);
            if (query.GroupBy.Count > 0) sql.Append(" group by ").Append(string.Join(", ", query.GroupBy.Select(Value)));
            if (query.Having != null) sql.Append(" having ").Append(Predicate(query.Having));
            if (query.Orderings.Count > 0)
                sql.Append(" order by ").Append(string.Join(", ", query.Orderings.Select(o => Value(o.Expression) + (o.Ascending ? " asc" : " desc"))));
            else if (_dialect.Kind == SqlDialectKind.SqlServer && query.Offset != null) sql.Append(" order by (select 1)");
            if (_dialect.Kind == SqlDialectKind.SqlServer)
            {
                if (query.Offset != null)
                {
                    sql.Append(" offset ").Append(Value(query.Offset)).Append(" rows");
                    if (query.Limit != null)
                    {
                        var limit = Value(query.Limit);
                        sql.Append(" fetch next (case when ").Append(limit).Append(" > 0 then ").Append(limit).Append(" else 1 end) rows only");
                    }
                }
            }
            else
            {
                if (query.Limit != null) sql.Append(" limit ").Append(PagingValue(query.Limit));
                else if (query.Offset != null && _dialect.Kind != SqlDialectKind.PostgreSql)
                    sql.Append(" limit ").Append(_dialect.Kind == SqlDialectKind.MySql ? SqlConstants.MySqlOffsetWithoutLimitMax : "-1");
                if (query.Offset != null) sql.Append(" offset ").Append(PagingValue(query.Offset));
            }
            return sql.ToString();
        }

        string PagingValue(SqlExpression expression)
        {
            if (_dialect.Kind != SqlDialectKind.MySql) return Value(expression);
            // MySQL LIMIT/OFFSET accepts integer literals or parameters, not CASE/scalar expressions.
            _runtimeShape = true;
            return Parameter(EvaluatePaging(expression), typeof(int));
        }

        int EvaluatePaging(SqlExpression expression)
        {
            if (expression is SqlParameterExpression parameter) return (int)_values[parameter.Index];
            if (expression is SqlConstantExpression constant) return Convert.ToInt32(constant.Value, CultureInfo.InvariantCulture);
            if (expression is SqlOperationExpression operation)
            {
                if (operation.Operation == "NonNegative") return Math.Max(0, EvaluatePaging(operation.Arguments[0]));
                if (operation.Operation == "Least") return Math.Min(EvaluatePaging(operation.Arguments[0]), EvaluatePaging(operation.Arguments[1]));
            }
            throw new NotSupportedException("Paging requires an integer value independent of the current row.");
        }

        string Source(SqlTableSource source)
        {
            string body;
            if (source.Query != null) body = "(" + Select(source.Query) + ")";
            else if (source.Left != null)
            {
                // Wrapping each operand makes precedence and operand paging portable across all four databases.
                var leftAlias = _dialect.EscapeIdentifier("s" + _derived++);
                var rightAlias = _dialect.EscapeIdentifier("s" + _derived++);
                body = "(select * from (" + Select(source.Left) + ") " + leftAlias + " " + source.SetOperator
                    + " select * from (" + Select(source.Right) + ") " + rightAlias + ")";
            }
            else body = _dialect.EscapeIdentifier(source.TableName);
            return body + " " + _dialect.EscapeIdentifier(source.Alias);
        }

        internal string Value(SqlExpression expression)
        {
            if (_dialect.Kind == SqlDialectKind.SqlServer && IsPredicate(expression))
                return "(case when " + Predicate(expression) + " then cast(1 as bit) else cast(0 as bit) end)";
            return Render(expression);
        }

        internal string Predicate(SqlExpression expression)
        {
            if (IsPredicate(expression)) return Render(expression);
            return _dialect.Kind == SqlDialectKind.PostgreSql ? Value(expression) : "(" + Value(expression) + " = 1)";
        }

        static bool IsPredicate(SqlExpression expression)
        {
            if (expression is SqlSubqueryExpression subquery) return subquery.Operation != "scalar";
            if (!(expression is SqlOperationExpression op)) return false;
            return PredicateOperations.Contains(op.Operation)
                || ((op.Operation == "And" || op.Operation == "Or") && op.Type == typeof(bool));
        }

        string Render(SqlExpression expression)
        {
            if (expression is SqlColumnExpression column)
                return _dialect.EscapeIdentifier(column.Alias) + "." + _dialect.EscapeIdentifier(column.Name);
            if (expression is SqlParameterExpression parameter)
            {
                if (!_parameters.TryGetValue(parameter.Index, out var name))
                { name = Parameter(_values[parameter.Index], parameter.Type, parameter.Index); _parameters.Add(parameter.Index, name); }
                return name;
            }
            if (expression is SqlConstantExpression constant)
            {
                if (constant.Value == null) return "null";
                if (constant.Value is bool boolean) return _dialect.Kind == SqlDialectKind.PostgreSql ? (boolean ? "true" : "false") : (boolean ? "1" : "0");
                if (constant.Value is string text) return "'" + text.Replace("'", "''") + "'";
                return Convert.ToString(constant.Value, CultureInfo.InvariantCulture);
            }
            if (expression is SqlSubqueryExpression subquery)
                return subquery.Operation == "scalar" ? "(" + Select(subquery.Query) + ")" : subquery.Operation + " (" + Select(subquery.Query) + ")";
            var operation = (SqlOperationExpression)expression;
            var args = operation.Arguments;
            var nameOp = operation.Operation;
            if (nameOp == "Contains") return Contains(args[0], args[1]);
            if (nameOp == "EntityIsNull" || nameOp == "EntityIsNotNull")
            {
                if (!IsNull(args[1])) throw new NotSupportedException("Entity comparisons support only null. Compare mapped key properties instead.");
                return "(" + Value(args[0]) + (nameOp == "EntityIsNull" ? " is null)" : " is not null)");
            }
            if (nameOp == "Equal" || nameOp == "NotEqual") return Compare(args[0], args[1], nameOp == "Equal");
            if (nameOp == "SqlEqual") return "(" + Value(args[0]) + " = " + Value(args[1]) + ")";
            if (nameOp == "Not") return "(not (" + Predicate(args[0]) + "))";
            if (nameOp == "AndAlso" || nameOp == "OrElse" || ((nameOp == "And" || nameOp == "Or") && operation.Type == typeof(bool)))
                return "(" + Predicate(args[0]) + ((nameOp == "AndAlso" || nameOp == "And") ? " and " : " or ") + Predicate(args[1]) + ")";
            if (nameOp == "IsNull" || nameOp == "IsNotNull") return "(" + Value(args[0]) + (nameOp == "IsNull" ? " is null)" : " is not null)");
            if (nameOp == "ClrConvert") return Value(args[0]);
            if (nameOp == "EnumNumber")
            {
                var type = Nullable.GetUnderlyingType(args[0].Type) ?? args[0].Type;
                return "(case " + Value(args[0]) + " " + string.Join(" ", Enum.GetNames(type).Select(name => "when '" + name.Replace("'", "''") + "' then "
                    + Convert.ToString(Convert.ChangeType(Enum.Parse(type, name), Enum.GetUnderlyingType(type), CultureInfo.InvariantCulture), CultureInfo.InvariantCulture))) + " end)";
            }
            if (nameOp == "Convert") return "cast(" + Value(args[0]) + " as " + _dialect.CastType(operation.Type) + ")";
            if (nameOp == "Case") return "(case when " + Predicate(args[0]) + " then " + Value(args[1]) + " else " + Value(args[2]) + " end)";
            if (nameOp == "Coalesce") return "coalesce(" + string.Join(", ", args.Select(Value)) + ")";
            if (nameOp == "NonNegative") { var arg = Value(args[0]); return "(case when " + arg + " < 0 then 0 else " + arg + " end)"; }
            if (nameOp == "Least" || nameOp == "Greatest")
            {
                var a = Value(args[0]); var b = Value(args[1]);
                return "(case when " + a + (nameOp == "Least" ? " < " : " > ") + b + " then " + a + " else " + b + " end)";
            }
            if (nameOp == "Negate") return "(-" + Value(args[0]) + ")";
            if (nameOp == "BitNot") return "(~" + Value(args[0]) + ")";
            if (nameOp == "Concat") return _dialect.Function("String.Concat", args.Select(Value).ToArray());
            if (nameOp == "Like") return "(" + Value(args[0]) + " like " + Value(args[1]) + ")";
            if (nameOp.StartsWith("String.", StringComparison.Ordinal)) return StringFunction(operation);
            if (nameOp.StartsWith("DateTime.", StringComparison.Ordinal) || nameOp.StartsWith("Math.", StringComparison.Ordinal))
                return _dialect.Function(nameOp, args.Select(Value).ToArray());
            if (nameOp == "Count" || nameOp == "LongCount" || nameOp == "Sum" || nameOp == "Min" || nameOp == "Max" || nameOp == "Average")
            {
                var function = nameOp == "Average" ? "avg" : nameOp == "LongCount" ? (_dialect.Kind == SqlDialectKind.SqlServer ? "count_big" : "count") : nameOp.ToLowerInvariant();
                var arg = args.Count == 0 ? "*" : Value(args[0]);
                if (nameOp == "Average" && args.Count == 1 && IsIntegral(args[0].Type)) arg = "cast(" + arg + " as " + _dialect.CastType(typeof(double)) + ")";
                return function + "(" + arg + ")";
            }
            if (!BinaryOperators.TryGetValue(nameOp, out var token)) throw new NotSupportedException("Unsupported SQL operation: " + nameOp);
            var left = Value(args[0]); var right = Value(args[1]);
            if (nameOp == "ExclusiveOr" && _dialect.Kind == SqlDialectKind.Sqlite) return "((" + left + " | " + right + ") - (" + left + " & " + right + "))";
            if (nameOp == "ExclusiveOr" && _dialect.Kind == SqlDialectKind.PostgreSql) token = "#";
            var result = "(" + left + " " + token + " " + right + ")";
            if ((token == ">" || token == ">=" || token == "<" || token == "<=") && operation.Type == typeof(bool) && (args[0].IsNullable || args[1].IsNullable))
                result = "(" + left + " is not null and " + right + " is not null and " + result + ")";
            return result;
        }

        string Compare(SqlExpression left, SqlExpression right, bool equal)
        {
            var a = Value(left); var b = Value(right);
            var leftNull = IsNull(left); var rightNull = IsNull(right);
            if (leftNull || rightNull) return "(" + (leftNull ? b : a) + (equal ? " is null)" : " is not null)");
            var comparison = "(" + a + (equal ? " = " : " <> ") + b + ")";
            if (!left.IsNullable && !right.IsNullable) return comparison;
            if (equal)
                return "((" + comparison + " and " + a + " is not null and " + b + " is not null) or (" + a + " is null and " + b + " is null))";
            return "((" + comparison + " and " + a + " is not null and " + b + " is not null) or (" + a + " is null and " + b + " is not null) or (" + a + " is not null and " + b + " is null))";
        }

        bool IsNull(SqlExpression expression)
        {
            if (expression is SqlConstantExpression constant) return constant.Value == null;
            if (expression is SqlParameterExpression parameter)
            {
                if (!parameter.IsNullable) return false;
                _runtimeShape = true;
                return _values[parameter.Index] == null;
            }
            if (expression is SqlOperationExpression op && (op.Operation == "ClrConvert" || op.Operation == "Convert")) return IsNull(op.Arguments[0]);
            return false;
        }

        string Contains(SqlExpression source, SqlExpression operand)
        {
            _runtimeShape = true;
            object collection = source is SqlParameterExpression parameter ? _values[parameter.Index] : (source as SqlConstantExpression)?.Value;
            if (!(collection is IEnumerable items)) throw new NotSupportedException("Contains requires a non-null collection parameter.");
            var parameters = new List<string>(); bool hasNull = false;
            var type = source.Type.IsArray ? source.Type.GetElementType() : source.Type.GetInterfaces().Concat(new[] { source.Type })
                .FirstOrDefault(t => t.IsGenericType && t.GetGenericTypeDefinition() == typeof(IEnumerable<>))?.GetGenericArguments()[0] ?? operand.Type;
            foreach (var item in items)
            {
                if (item == null) hasNull = true;
                else parameters.Add(Parameter(item, type));
            }
            var value = Value(operand);
            if (parameters.Count == 0) return hasNull ? "(" + value + " is null)" : "(1 = 0)";
            var result = "(" + value + " is not null and " + value + " in (" + string.Join(", ", parameters) + "))";
            return hasNull ? "(" + result + " or " + value + " is null)" : result;
        }

        string StringFunction(SqlOperationExpression operation)
        {
            var args = operation.Arguments;
            var name = operation.Operation;
            if (name == "String.Contains" || name == "String.StartsWith" || name == "String.EndsWith")
            {
                var source = Value(args[0]); var pattern = Value(args[1]);
                if (args.Count == 3)
                {
                    _runtimeShape = true;
                    var comparison = args[2] is SqlParameterExpression parameter ? (StringComparison)_values[parameter.Index] : (StringComparison)((SqlConstantExpression)args[2]).Value;
                    if (comparison == StringComparison.OrdinalIgnoreCase || comparison == StringComparison.CurrentCultureIgnoreCase || comparison == StringComparison.InvariantCultureIgnoreCase)
                    { source = "lower(" + source + ")"; pattern = "lower(" + pattern + ")"; }
                }
                var length = _dialect.Function("String.Length", new[] { pattern });
                string condition;
                if (name == "String.Contains") condition = "(" + _dialect.Function("String.Position", new[] { source, pattern }) + " > 0)";
                else if (name == "String.StartsWith") condition = "(" + _dialect.Function("String.Substring", new[] { source, "0", length }) + " = " + pattern + ")";
                else condition = "(" + _dialect.Function("String.Substring", new[] { source, "(" + _dialect.Function("String.Length", new[] { source }) + " - " + length + ")", length }) + " = " + pattern + ")";
                return "(" + source + " is not null and " + pattern + " is not null and (" + length + " = 0 or " + condition + "))";
            }
            if (name == "String.IsNullOrEmpty" || name == "String.IsNullOrWhiteSpace")
            {
                var value = Value(args[0]);
                return "(" + value + " is null or " + (name == "String.IsNullOrWhiteSpace" ? _dialect.Function("String.Trim", new[] { value }) : value) + " = '')";
            }
            return _dialect.Function(name, args.Select(Value).ToArray());
        }

        static object ParameterValue(object value, Type type)
        {
            var target = Nullable.GetUnderlyingType(type) ?? type;
            if (!target.IsEnum || value == null) return value;
            return target.IsDefined(typeof(StoreAsTextAttribute), true) ? value.ToString()
                : Convert.ChangeType(value, Enum.GetUnderlyingType(target), CultureInfo.InvariantCulture);
        }

        string Parameter(object value, Type type, int sourceIndex = -1)
        {
            if (type == typeof(object))
            {
                _runtimeShape = true;
                if (value != null) type = value.GetType();
            }
            var inputType = type;
            value = ParameterValue(value, type);
            var target = Nullable.GetUnderlyingType(type) ?? type;
            if (target.IsEnum)
            {
                bool text = target.IsDefined(typeof(StoreAsTextAttribute), true);
                type = text ? typeof(string) : Enum.GetUnderlyingType(target);
            }
            var name = _dialect.FormatParameterName("p" + (_command.Parameters.Count + 1));
            _command.Parameters.Add(new BoundParameter { Name = name, Value = value, Type = type, InputType = inputType, SourceIndex = sourceIndex });
            return name;
        }

        static bool IsIntegral(Type type)
        { type = Nullable.GetUnderlyingType(type) ?? type; return type == typeof(int) || type == typeof(long) || type == typeof(short) || type == typeof(byte); }
    }
}
