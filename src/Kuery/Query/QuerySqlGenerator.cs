using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using Kuery.Query.SqlExpressions;

namespace Kuery.Query
{
    internal sealed class QuerySqlGenerator : SqlExpressionVisitor
    {
        static readonly HashSet<string> PredicateOperations = new HashSet<string>
        {
            "String.Contains", "String.StartsWith", "String.EndsWith", "String.IsNullOrEmpty", "String.IsNullOrWhiteSpace",
        };
        static readonly IReadOnlyDictionary<ExpressionType, string> BinaryOperators = new Dictionary<ExpressionType, string>
        {
            { ExpressionType.Add, "+" }, { ExpressionType.AddChecked, "+" }, { ExpressionType.Subtract, "-" }, { ExpressionType.SubtractChecked, "-" },
            { ExpressionType.Multiply, "*" }, { ExpressionType.MultiplyChecked, "*" }, { ExpressionType.Divide, "/" }, { ExpressionType.Modulo, "%" },
            { ExpressionType.And, "&" }, { ExpressionType.Or, "|" }, { ExpressionType.ExclusiveOr, "^" },
            { ExpressionType.GreaterThan, ">" }, { ExpressionType.GreaterThanOrEqual, ">=" }, { ExpressionType.LessThan, "<" }, { ExpressionType.LessThanOrEqual, "<=" },
        };
        readonly ISqlDialect _dialect;
        readonly IReadOnlyList<object> _values;
        readonly RelationalCommand _command = new RelationalCommand();
        readonly Dictionary<int, string> _parameters = new Dictionary<int, string>();
        int _derived;
        bool _runtimeShape;
        string _sqlFragment;

        internal QuerySqlGenerator(ISqlDialect dialect, IReadOnlyList<object> values)
        {
            _dialect = dialect;
            _values = values;
        }

        internal bool CanCache => !_runtimeShape;

        internal RelationalCommand GetCommand(Expression queryExpression)
        {
            var nonQuery = queryExpression as NonQueryExpression;
            var select = nonQuery?.QueryExpression ?? (SelectExpression)queryExpression;
            var nullabilityProcessor = new SqlNullabilityProcessor(_values);
            var query = nullabilityProcessor.Process(select);
            if (nonQuery == null)
            {
                Visit(query);
                _command.CommandText = _sqlFragment;
            }
            else
            {
                var table = _dialect.EscapeIdentifier(nonQuery.Table.TableName);
                var alias = _dialect.EscapeIdentifier(query.From.Alias);
                if (nonQuery.Operation == "delete")
                    _command.CommandText = (_dialect.Kind == SqlDialectKind.SqlServer || _dialect.Kind == SqlDialectKind.MySql)
                        ? "delete " + alias + " from " + table + " " + alias : "delete from " + table + " as " + alias;
                else
                {
                    _command.CommandText = "update " + (_dialect.Kind == SqlDialectKind.SqlServer ? alias : table + " as " + alias)
                        + " set " + string.Join(", ", nonQuery.Setters.Select(s => _dialect.EscapeIdentifier(s.Key) + " = " + Value(nullabilityProcessor.Process(s.Value))));
                    if (_dialect.Kind == SqlDialectKind.SqlServer) _command.CommandText += " from " + table + " " + alias;
                }
                _command.CommandText += " where " + Predicate(query.Predicate);
            }
            _runtimeShape |= !nullabilityProcessor.CanCache;
            return _command;
        }

        string Select(SelectExpression query)
        {
            var sql = new StringBuilder("select ");
            if (query.IsDistinct) sql.Append("distinct ");
            if (_dialect.Kind == SqlDialectKind.SqlServer && query.Limit != null && query.Offset == null)
                sql.Append("top (").Append(Value(query.Limit)).Append(") ");
            sql.Append(query.Projection.Count == 0 ? "1" : string.Join(", ", query.Projection.Select(p => Value(p.Expression) + " as " + _dialect.EscapeIdentifier(p.Alias))));
            if (query.From != null) sql.Append(" from ").Append(Source(query.From));
            foreach (var join in query.Joins)
            {
                sql.Append(join is InnerJoinExpression ? " inner join " : join is LeftJoinExpression ? " left join " : " cross join ").Append(Source(join.Table));
                if (join is PredicateJoinExpressionBase predicateJoin) sql.Append(" on ").Append(Predicate(predicateJoin.JoinPredicate));
            }
            var predicate = query.Predicate == null ? null : Predicate(query.Predicate);
            if (_dialect.Kind == SqlDialectKind.SqlServer && query.Offset != null && query.Limit != null)
                predicate = (predicate == null ? "" : "(" + predicate + ") and ") + "(" + Value(query.Limit) + " > 0)";
            if (predicate != null) sql.Append(" where ").Append(predicate);
            if (query.GroupBy.Count > 0) sql.Append(" group by ").Append(string.Join(", ", query.GroupBy.Select(Value)));
            if (query.Having != null) sql.Append(" having ").Append(Predicate(query.Having));
            if (query.Orderings.Count > 0)
                sql.Append(" order by ").Append(string.Join(", ", query.Orderings.Select(o => Value(o.Expression) + (o.IsAscending ? " asc" : " desc"))));
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
            if (expression is SqlFunctionExpression operation)
            {
                if (operation.Name == "NonNegative") return Math.Max(0, EvaluatePaging(operation.Arguments[0]));
                if (operation.Name == "Least") return Math.Min(EvaluatePaging(operation.Arguments[0]), EvaluatePaging(operation.Arguments[1]));
            }
            throw new NotSupportedException("Paging requires an integer value independent of the current row.");
        }

        string Source(TableExpressionBase source)
        {
            string body;
            if (source is SelectExpression select) body = "(" + Select(select) + ")";
            else if (source is SetOperationBase set)
            {
                var leftAlias = _dialect.EscapeIdentifier("s" + _derived++);
                var rightAlias = _dialect.EscapeIdentifier("s" + _derived++);
                var operation = set is IntersectExpression ? "intersect" : set is ExceptExpression ? "except" : set.IsDistinct ? "union" : "union all";
                body = "(select * from (" + Select(set.Source1) + ") " + leftAlias + " " + operation
                    + " select * from (" + Select(set.Source2) + ") " + rightAlias + ")";
            }
            else body = _dialect.EscapeIdentifier(((TableExpression)source).Name);
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
            if (expression is ExistsExpression || expression is InExpression || expression is LikeExpression) return true;
            if (expression is SqlBinaryExpression binary)
                return binary.OperatorType == ExpressionType.Equal || binary.OperatorType == ExpressionType.NotEqual
                    || binary.OperatorType == ExpressionType.GreaterThan || binary.OperatorType == ExpressionType.GreaterThanOrEqual
                    || binary.OperatorType == ExpressionType.LessThan || binary.OperatorType == ExpressionType.LessThanOrEqual
                    || binary.OperatorType == ExpressionType.AndAlso || binary.OperatorType == ExpressionType.OrElse
                    || ((binary.OperatorType == ExpressionType.And || binary.OperatorType == ExpressionType.Or) && binary.Type == typeof(bool));
            if (expression is SqlUnaryExpression unary)
                return unary.OperatorType == ExpressionType.Equal || unary.OperatorType == ExpressionType.NotEqual
                    || (unary.OperatorType == ExpressionType.Not && (unary.Type == typeof(bool) || unary.Type == typeof(bool?)));
            return expression is SqlFunctionExpression function && PredicateOperations.Contains(function.Name);
        }

        string Render(SqlExpression expression)
        {
            Visit(expression);
            return _sqlFragment;
        }

        protected override Expression VisitColumn(ColumnExpression column)
        {
            _sqlFragment = _dialect.EscapeIdentifier(column.TableAlias) + "." + _dialect.EscapeIdentifier(column.Name);
            return column;
        }

        protected override Expression VisitSqlParameter(SqlParameterExpression parameter)
        {
            if (!_parameters.TryGetValue(parameter.Index, out var name))
            {
                name = Parameter(_values[parameter.Index], parameter.Type, parameter.Index);
                _parameters.Add(parameter.Index, name);
            }
            _sqlFragment = name;
            return parameter;
        }

        protected override Expression VisitSqlConstant(SqlConstantExpression constant)
        {
            if (constant.Value == null) _sqlFragment = "null";
            else if (constant.Value is bool boolean) _sqlFragment = _dialect.Kind == SqlDialectKind.PostgreSql ? (boolean ? "true" : "false") : (boolean ? "1" : "0");
            else if (constant.Value is string text) _sqlFragment = "'" + text.Replace("'", "''") + "'";
            else _sqlFragment = Convert.ToString(constant.Value, CultureInfo.InvariantCulture);
            return constant;
        }

        protected override Expression VisitSqlBinary(SqlBinaryExpression binary)
        {
            _sqlFragment = GenerateBinary(binary);
            return binary;
        }

        string GenerateBinary(SqlBinaryExpression binary)
        {
            var op = binary.OperatorType;
            if (op == ExpressionType.Equal || op == ExpressionType.NotEqual)
            {
                return "(" + Value(binary.Left) + (op == ExpressionType.Equal ? " = " : " <> ") + Value(binary.Right) + ")";
            }
            if (op == ExpressionType.AndAlso || op == ExpressionType.OrElse || ((op == ExpressionType.And || op == ExpressionType.Or) && binary.Type == typeof(bool)))
                return "(" + Predicate(binary.Left) + ((op == ExpressionType.AndAlso || op == ExpressionType.And) ? " and " : " or ") + Predicate(binary.Right) + ")";
            if (!BinaryOperators.TryGetValue(op, out var token)) throw new NotSupportedException("Unsupported SQL operator: " + op);
            var left = Value(binary.Left); var right = Value(binary.Right);
            if (op == ExpressionType.ExclusiveOr && _dialect.Kind == SqlDialectKind.Sqlite) return "((" + left + " | " + right + ") - (" + left + " & " + right + "))";
            if (op == ExpressionType.ExclusiveOr && _dialect.Kind == SqlDialectKind.PostgreSql) token = "#";
            var result = "(" + left + " " + token + " " + right + ")";
            return result;
        }

        protected override Expression VisitSqlUnary(SqlUnaryExpression unary)
        {
            _sqlFragment = GenerateUnary(unary);
            return unary;
        }

        string GenerateUnary(SqlUnaryExpression unary)
        {
            switch (unary.OperatorType)
            {
                case ExpressionType.Equal: return "(" + Value(unary.Operand) + " is null)";
                case ExpressionType.NotEqual: return "(" + Value(unary.Operand) + " is not null)";
                case ExpressionType.Not:
                    return unary.Type == typeof(bool) || unary.Type == typeof(bool?)
                        ? "(not (" + Predicate(unary.Operand) + "))" : "(~" + Value(unary.Operand) + ")";
                case ExpressionType.Negate: return "(-" + Value(unary.Operand) + ")";
                case ExpressionType.Convert:
                    return unary.IsClrConversion ? Value(unary.Operand) : "cast(" + Value(unary.Operand) + " as " + _dialect.CastType(unary.Type) + ")";
                default: throw new NotSupportedException("Unsupported SQL unary operator: " + unary.OperatorType);
            }
        }

        protected override Expression VisitCase(CaseExpression expression)
        {
            _sqlFragment = "(case " + string.Join(" ", expression.WhenClauses.Select(c => "when " + Predicate(c.Test) + " then " + Value(c.Result)))
                + " else " + Value(expression.ElseResult) + " end)";
            return expression;
        }

        protected override Expression VisitIn(InExpression expression)
        {
            _sqlFragment = Contains(expression.Values, expression.Item);
            return expression;
        }

        protected override Expression VisitLike(LikeExpression expression)
        {
            _sqlFragment = "(" + Value(expression.Match) + " like " + Value(expression.Pattern) + ")";
            return expression;
        }

        protected override Expression VisitExists(ExistsExpression expression)
        {
            _sqlFragment = "exists (" + Select(expression.Subquery) + ")";
            return expression;
        }

        protected override Expression VisitScalarSubquery(ScalarSubqueryExpression expression)
        {
            _sqlFragment = "(" + Select(expression.Subquery) + ")";
            return expression;
        }

        protected override Expression VisitSqlFunction(SqlFunctionExpression expression)
        {
            _sqlFragment = GenerateFunction(expression);
            return expression;
        }

        protected override Expression VisitSelect(SelectExpression expression)
        {
            _sqlFragment = Select(expression);
            return expression;
        }

        string GenerateFunction(SqlFunctionExpression operation)
        {
            var args = operation.Arguments;
            var nameOp = operation.Name;
            if (nameOp == "EnumNumber")
            {
                var type = Nullable.GetUnderlyingType(args[0].Type) ?? args[0].Type;
                return "(case " + Value(args[0]) + " " + string.Join(" ", Enum.GetNames(type).Select(name => "when '" + name.Replace("'", "''") + "' then "
                    + Convert.ToString(Convert.ChangeType(Enum.Parse(type, name), Enum.GetUnderlyingType(type), CultureInfo.InvariantCulture), CultureInfo.InvariantCulture))) + " end)";
            }
            if (nameOp == "Coalesce") return "coalesce(" + string.Join(", ", args.Select(Value)) + ")";
            if (nameOp == "NonNegative") { var arg = Value(args[0]); return "(case when " + arg + " < 0 then 0 else " + arg + " end)"; }
            if (nameOp == "Least" || nameOp == "Greatest")
            {
                var a = Value(args[0]); var b = Value(args[1]);
                return "(case when " + a + (nameOp == "Least" ? " < " : " > ") + b + " then " + a + " else " + b + " end)";
            }
            if (nameOp == "Concat") return _dialect.Function("String.Concat", args.Select(Value).ToArray());
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
            throw new NotSupportedException("Unsupported SQL function: " + nameOp);
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

        string StringFunction(SqlFunctionExpression operation)
        {
            var args = operation.Arguments;
            var name = operation.Name;
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

        string Parameter(object value, Type type, int sourceIndex = -1)
        {
            if (type == typeof(object))
            {
                _runtimeShape = true;
                if (value != null) type = value.GetType();
            }
            var inputType = type;
            value = BoundParameter.ConvertValue(value, type);
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
