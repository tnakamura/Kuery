using System;
using System.Linq;
using System.Linq.Expressions;
using Kuery.Query.SqlExpressions;

namespace Kuery.Query
{
    internal sealed class SqlExpressionFactory
    {
        internal SqlBinaryExpression MakeBinary(ExpressionType operatorType, Type type, SqlExpression left, SqlExpression right, bool useRelationalNulls = false)
            => new SqlBinaryExpression(operatorType, left, right, type, Nullable.GetUnderlyingType(type) != null
                || (!type.IsValueType && (left.IsNullable || right.IsNullable)), useRelationalNulls);

        internal SqlUnaryExpression MakeUnary(ExpressionType operatorType, Type type, SqlExpression operand, bool clrOnly = false)
            => new SqlUnaryExpression(operatorType, operand, type, Nullable.GetUnderlyingType(type) != null || (!type.IsValueType && operand.IsNullable), clrOnly);

        internal SqlFunctionExpression Function(string name, Type type, params SqlExpression[] arguments)
            => new SqlFunctionExpression(name, type, Nullable.GetUnderlyingType(type) != null || (!type.IsValueType && arguments.Any(a => a.IsNullable)), arguments);

        internal CaseExpression Case(Type type, SqlExpression test, SqlExpression ifTrue, SqlExpression ifFalse)
            => new CaseExpression(type, ifTrue.IsNullable || ifFalse.IsNullable, new[] { new CaseWhenClause(test, ifTrue) }, ifFalse);

        internal SqlExpression AndAlso(SqlExpression left, SqlExpression right)
            => left == null ? right : MakeBinary(ExpressionType.AndAlso, typeof(bool), left, right);
    }
}
