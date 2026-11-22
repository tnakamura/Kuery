using System;
using System.Linq;
using System.Linq.Expressions;
using Kuery.Query.SqlExpressions;

namespace Kuery.Query
{
    /// <summary>Constructs SQL expression nodes with consistent CLR type and nullability metadata.</summary>
    internal sealed class SqlExpressionFactory
    {
        /// <summary>Creates a binary SQL expression and infers its result nullability.</summary>
        /// <param name="operatorType">The expression tree operator that represents the SQL operation.</param>
        /// <param name="type">The CLR type associated with the value or result.</param>
        /// <param name="left">The left operand of the operation.</param>
        /// <param name="right">The right operand of the operation.</param>
        /// <param name="useRelationalNulls">Whether to preserve SQL null semantics without CLR null compensation.</param>
        /// <returns>The new binary SQL expression.</returns>
        internal SqlBinaryExpression MakeBinary(ExpressionType operatorType, Type type, SqlExpression left, SqlExpression right, bool useRelationalNulls = false)
            => new SqlBinaryExpression(operatorType, left, right, type, Nullable.GetUnderlyingType(type) != null
                || (!type.IsValueType && (left.IsNullable || right.IsNullable)), useRelationalNulls);

        /// <summary>Creates a unary SQL expression and infers its result nullability.</summary>
        /// <param name="operatorType">The expression tree operator that represents the SQL operation.</param>
        /// <param name="type">The CLR type associated with the value or result.</param>
        /// <param name="operand">The operand of the SQL operation.</param>
        /// <param name="clrOnly">Whether the conversion changes the CLR type without emitting a SQL cast.</param>
        /// <returns>The new unary SQL expression.</returns>
        internal SqlUnaryExpression MakeUnary(ExpressionType operatorType, Type type, SqlExpression operand, bool clrOnly = false)
            => new SqlUnaryExpression(operatorType, operand, type, Nullable.GetUnderlyingType(type) != null || (!type.IsValueType && operand.IsNullable), clrOnly);

        /// <summary>Creates a typed function expression with derived nullability metadata.</summary>
        /// <param name="name">The logical name of the function.</param>
        /// <param name="type">The CLR type associated with the value or result.</param>
        /// <param name="arguments">The SQL argument expressions in call order.</param>
        /// <returns>The function expression.</returns>
        internal SqlFunctionExpression Function(string name, Type type, params SqlExpression[] arguments)
            => new SqlFunctionExpression(name, type, Nullable.GetUnderlyingType(type) != null || (!type.IsValueType && arguments.Any(a => a.IsNullable)), arguments);

        /// <summary>Creates a conditional SQL CASE with one WHEN branch and an ELSE branch.</summary>
        /// <param name="type">The CLR type associated with the value or result.</param>
        /// <param name="test">The SQL condition evaluated before choosing the corresponding value.</param>
        /// <param name="ifTrue">The value used when the test matches.</param>
        /// <param name="ifFalse">The value used when the test does not match.</param>
        /// <returns>The CASE expression.</returns>
        internal CaseExpression Case(Type type, SqlExpression test, SqlExpression ifTrue, SqlExpression ifFalse)
            => new CaseExpression(type, ifTrue.IsNullable || ifFalse.IsNullable, new[] { new CaseWhenClause(test, ifTrue) }, ifFalse);

        /// <summary>Combines two SQL predicates with AND, allowing the first predicate to be absent.</summary>
        /// <param name="left">The existing predicate, or null if there is none.</param>
        /// <param name="right">The predicate to add.</param>
        /// <returns>The combined predicate, or the second predicate if the first is absent.</returns>
        internal SqlExpression AndAlso(SqlExpression left, SqlExpression right)
            => left == null ? right : MakeBinary(ExpressionType.AndAlso, typeof(bool), left, right);
    }
}
