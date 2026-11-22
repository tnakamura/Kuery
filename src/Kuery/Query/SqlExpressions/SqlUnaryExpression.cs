using System;
using System.Linq.Expressions;

namespace Kuery.Query.SqlExpressions
{
    /// <summary>Represents a unary SQL operation or a conversion that affects only the CLR type.</summary>
    internal sealed class SqlUnaryExpression : SqlExpression
    {
        /// <summary>Creates a unary operation with optional CLR-only conversion semantics.</summary>
        /// <param name="operatorType">The expression tree operator that represents the SQL operation.</param>
        /// <param name="operand">The operand of the SQL operation.</param>
        /// <param name="type">The CLR type associated with the value or result.</param>
        /// <param name="nullable">Whether the SQL value can be NULL.</param>
        /// <param name="clrOnly">Whether the conversion changes the CLR type without emitting a SQL cast.</param>
        internal SqlUnaryExpression(ExpressionType operatorType, SqlExpression operand, Type type, bool nullable, bool clrOnly = false)
            : base(type, nullable)
        {
            OperatorType = operatorType;
            Operand = operand;
            IsClrConversion = clrOnly;
        }
        /// <summary>Gets the kind of unary operation represented by this expression.</summary>
        internal ExpressionType OperatorType { get; }
        /// <summary>Gets the SQL value or predicate on which the operation acts.</summary>
        internal SqlExpression Operand { get; }
        /// <summary>Gets whether the conversion changes the CLR type without emitting a SQL cast.</summary>
        internal bool IsClrConversion { get; }
        /// <summary>Replaces the operand of the unary operation.</summary>
        /// <param name="operand">The operand of the SQL operation.</param>
        /// <returns>This expression if unchanged, or the updated operation.</returns>
        internal SqlUnaryExpression Update(SqlExpression operand) => ReferenceEquals(operand, Operand) ? this
            : new SqlUnaryExpression(OperatorType, operand, Type, IsNullable, IsClrConversion);
        /// <summary>Visits the operand of the unary operation.</summary>
        /// <param name="visitor">The visitor to apply to the child expressions.</param>
        /// <returns>This expression, or an expression updated with the visited children.</returns>
        protected override Expression VisitChildren(ExpressionVisitor visitor) => Update((SqlExpression)visitor.Visit(Operand));
    }
}
