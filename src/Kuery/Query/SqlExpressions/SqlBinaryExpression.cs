using System;
using System.Linq.Expressions;

namespace Kuery.Query.SqlExpressions
{
    /// <summary>Represents a SQL operation over two operands.</summary>
    internal sealed class SqlBinaryExpression : SqlExpression
    {
        /// <summary>Creates a typed binary SQL operation with explicit null comparison semantics.</summary>
        /// <param name="operatorType">The expression tree operator that represents the SQL operation.</param>
        /// <param name="left">The left operand of the operation.</param>
        /// <param name="right">The right operand of the operation.</param>
        /// <param name="type">The CLR type associated with the value or result.</param>
        /// <param name="nullable">Whether the SQL value can be NULL.</param>
        /// <param name="useRelationalNulls">Whether to preserve SQL null semantics without CLR null compensation.</param>
        internal SqlBinaryExpression(ExpressionType operatorType, SqlExpression left, SqlExpression right, Type type, bool nullable, bool useRelationalNulls = false)
            : base(type, nullable)
        {
            OperatorType = operatorType;
            Left = left;
            Right = right;
            UseRelationalNulls = useRelationalNulls;
        }
        /// <summary>Gets the kind of binary operation represented by this expression.</summary>
        internal ExpressionType OperatorType { get; }
        /// <summary>Gets the left operand of the binary operation.</summary>
        internal SqlExpression Left { get; }
        /// <summary>Gets the right operand of the binary operation.</summary>
        internal SqlExpression Right { get; }
        /// <summary>Gets whether this comparison must use SQL null semantics without CLR null compensation.</summary>
        internal bool UseRelationalNulls { get; }
        /// <summary>Replaces the left and right operands.</summary>
        /// <param name="left">The left operand of the operation.</param>
        /// <param name="right">The right operand of the operation.</param>
        /// <returns>This expression if unchanged, or the updated operation.</returns>
        internal SqlBinaryExpression Update(SqlExpression left, SqlExpression right)
            => ReferenceEquals(left, Left) && ReferenceEquals(right, Right) ? this
                : new SqlBinaryExpression(OperatorType, left, right, Type, IsNullable, UseRelationalNulls);
        /// <summary>Visits the left and right operands.</summary>
        /// <param name="visitor">The visitor to apply to the child expressions.</param>
        /// <returns>This expression, or an expression updated with the visited children.</returns>
        protected override Expression VisitChildren(ExpressionVisitor visitor)
            => Update((SqlExpression)visitor.Visit(Left), (SqlExpression)visitor.Visit(Right));
    }
}
