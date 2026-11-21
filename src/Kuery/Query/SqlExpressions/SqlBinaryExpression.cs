using System;
using System.Linq.Expressions;

namespace Kuery.Query.SqlExpressions
{
    internal sealed class SqlBinaryExpression : SqlExpression
    {
        internal SqlBinaryExpression(ExpressionType operatorType, SqlExpression left, SqlExpression right, Type type, bool nullable, bool useRelationalNulls = false)
            : base(type, nullable)
        {
            OperatorType = operatorType;
            Left = left;
            Right = right;
            UseRelationalNulls = useRelationalNulls;
        }
        internal ExpressionType OperatorType { get; }
        internal SqlExpression Left { get; }
        internal SqlExpression Right { get; }
        internal bool UseRelationalNulls { get; }
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
