using System;
using System.Linq.Expressions;

namespace Kuery.Query.SqlExpressions
{
    internal sealed class SqlUnaryExpression : SqlExpression
    {
        internal SqlUnaryExpression(ExpressionType operatorType, SqlExpression operand, Type type, bool nullable, bool clrOnly = false)
            : base(type, nullable)
        {
            OperatorType = operatorType;
            Operand = operand;
            IsClrConversion = clrOnly;
        }
        internal ExpressionType OperatorType { get; }
        internal SqlExpression Operand { get; }
        internal bool IsClrConversion { get; }
        internal SqlUnaryExpression Update(SqlExpression operand) => ReferenceEquals(operand, Operand) ? this
            : new SqlUnaryExpression(OperatorType, operand, Type, IsNullable, IsClrConversion);
        protected override Expression VisitChildren(ExpressionVisitor visitor) => Update((SqlExpression)visitor.Visit(Operand));
    }
}
