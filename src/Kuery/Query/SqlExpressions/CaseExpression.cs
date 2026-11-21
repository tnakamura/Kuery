using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace Kuery.Query.SqlExpressions
{
    internal sealed class CaseExpression : SqlExpression
    {
        internal CaseExpression(Type type, bool nullable, IReadOnlyList<CaseWhenClause> whenClauses, SqlExpression elseResult)
            : base(type, nullable)
        {
            WhenClauses = whenClauses;
            ElseResult = elseResult;
        }
        internal IReadOnlyList<CaseWhenClause> WhenClauses { get; }
        internal SqlExpression ElseResult { get; }
        internal CaseExpression Update(IReadOnlyList<CaseWhenClause> clauses, SqlExpression elseResult)
            => WhenClauses.SequenceEqual(clauses) && ReferenceEquals(elseResult, ElseResult) ? this
                : new CaseExpression(Type, IsNullable, clauses, elseResult);
        /// <summary>Visits the CASE conditions, results, and fallback expression.</summary>
        /// <param name="visitor">The visitor to apply to the child expressions.</param>
        /// <returns>This expression, or an expression updated with the visited children.</returns>
        protected override Expression VisitChildren(ExpressionVisitor visitor)
            => Update(WhenClauses.Select(c => c.Update((SqlExpression)visitor.Visit(c.Test), (SqlExpression)visitor.Visit(c.Result))).ToArray(),
                (SqlExpression)visitor.Visit(ElseResult));
    }
}
