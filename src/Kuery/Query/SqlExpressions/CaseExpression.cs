using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace Kuery.Query.SqlExpressions
{
    /// <summary>Represents a searched SQL CASE expression.</summary>
    internal sealed class CaseExpression : SqlExpression
    {
        /// <summary>Creates a CASE expression with the specified branches.</summary>
        /// <param name="type">The CLR type associated with the value or result.</param>
        /// <param name="nullable">Whether the SQL value can be NULL.</param>
        /// <param name="whenClauses">The WHEN branches in evaluation order.</param>
        /// <param name="elseResult">The SQL result used when no WHEN branch matches.</param>
        internal CaseExpression(Type type, bool nullable, IReadOnlyList<CaseWhenClause> whenClauses, SqlExpression elseResult)
            : base(type, nullable)
        {
            WhenClauses = whenClauses;
            ElseResult = elseResult;
        }
        /// <summary>Gets the ordered tests and results of the CASE expression.</summary>
        internal IReadOnlyList<CaseWhenClause> WhenClauses { get; }
        /// <summary>Gets the result used when none of the WHEN clauses match.</summary>
        internal SqlExpression ElseResult { get; }
        /// <summary>Replaces the WHEN clauses and fallback result.</summary>
        /// <param name="clauses">The replacement WHEN branches in evaluation order.</param>
        /// <param name="elseResult">The SQL result used when no WHEN branch matches.</param>
        /// <returns>This expression if unchanged, or the updated CASE expression.</returns>
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
