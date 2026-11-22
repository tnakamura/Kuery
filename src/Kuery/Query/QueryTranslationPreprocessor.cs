using System.Linq;
using System.Linq.Expressions;

namespace Kuery.Query
{
    /// <summary>Normalizes supported LINQ expression patterns before relational translation.</summary>
    internal sealed class QueryTranslationPreprocessor
    {
        /// <summary>Rewrites supported LINQ aliases and directly invoked lambdas.</summary>
        /// <param name="query">The query expression or SELECT to process.</param>
        /// <returns>The expression ready for relational translation.</returns>
        internal Expression Process(Expression query)
        {
            return new QueryableMethodNormalizingExpressionVisitor().Visit(query);
        }

        private sealed class QueryableMethodNormalizingExpressionVisitor : ExpressionVisitor
        {
            /// <summary>Keeps normalized query extension nodes intact during method rewriting.</summary>
            /// <param name="node">The expression to visit.</param>
            /// <returns>The visited expression.</returns>
            protected override Expression VisitExtension(Expression node) => node;

            /// <summary>Rewrites supported shorthand ordering methods to standard ordering calls.</summary>
            /// <param name="node">The method call to visit.</param>
            /// <returns>The visited expression.</returns>
            protected override Expression VisitMethodCall(MethodCallExpression node)
            {
                var visited = (MethodCallExpression)base.VisitMethodCall(node);
                if (visited.Method.DeclaringType != typeof(Queryable)
                    || (visited.Method.Name != "Order" && visited.Method.Name != "OrderDescending"))
                    return visited;

                if (visited.Arguments.Count != 1) throw RelationalQueryableMethodTranslatingExpressionVisitor.Unsupported(visited);
                var elementType = visited.Method.GetGenericArguments()[0];
                var element = Expression.Parameter(elementType, "element");
                return Expression.Call(typeof(Queryable),
                    visited.Method.Name == "Order" ? nameof(Queryable.OrderBy) : nameof(Queryable.OrderByDescending),
                    new[] { elementType, elementType }, visited.Arguments[0], Expression.Quote(Expression.Lambda(element, element)));
            }

            /// <summary>Inlines a directly invoked lambda into the query expression.</summary>
            /// <param name="node">The lambda invocation to visit.</param>
            /// <returns>The visited expression.</returns>
            protected override Expression VisitInvocation(InvocationExpression node)
            {
                var expression = RelationalQueryableMethodTranslatingExpressionVisitor.StripQuote(node.Expression);
                if (expression is LambdaExpression lambda)
                    return Visit(RelationalQueryableMethodTranslatingExpressionVisitor.Bind(lambda, node.Arguments.ToArray()));
                throw RelationalQueryableMethodTranslatingExpressionVisitor.Unsupported(node);
            }
        }
    }
}
