using System;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace Kuery.Linq
{
    internal static class Evaluator
    {
        /// <summary>
        /// Performs evaluation and replacement of independent sub-trees
        /// </summary>
        /// <param name="expression">
        /// The root of the expression tree.
        /// </param>
        /// <param name="fnCanBeEvaluated">
        /// A function that decides whether a given expression node can be part of the local function.
        /// </param>
        /// <returns>
        /// A new tree with sub-trees evaluated and replaced.
        /// </returns>
        internal static Expression PartialEval(
            Expression expression,
            Func<Expression, bool> fnCanBeEvaluated)
        {
            return new SubtreeEvaluator(
                new Nominator(fnCanBeEvaluated).Nominate(expression))
                .Eval(expression);
        }

        /// <summary>
        /// Performs evaluation and replacement of independent sub-trees
        /// </summary>
        /// <param name="expression">
        /// The root of the expression tree.
        /// </param>
        /// <returns>
        /// A new tree with sub-trees evaluated and replaced.
        /// </returns>
        internal static Expression PartialEval(Expression expression)
        {
            return PartialEval(expression, CanBeEvaluatedLocally);
        }

        private static bool CanBeEvaluatedLocally(Expression expression)
        {
            return expression.NodeType != ExpressionType.Parameter;
        }

        /// <summary>
        /// Evaluates and replaces sub-trees when first candidate is reached (top-down)
        /// </summary>
        class SubtreeEvaluator : ExpressionVisitor
        {
            private readonly HashSet<Expression> candidates;

            internal SubtreeEvaluator(HashSet<Expression> candidates)
            {
                this.candidates = candidates;
            }

            internal Expression Eval(Expression expression)
            {
                return Visit(expression);
            }

            /// <inheritdoc/>
            public override Expression Visit(Expression node)
            {
                if (node == null)
                {
                    return null;
                }

                if (candidates.Contains(node))
                {
                    return Evaluate(node);
                }

                return base.Visit(node);
            }

            private Expression Evaluate(Expression e)
            {
                if (e.NodeType == ExpressionType.Constant)
                {
                    return e;
                }

                var lambda = Expression.Lambda(e);
                var fn = lambda.Compile();
                return Expression.Constant(
                    value: fn.DynamicInvoke(null),
                    type: e.Type);
            }
        }

        /// <summary>
        /// Performs bottom-up analysis to determine which nodes can possibly
        /// be part of an evaluated sub-tree.
        /// </summary>
        class Nominator : ExpressionVisitor
        {
            Func<Expression, bool> fnCanBeEvaluated;
            HashSet<Expression> candidates;
            bool cannotBeEvaluated;

            internal Nominator(Func<Expression, bool> fnCanBeEvaluated)
            {
                this.fnCanBeEvaluated = fnCanBeEvaluated;
            }

            internal HashSet<Expression> Nominate(Expression expression)
            {
                candidates = new HashSet<Expression>();
                Visit(expression);
                return candidates;
            }

            /// <inheritdoc/>
            public override Expression Visit(Expression node)
            {
                if (node != null)
                {
                    var saveCannotBeEvaluated = cannotBeEvaluated;
                    cannotBeEvaluated = false;
                    base.Visit(node);

                    if (!cannotBeEvaluated)
                    {
                        if (fnCanBeEvaluated(node))
                        {
                            candidates.Add(node);
                        }
                        else
                        {
                            cannotBeEvaluated = true;
                        }

                        cannotBeEvaluated |= saveCannotBeEvaluated;
                    }
                }

                return node;
            }
        }
    }
}
