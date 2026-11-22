using System;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace Kuery
{
    /// <summary>Pairs a mapped property selector with the expression that supplies its update value.</summary>
    internal sealed class SetPropertyCall
    {
        /// <summary>Creates a property assignment from the target member and value expression.</summary>
        /// <param name="propertyExpression">The lambda selecting the mapped property to set.</param>
        /// <param name="valueExpression">The lambda computing the value to write for each selected row.</param>
        /// <exception cref="System.ArgumentNullException">The property selector or value expression is null.</exception>
        internal SetPropertyCall(LambdaExpression propertyExpression, LambdaExpression valueExpression)
        {
            PropertyExpression = propertyExpression ?? throw new ArgumentNullException(nameof(propertyExpression));
            ValueExpression = valueExpression ?? throw new ArgumentNullException(nameof(valueExpression));
        }

        /// <summary>Gets the lambda that selects the mapped property to update.</summary>
        internal LambdaExpression PropertyExpression { get; }

        /// <summary>Gets the lambda that computes the new value for a selected row.</summary>
        internal LambdaExpression ValueExpression { get; }
    }

    /// <summary>Builds the property assignments for a set-based update.</summary>
    /// <typeparam name="TSource">The type of entity being updated.</typeparam>
    public sealed class SetPropertyCalls<TSource>
    {
        readonly IReadOnlyList<SetPropertyCall> _setters;

        /// <summary>Initializes an empty list of property assignments.</summary>
        public SetPropertyCalls()
            : this(Array.Empty<SetPropertyCall>())
        {
        }

        SetPropertyCalls(IReadOnlyList<SetPropertyCall> setters)
        {
            _setters = setters ?? throw new ArgumentNullException(nameof(setters));
        }

        /// <summary>Gets the property assignments in the order they were added.</summary>
        internal IReadOnlyList<SetPropertyCall> Setters => _setters;

        /// <summary>Appends a constant property assignment for all selected rows.</summary>
        /// <typeparam name="TProperty">The CLR type of the mapped property.</typeparam>
        /// <param name="propertyExpression">An expression selecting the mapped property to set.</param>
        /// <param name="valueExpression">The new value to assign to every selected row.</param>
        /// <returns>A new builder containing the previous assignments and the new one.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="propertyExpression"/> is null.</exception>
        public SetPropertyCalls<TSource> SetProperty<TProperty>(
            Expression<Func<TSource, TProperty>> propertyExpression,
            TProperty valueExpression)
        {
            Requires.NotNull(propertyExpression, nameof(propertyExpression));
            var list = new List<SetPropertyCall>(_setters)
            {
                new SetPropertyCall(propertyExpression, Expression.Lambda<Func<TSource, TProperty>>(Expression.Constant(valueExpression, typeof(TProperty)), propertyExpression.Parameters))
            };
            return new SetPropertyCalls<TSource>(list);
        }

        /// <summary>Appends a property assignment computed from each selected row.</summary>
        /// <typeparam name="TProperty">The CLR type of the mapped property.</typeparam>
        /// <param name="propertyExpression">An expression selecting the mapped property to set.</param>
        /// <param name="valueExpression">An expression that computes the new value from the selected row.</param>
        /// <returns>A new builder containing the previous assignments and the new one.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="propertyExpression"/> or <paramref name="valueExpression"/> is null.</exception>
        public SetPropertyCalls<TSource> SetProperty<TProperty>(
            Expression<Func<TSource, TProperty>> propertyExpression,
            Expression<Func<TSource, TProperty>> valueExpression)
        {
            Requires.NotNull(propertyExpression, nameof(propertyExpression));
            Requires.NotNull(valueExpression, nameof(valueExpression));
            var list = new List<SetPropertyCall>(_setters)
            {
                new SetPropertyCall(propertyExpression, valueExpression)
            };
            return new SetPropertyCalls<TSource>(list);
        }
    }
}
