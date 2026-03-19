using System;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace Kuery
{
    internal sealed class SetPropertyCall
    {
        internal SetPropertyCall(LambdaExpression propertyExpression, LambdaExpression valueExpression)
        {
            PropertyExpression = propertyExpression ?? throw new ArgumentNullException(nameof(propertyExpression));
            ValueExpression = valueExpression ?? throw new ArgumentNullException(nameof(valueExpression));
        }

        internal LambdaExpression PropertyExpression { get; }

        internal LambdaExpression ValueExpression { get; }
    }

    public sealed class SetPropertyCalls<TSource>
    {
        readonly IReadOnlyList<SetPropertyCall> _setters;

        public SetPropertyCalls()
            : this(Array.Empty<SetPropertyCall>())
        {
        }

        SetPropertyCalls(IReadOnlyList<SetPropertyCall> setters)
        {
            _setters = setters ?? throw new ArgumentNullException(nameof(setters));
        }

        internal IReadOnlyList<SetPropertyCall> Setters => _setters;

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
