using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.Common;
using System.Dynamic;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Security;
using System.Text;

namespace Kuery
{
    public class Query<T> : IQueryable<T>,
        IQueryable,
        IEnumerable<T>,
        IEnumerable,
        IOrderedQueryable<T>,
        IOrderedQueryable
    {
        QueryProvider provider;

        Expression expression;

        public Query(QueryProvider provider)
        {
            Requires.NotNull(provider, nameof(provider));
            this.provider = provider;
            expression = Expression.Constant(this);
        }

        public Query(QueryProvider provider, Expression expression)
        {
            Requires.NotNull(provider, nameof(provider));
            Requires.NotNull(expression, nameof(expression));
            this.provider = provider;
            this.expression = expression;
        }

        /// <inheritdoc/>
        Expression IQueryable.Expression => expression;

        /// <inheritdoc/>
        Type IQueryable.ElementType => typeof(T);

        /// <inheritdoc/>
        IQueryProvider IQueryable.Provider => provider;

        /// <inheritdoc/>
        public IEnumerator<T> GetEnumerator()
            => ((IEnumerable<T>)provider.Execute(expression)).GetEnumerator();

        /// <inheritdoc/>
        IEnumerator IEnumerable.GetEnumerator()
            => ((IEnumerable)provider.Execute(expression)).GetEnumerator();

        /// <inheritdoc/>
        public override string ToString()
            => provider.GetQueryText(expression);
    }

    public abstract class QueryProvider : IQueryProvider
    {
        protected QueryProvider() { }

        /// <inheritdoc/>
        IQueryable<TElement> IQueryProvider.CreateQuery<TElement>(Expression expression)
            => new Query<TElement>(this, expression);

        /// <inheritdoc/>
        IQueryable IQueryProvider.CreateQuery(Expression expression)
        {
            var elementType = TypeSystem.GetElementType(expression.Type);
            try
            {
                return (IQueryable)Activator.CreateInstance(
                    type: typeof(Query<>).MakeGenericType(elementType),
                    args: new object[] { this, expression });
            }
            catch (TargetInvocationException ex)
            {
                throw ex.InnerException;
            }
        }

        /// <inheritdoc/>
        TResult IQueryProvider.Execute<TResult>(Expression expression)
            => (TResult)Execute(expression);

        /// <inheritdoc/>
        object IQueryProvider.Execute(Expression expression)
            => Execute(expression);

        public abstract string GetQueryText(Expression expression);

        public abstract object Execute(Expression expression);
    }

    internal static class TypeSystem
    {
        internal static Type GetElementType(Type seqType)
        {
            var ienum = FindIEnumerable(seqType);
            if (ienum == null)
            {
                return seqType;
            }
            else
            {
                return ienum.GetGenericArguments()[0];
            }
        }

        private static Type FindIEnumerable(Type seqType)
        {
            if (seqType == null || seqType == typeof(string))
            {
                return null;
            }

            if (seqType.IsArray)
            {
                return typeof(IEnumerable<>).MakeGenericType(seqType.GetElementType());
            }

            if (seqType.IsGenericType)
            {
                foreach (var arg in seqType.GetGenericArguments())
                {
                    var ienum = typeof(IEnumerable<>).MakeGenericType(arg);
                    if (ienum.IsAssignableFrom(seqType))
                    {
                        return ienum;
                    }
                }
            }

            var ifaces = seqType.GetInterfaces();

            if (ifaces != null && ifaces.Length > 0)
            {
                foreach (var iface in ifaces)
                {
                    var ienum = FindIEnumerable(iface);
                    if (ienum != null)
                    {
                        return ienum;
                    }
                }
            }

            if (seqType.BaseType != null && seqType.BaseType != typeof(object))
            {
                return FindIEnumerable(seqType.BaseType);
            }

            return null;
        }
    }

    internal class QueryTranslator : ExpressionVisitor
    {
        private StringBuilder sb;

        internal QueryTranslator()
        {
        }

        internal string Translate(Expression expression)
        {
            sb = new StringBuilder();
            Visit(expression);
            return sb.ToString();
        }

        private static Expression StripQuotes(Expression expression)
        {
            while (expression.NodeType == ExpressionType.Quote)
            {
                expression = ((UnaryExpression)expression).Operand;
            }
            return expression;
        }

        /// <inheritdoc/>
        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            if (node.Method.DeclaringType == typeof(Queryable) &&
                node.Method.Name == nameof(Queryable.Where))
            {
                sb.Append("SELECT * FROM (");
                Visit(node.Arguments[0]);
                sb.Append(") AS T WHERE ");
                var lambda = (LambdaExpression)StripQuotes(node.Arguments[1]);
                Visit(lambda.Body);
                return node;
            }

            throw new NotSupportedException($"The method '{node.Method.Name}' is not supported");
        }

        /// <inheritdoc/>
        protected override Expression VisitUnary(UnaryExpression node)
        {
            switch (node.NodeType)
            {
                case ExpressionType.Not:
                    sb.Append(" NOT ");
                    Visit(node.Operand);
                    break;
                default:
                    throw new NotSupportedException($"The unary operator '{node.NodeType}' is not supported");
            }
            return node;
        }

        protected override Expression VisitBinary(BinaryExpression node)
        {
            sb.Append("(");

            Visit(node.Left);

            switch (node.NodeType)
            {
                case ExpressionType.And:
                    sb.Append(" AND ");
                    break;
                case ExpressionType.Or:
                    sb.Append(" OR ");
                    break;
                case ExpressionType.Equal:
                    sb.Append(" = ");
                    break;
                case ExpressionType.NotEqual:
                    sb.Append(" <> ");
                    break;
                case ExpressionType.LessThan:
                    sb.Append(" < ");
                    break;
                case ExpressionType.LessThanOrEqual:
                    sb.Append(" <= ");
                    break;
                case ExpressionType.GreaterThan:
                    sb.Append(" > ");
                    break;
                case ExpressionType.GreaterThanOrEqual:
                    sb.Append(" >= ");
                    break;
                default:
                    throw new NotSupportedException($"The binary operator '{node.NodeType}' is not supported");
            }

            Visit(node.Right);

            sb.Append(")");

            return node;
        }

        /// <inheritdoc/>
        protected override Expression VisitConstant(ConstantExpression node)
        {
            var q = node.Value as IQueryable;

            if (q != null)
            {
                sb.Append("SELECT * FROM ");
                sb.Append(q.ElementType.Name);
            }
            else if (node.Value == null)
            {
                sb.Append("NULL");
            }
            else
            {
                switch (Type.GetTypeCode(node.Value.GetType()))
                {
                    case TypeCode.Boolean:
                        sb.Append((bool)node.Value);
                        break;
                    case TypeCode.String:
                        sb.Append("'");
                        sb.Append(node.Value);
                        sb.Append("'");
                        break;
                    case TypeCode.Object:
                        throw new NotSupportedException(
                            $"The constant for '{node.Value}' is not supported");
                    default:
                        sb.Append(node.Value);
                        break;
                }
            }
            return node;
        }

        /// <inheritdoc/>
        protected override Expression VisitMember(MemberExpression node)
        {
            if (node.Expression != null && node.Expression.NodeType == ExpressionType.Parameter)
            {
                sb.Append(node.Member.Name);
                return node;
            }
            throw new NotSupportedException(
                $"The member '{node.Member.Name}' is not supported");
        }
    }

    internal class ObjectReader<T> : IEnumerable<T>, IEnumerable
        where T : class, new()
    {
        Enumerator enumerator;

        internal ObjectReader(DbDataReader reader)
        {
            enumerator = new Enumerator(reader);
        }

        public IEnumerator<T> GetEnumerator()
        {
            var e = enumerator;
            if (e == null)
            {
                throw new InvalidOperationException("Cannot enumerate more than once");
            }
            enumerator = null;
            return e;
        }

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();


        private class Enumerator : IEnumerator<T>, IEnumerator
        {
            DbDataReader reader;
            FieldInfo[] fields;
            int[] fieldLookup;
            T current;

            internal Enumerator(DbDataReader reader)
            {
                this.reader = reader;
                fields = typeof(T).GetFields();
            }

            public T Current => current;

            object IEnumerator.Current => current;

            public bool MoveNext()
            {
                if (reader.Read())
                {
                    if (fieldLookup == null)
                    {
                        InitFieldLookup();
                    }

                    var instance = new T();

                    for (var i = 0; i < fields.Length; i++)
                    {
                        var index = fieldLookup[i];
                        if (index >= 0)
                        {
                            var fi = fields[index];
                            if (reader.IsDBNull(index))
                            {
                                fi.SetValue(instance, null);
                            }
                            else
                            {
                                fi.SetValue(instance, reader.GetValue(index));
                            }
                        }
                    }

                    current = instance;

                    return true;
                }

                return false;
            }

            public void Reset() { }

            public void Dispose() => reader.Dispose();

            private void InitFieldLookup()
            {
                var map = new Dictionary<string, int>(StringComparer.InvariantCultureIgnoreCase);
                for (var i = 0; i < reader.FieldCount; i++)
                {
                    map.Add(reader.GetName(i), i);
                }

                fieldLookup = new int[fields.Length];
                for (var i = 0; i < fields.Length; i++)
                {
                    if (map.TryGetValue(fields[i].Name, out var index))
                    {
                        fieldLookup[i] = index;
                    }
                    else
                    {
                        fieldLookup[i] = -1;
                    }
                }
            }
        }
    }

    public class DbQueryProvider : QueryProvider
    {
        private readonly DbConnection connection;

        public DbQueryProvider(DbConnection connection)
        {
            this.connection = connection;
        }

        /// <inheritdoc/>
        public override string GetQueryText(Expression expression)
            => Translate(expression);

        /// <inheritdoc/>
        public override object Execute(Expression expression)
        {
            var cmd = connection.CreateCommand();
            cmd.CommandText = Translate(expression);
            var reader = cmd.ExecuteReader();
            var elementType = TypeSystem.GetElementType(expression.Type);
            return Activator.CreateInstance(
                type: typeof(ObjectReader<>).MakeGenericType(elementType),
                bindingAttr: BindingFlags.Instance | BindingFlags.NonPublic,
                binder: null,
                args: new object[] { reader },
                culture: null);
        }

        private string Translate(Expression expression)
            => new QueryTranslator().Translate(expression);
    }
}
