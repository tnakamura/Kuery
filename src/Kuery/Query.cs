﻿using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data.Common;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.InteropServices;
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
        internal static Type GetElementType(Type sequenceType)
        {
            var ienumerableType = FindIEnumerable(sequenceType);
            if (ienumerableType == null)
            {
                return sequenceType;
            }
            else
            {
                return ienumerableType.GetGenericArguments()[0];
            }
        }

        private static Type FindIEnumerable(Type sequenceType)
        {
            if (sequenceType == null || sequenceType == typeof(string))
            {
                return null;
            }

            if (sequenceType.IsArray)
            {
                return typeof(IEnumerable<>).MakeGenericType(sequenceType.GetElementType());
            }

            if (sequenceType.IsGenericType)
            {
                foreach (var arg in sequenceType.GetGenericArguments())
                {
                    var ienumerableType = typeof(IEnumerable<>).MakeGenericType(arg);
                    if (ienumerableType.IsAssignableFrom(sequenceType))
                    {
                        return ienumerableType;
                    }
                }
            }

            var interfaceTypes = sequenceType.GetInterfaces();

            if (interfaceTypes != null && interfaceTypes.Length > 0)
            {
                foreach (var interfaceType in interfaceTypes)
                {
                    var ienumerableType = FindIEnumerable(interfaceType);
                    if (ienumerableType != null)
                    {
                        return ienumerableType;
                    }
                }
            }

            if (sequenceType.BaseType != null && sequenceType.BaseType != typeof(object))
            {
                return FindIEnumerable(sequenceType.BaseType);
            }

            return null;
        }
    }

    internal class QueryTranslator : ExpressionVisitor
    {
        private StringBuilder sb;
        private ParameterExpression row;
        private ColumnProjection projection;

        internal QueryTranslator()
        {
        }

        internal TranslateResult Translate(Expression node)
        {
            sb = new StringBuilder();
            row = Expression.Parameter(typeof(ProjectionRow), nameof(row));
            Visit(node);
            return new TranslateResult
            {
                CommandText = sb.ToString(),
                Projector = projection != null
                ? Expression.Lambda(projection.Selector, row)
                : null,
            };
        }

        private static Expression StripQuotes(Expression node)
        {
            while (node.NodeType == ExpressionType.Quote)
            {
                node = ((UnaryExpression)node).Operand;
            }
            return node;
        }

        /// <inheritdoc/>
        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            if (node.Method.DeclaringType == typeof(Queryable))
            {
                if (node.Method.Name == nameof(Queryable.Where))
                {
                    sb.Append("SELECT * FROM (");
                    Visit(node.Arguments[0]);
                    sb.Append(") AS T WHERE ");
                    var lambda = (LambdaExpression)StripQuotes(node.Arguments[1]);
                    Visit(lambda.Body);
                    return node;
                }
                else if (node.Method.Name == nameof(Queryable.Select))
                {
                    var lambda = (LambdaExpression)StripQuotes(node.Arguments[1]);
                    var projection = new ColumnProjector()
                        .ProjectColumns(lambda.Body, row);
                    sb.Append("SELECT ");
                    sb.Append(projection.Columns);
                    sb.Append(" FROM (");
                    Visit(node.Arguments[0]);
                    sb.Append(") AS T ");
                    this.projection = projection;
                    return node;
                }
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
            => Translate(expression).CommandText;

        /// <inheritdoc/>
        public override object Execute(Expression expression)
        {
            var result = Translate(expression);
            var command = connection.CreateCommand();
            command.CommandText = result.CommandText;
            var reader = command.ExecuteReader();
            var elementType = TypeSystem.GetElementType(expression.Type);

            if (result.Projector != null)
            {
                var projector = result.Projector.Compile();
                return Activator.CreateInstance(
                    type: typeof(ProjectionReader<>).MakeGenericType(elementType),
                    bindingAttr: BindingFlags.Instance | BindingFlags.NonPublic,
                    binder: null,
                    args: new object[] { reader, projector },
                    culture: null);
            }
            else
            {
                return Activator.CreateInstance(
                    type: typeof(ObjectReader<>).MakeGenericType(elementType),
                    bindingAttr: BindingFlags.Instance | BindingFlags.NonPublic,
                    binder: null,
                    args: new object[] { reader },
                    culture: null);
            }
        }

        private TranslateResult Translate(Expression expression)
        {
            expression = Evaluator.PartialEval(expression);
            return new QueryTranslator().Translate(expression);
        }
    }

    internal static class Evaluator
    {
        internal static Expression PartialEval(
            Expression expression,
            Func<Expression, bool> fnCanBeEvaluated)
        {
            return new SubtreeEvaluator(new Nominator(fnCanBeEvaluated).Nominate(expression))
                .Eval(expression);
        }

        internal static Expression PartialEval(Expression expression)
            => PartialEval(expression, CanBeEvaluatedLocally);

        private static bool CanBeEvaluatedLocally(Expression expression)
            => expression.NodeType != ExpressionType.Parameter;

        class SubtreeEvaluator : ExpressionVisitor
        {
            HashSet<Expression> candidates;

            internal SubtreeEvaluator(HashSet<Expression> candidates)
            {
                this.candidates = candidates;
            }

            internal Expression Eval(Expression expression)
            {
                return Visit(expression);
            }

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

            private Expression Evaluate(Expression node)
            {
                if (node.NodeType == ExpressionType.Constant)
                {
                    return node;
                }
                var lambda = Expression.Lambda(node);
                var fn = lambda.Compile();
                return Expression.Constant(fn.DynamicInvoke(null), node.Type);
            }
        }

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
                    }

                    cannotBeEvaluated |= saveCannotBeEvaluated;
                }

                return node;
            }
        }
    }

    public abstract class ProjectionRow
    {
        public abstract object GetValue(int index);
    }

    internal class ColumnProjection
    {
        internal string Columns;
        internal Expression Selector;
    }

    internal class ColumnProjector : ExpressionVisitor
    {
        StringBuilder sb;
        int iColumn;
        ParameterExpression row;
        static MethodInfo miGetValue;

        internal ColumnProjector()
        {
            if (miGetValue == null)
            {
                miGetValue = typeof(ProjectionRow).GetMethod(nameof(ProjectionRow.GetValue));
            }
        }

        internal ColumnProjection ProjectColumns(
            Expression expression,
            ParameterExpression row)
        {
            sb = new StringBuilder();
            this.row = row;
            var selector = Visit(expression);
            return new ColumnProjection
            {
                Columns = sb.ToString(),
                Selector = selector,
            };
        }

        protected override Expression VisitMember(MemberExpression node)
        {
            if (node.Expression != null && node.Expression.NodeType == ExpressionType.Parameter)
            {
                if (sb.Length > 0)
                {
                    sb.Append(", ");
                }
                sb.Append(node.Member.Name);
                return Expression.Convert(
                    expression: Expression.Call(
                        row,
                        miGetValue,
                        Expression.Constant(iColumn++)),
                    type: node.Type);
            }
            else
            {
                return base.VisitMember(node);
            }
        }
    }

    internal class TranslateResult
    {
        internal string CommandText;
        internal LambdaExpression Projector;
    }

    internal class ProjectionReader<T> : IEnumerable<T>, IEnumerable
    {
        Enumerator enumerator;

        internal ProjectionReader(DbDataReader reader, Func<ProjectionRow, T> projector)
        {
            enumerator = new Enumerator(reader, projector);
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

        class Enumerator : ProjectionRow, IEnumerator<T>, IEnumerator, IDisposable
        {
            DbDataReader reader;
            T current;
            Func<ProjectionRow, T> projector;

            internal Enumerator(DbDataReader reader, Func<ProjectionRow, T> projector)
            {
                this.reader = reader;
                this.projector = projector;
            }

            public override object GetValue(int index)
            {
                if (index >= 0)
                {
                    if (reader.IsDBNull(index))
                    {
                        return null;
                    }
                    else
                    {
                        return reader.GetValue(index);
                    }
                }
                throw new IndexOutOfRangeException();
            }

            public T Current => current;

            object IEnumerator.Current => current;

            public bool MoveNext()
            {
                if (reader.Read())
                {
                    current = projector(this);
                    return true;
                }
                return false;
            }

            public void Reset() { }

            public void Dispose() => reader.Dispose();
        }
    }

    internal enum DbExpressionType
    {
        Table = 1000,
        Column,
        Select,
        Projection,
    }

    internal class TableExpression : Expression
    {
        internal TableExpression(Type type, string alias, string name)
            : base()
        {
            Type = type;
            Alias = alias;
            Name = name;
        }

        public override ExpressionType NodeType => (ExpressionType)DbExpressionType.Table;

        public override Type Type { get; }

        internal string Alias { get; }

        internal string Name { get; }
    }

    internal class ColumnExpression : Expression
    {
        internal ColumnExpression(Type type, string alias, string name, int ordinal)
            : base()
        {
            Type = type;
            Alias = alias;
            Name = name;
            Ordinal = ordinal;
        }

        public override ExpressionType NodeType => (ExpressionType)DbExpressionType.Column;

        public override Type Type { get; }

        internal string Alias { get; }

        internal string Name { get; }

        internal int Ordinal { get; }
    }

    internal class ColumnDeclaration
    {
        internal ColumnDeclaration(string name, Expression expression)
        {
            Name = name;
            Expression = expression;
        }

        internal string Name { get; }

        internal Expression Expression { get; }
    }

    internal class SelectExpression : Expression
    {
        internal SelectExpression(
            Type type,
            string alias,
            IEnumerable<ColumnDeclaration> columns,
            Expression from,
            Expression where)
            : base()
        {
            Type = type;
            Alias = alias;
            Columns = columns as ReadOnlyCollection<ColumnDeclaration>;
            if (Columns == null)
            {
                Columns = new List<ColumnDeclaration>(columns).AsReadOnly();
            }
            From = from;
            Where = where;
        }

        public override ExpressionType NodeType => (ExpressionType)DbExpressionType.Select;

        public override Type Type { get; }

        internal string Alias { get; }

        internal ReadOnlyCollection<ColumnDeclaration> Columns { get; }

        internal Expression From { get; }

        internal Expression Where { get; }
    }

    internal class ProjectionExpression : Expression
    {
        internal ProjectionExpression(SelectExpression source, Expression projector)
        {
            Type = projector.Type;
            Source = source;
            Projector = projector;
        }

        public override ExpressionType NodeType => (ExpressionType)DbExpressionType.Projection;

        public override Type Type { get; }

        internal SelectExpression Source { get; }

        internal Expression Projector { get; }
    }

    internal class DbExpressionVisitor : ExpressionVisitor
    {
        public override Expression Visit(Expression node)
        {
            if (node == null)
            {
                return null;
            }

            switch ((DbExpressionType)node.NodeType)
            {
                case DbExpressionType.Table:
                    return VisitTable((TableExpression)node);
                case DbExpressionType.Column:
                    return VisitColumn((ColumnExpression)node);
                case DbExpressionType.Select:
                    return VisitSelect((SelectExpression)node);
                case DbExpressionType.Projection:
                    return VisitProjection((ProjectionExpression)node);
                default:
                    return base.Visit(node);
            }
        }

        protected virtual Expression VisitTable(TableExpression node) => node;

        protected virtual Expression VisitColumn(ColumnExpression node) => node;

        protected virtual Expression VisitSelect(SelectExpression node)
        {
            var from = VisitSource(node.From);
            var where = Visit(node.Where);
            var columns = VisitColumnDeclarations(node.Columns);

            if (from != node.From || where != node.Where || columns != node.Columns)
            {
                return new SelectExpression(node.Type, node.Alias, columns, from, where);
            }

            return node;
        }

        protected virtual Expression VisitSource(Expression node) => Visit(node);

        protected virtual Expression VisitProjection(ProjectionExpression node)
        {
            var source = (SelectExpression)Visit(node.Source);
            var projector = Visit(node.Projector);

            if (source != node.Source || projector != node.Projector)
            {
                return new ProjectionExpression(source, projector);
            }

            return node;
        }

        protected ReadOnlyCollection<ColumnDeclaration> VisitColumnDeclarations(ReadOnlyCollection<ColumnDeclaration> columns)
        {
            List<ColumnDeclaration> alternate = null;

            for (var i = 0; i < columns.Count; i++)
            {
                var column = columns[i];
                var e = Visit(column.Expression);

                if (alternate == null && e != column.Expression)
                {
                    alternate = columns.Take(i).ToList();
                }

                if (alternate != null)
                {
                    alternate.Add(new ColumnDeclaration(column.Name, e));
                }
            }

            if (alternate != null)
            {
                return alternate.AsReadOnly();
            }

            return columns;
        }
    }

    internal class ProjectionBuilder : DbExpressionVisitor
    {
        private ParameterExpression row;

        private static MethodInfo miGetValue;

        internal ProjectionBuilder() : base()
        {
            if (miGetValue == null)
            {
                miGetValue = typeof(ProjectionRow).GetMethod(nameof(ProjectionRow.GetValue));
            }
        }

        internal LambdaExpression Build(Expression node)
        {
            row = Expression.Parameter(typeof(ProjectionRow), nameof(row));
            var body = Visit(node);
            return Expression.Lambda(body, row);
        }

        protected override Expression VisitColumn(ColumnExpression node)
        {
            return Expression.Convert(
                expression: Expression.Call(
                    instance: row,
                    method: miGetValue,
                    arguments: Expression.Constant(node.Ordinal)),
                type: node.Type);
        }
    }
}