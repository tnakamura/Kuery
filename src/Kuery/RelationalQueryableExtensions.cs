using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using Kuery.Query;

namespace Kuery
{
    /// <summary>Creates and inspects composable SQL queries and adds tags to their commands.</summary>
    public static class RelationalQueryableExtensions
    {
        /// <summary>Creates a Kuery query over a parameterized SQL SELECT statement.</summary>
        /// <typeparam name="T">The mapped entity or DTO returned by the SELECT statement.</typeparam>
        /// <param name="connection">The connection on which the query will later execute.</param>
        /// <param name="sql">A trusted SQL template whose interpolation arguments are database values.</param>
        /// <returns>A query that can be composed with LINQ before execution.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="connection"/> or <paramref name="sql"/> is null.</exception>
        /// <exception cref="ArgumentException">The template does not start with SELECT, contains a semicolon,
        /// or contains invalid interpolation placeholders.</exception>
        /// <exception cref="NotSupportedException">A template argument is not a supported scalar parameter or
        /// the target uses TPH mapping.</exception>
        /// <remarks>The SELECT must be valid as a derived table for the database and return the required mapped columns.
        /// Put interpolated values outside SQL string literals. Supply identifiers and SQL syntax as trusted template text,
        /// since interpolated arguments become parameters and cannot represent identifiers or SQL fragments.
        /// The caller owns the connection and must open it before execution.</remarks>
        public static IQueryable<T> FromSql<T>(this IDbConnection connection, FormattableString sql)
        {
            Requires.NotNull(connection, nameof(connection));
            Requires.NotNull(sql, nameof(sql));
            ValidateSelect(sql);
            if (InheritanceMapping.Find(typeof(T)) != null)
                throw new NotSupportedException("FromSql does not support TPH mappings.");

            var source = connection.Query<T>();
            var arguments = sql.GetArguments().Select(CreateArgument).ToArray();
            return source.Provider.CreateQuery<T>(new FromSqlQueryRootExpression(typeof(IQueryable<T>), source.Expression, sql.Format, arguments));
        }

        /// <summary>Returns the SQL command text generated for a Kuery query.</summary>
        /// <param name="source">The Kuery query to translate.</param>
        /// <returns>The command text, including tags and placeholders for database parameters.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is null.</exception>
        /// <exception cref="NotSupportedException">The source is not a Kuery query or cannot be translated.</exception>
        /// <remarks>This diagnostic operation may evaluate captured parameters but does not open the connection or read rows.
        /// Parameter values are not embedded in the generated text. SQL explicitly supplied by the caller and query tags are visible.</remarks>
        public static string ToQueryString(this IQueryable source)
            => GetProvider(source).GetQueryString(source.Expression, typeof(IEnumerable<>).MakeGenericType(source.ElementType));

        /// <summary>Adds a comment to the command generated for a Kuery query.</summary>
        /// <typeparam name="T">The element type of the query.</typeparam>
        /// <param name="source">The Kuery query to tag.</param>
        /// <param name="tag">The comment text. Each line is emitted as a separate SQL line comment.</param>
        /// <returns>A query with the same results and the specified comment added to its command text.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="tag"/> is null.</exception>
        /// <exception cref="NotSupportedException"><paramref name="source"/> is not a Kuery query.</exception>
        /// <remarks>Tags accumulate on read queries and are visible in database logs. Use descriptive text that contains no sensitive values.</remarks>
        public static IQueryable<T> TagWith<T>(this IQueryable<T> source, string tag)
        {
            var provider = GetProvider(source);
            Requires.NotNull(tag, nameof(tag));
            return provider.CreateQuery<T>(new QueryTagExpression(source.Expression, tag));
        }

        /// <summary>Adds the caller's source file and line number to the query as a SQL comment.</summary>
        /// <typeparam name="T">The element type of the query.</typeparam>
        /// <param name="source">The Kuery query to tag.</param>
        /// <param name="filePath">The source file path, normally supplied by the compiler.</param>
        /// <param name="lineNumber">The source line number, normally supplied by the compiler.</param>
        /// <returns>A query with a comment that identifies the call site.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is null.</exception>
        /// <exception cref="NotSupportedException"><paramref name="source"/> is not a Kuery query.</exception>
        /// <remarks>The resulting SQL comment can expose the build-time source path in database logs.</remarks>
        public static IQueryable<T> TagWithCallSite<T>(this IQueryable<T> source,
            [CallerFilePath] string filePath = "", [CallerLineNumber] int lineNumber = 0)
            => source.TagWith(filePath + ":" + lineNumber.ToString(CultureInfo.InvariantCulture));

        static KueryQueryProvider GetProvider(IQueryable source)
        {
            Requires.NotNull(source, nameof(source));
            return source.Provider as KueryQueryProvider ?? throw new NotSupportedException("This operation requires a Kuery query.");
        }

        static void ValidateSelect(FormattableString sql)
        {
            var text = sql.Format.TrimStart();
            if (text.Length <= 6 || !text.StartsWith("select", StringComparison.OrdinalIgnoreCase)
                || !char.IsWhiteSpace(text[6]) || text.IndexOf(';') >= 0)
                throw new ArgumentException("Specify a single composable SELECT without semicolons.", nameof(sql));

            try
            {
                // Parse the placeholders before creating the root, without formatting argument values.
                string.Format(CultureInfo.InvariantCulture, sql.Format, new object[sql.ArgumentCount]);
            }
            catch (FormatException ex)
            {
                throw new ArgumentException("Invalid SQL interpolation placeholders.", nameof(sql), ex);
            }
        }

        static Expression CreateArgument(object value)
        {
            if (value == DBNull.Value) value = null;
            var type = value?.GetType() ?? typeof(object);
            if (value != null && !Query.Internal.ExpressionTreeFuncletizer.IsScalar(type) && type != typeof(byte[]))
                throw new NotSupportedException("The SQL interpolation argument is not a supported scalar value: " + type);
            return Expression.Constant(value, type);
        }
    }
}
