using System;

namespace Kuery
{
    /// <summary>
    /// Provides helper methods for SQL-specific expressions
    /// that can be used in LINQ queries.
    /// These methods are translated to SQL by the query provider
    /// and should not be invoked directly.
    /// </summary>
    public static class KueryFunctions
    {
        /// <summary>
        /// Translates to SQL LIKE expression.
        /// Use SQL LIKE wildcards: % (any characters), _ (single character).
        /// </summary>
        /// <param name="column">The string column to match.</param>
        /// <param name="pattern">The LIKE pattern (e.g., "%abc%", "test_").</param>
        /// <returns>
        /// This method is not intended to be called directly.
        /// It is translated to a SQL LIKE expression by the query provider.
        /// </returns>
        /// <exception cref="NotSupportedException">
        /// Thrown when this method is called outside of a LINQ query context.
        /// </exception>
        public static bool Like(string column, string pattern)
        {
            throw new NotSupportedException(
                "KueryFunctions.Like() can only be used in LINQ queries translated to SQL.");
        }
    }
}
