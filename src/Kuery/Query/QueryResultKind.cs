using System;
using Kuery.Query.SqlExpressions;

namespace Kuery.Query
{
    /// <summary>Specifies how execution interprets the rows returned by a query.</summary>
    internal enum QueryResultKind
    {
        /// <summary>Materializes all rows as a sequence.</summary>
        Sequence,
        /// <summary>Takes the first row and requires that a row exist.</summary>
        First,
        /// <summary>Takes the first row or returns the default when the result is empty.</summary>
        FirstOrDefault,
        /// <summary>Requires exactly one row.</summary>
        Single,
        /// <summary>Allows zero or one row and defaults an empty result.</summary>
        SingleOrDefault,
        /// <summary>Requires a row at the requested index.</summary>
        ElementAt,
        /// <summary>Returns the row at the requested index or its default value.</summary>
        ElementAtOrDefault,
        /// <summary>Reads a single scalar result.</summary>
        Scalar
    }
}
