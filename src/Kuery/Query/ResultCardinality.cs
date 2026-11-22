namespace Kuery.Query
{
    /// <summary>Distinguishes sequence results from required and optional single results.</summary>
    internal enum ResultCardinality
    {
        /// <summary>The query yields a sequence of results.</summary>
        Enumerable,
        /// <summary>The query is expected to produce one result.</summary>
        Single,
        /// <summary>The query produces one result or a default value.</summary>
        SingleOrDefault,
    }
}
