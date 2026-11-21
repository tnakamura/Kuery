namespace Kuery.Query
{
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
