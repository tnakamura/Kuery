namespace Kuery
{
    /// <summary>Holds SQL literal text shared by the command and query builders.</summary>
    internal static class SqlConstants
    {
        // MySQL requires LIMIT when OFFSET is used; 2^64-1 is a common "effectively no limit" value.
        /// <summary>The unsigned maximum used as a MySQL LIMIT when only an offset is requested.</summary>
        internal const string MySqlOffsetWithoutLimitMax = "18446744073709551615";
    }
}
