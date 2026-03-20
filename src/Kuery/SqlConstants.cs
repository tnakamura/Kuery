namespace Kuery
{
    internal static class SqlConstants
    {
        // MySQL requires LIMIT when OFFSET is used; 2^64-1 is a common "effectively no limit" value.
        internal const string MySqlOffsetWithoutLimitMax = "18446744073709551615";
    }
}
