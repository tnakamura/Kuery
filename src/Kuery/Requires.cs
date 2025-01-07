using System;
using System.Runtime.CompilerServices;

namespace Kuery
{
    internal static class Requires
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void NotNull<T>(T value, string paramName)
        {
            if (value == null)
            {
                throw new ArgumentNullException(paramName: paramName);
            }
        }
    }
}
