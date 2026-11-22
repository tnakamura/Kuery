using System;
using System.Runtime.CompilerServices;

namespace Kuery
{
    /// <summary>Provides common argument validation for Kuery operations.</summary>
    internal static class Requires
    {
        /// <summary>Rejects a null value for a required argument.</summary>
        /// <typeparam name="T">The type of the argument to validate.</typeparam>
        /// <param name="value">The value used by the operation.</param>
        /// <param name="paramName">The argument name to include in a validation exception.</param>
        /// <exception cref="System.ArgumentNullException">The value is null.</exception>
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
