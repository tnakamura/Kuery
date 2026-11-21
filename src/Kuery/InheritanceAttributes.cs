using System;

namespace Kuery
{
    /// <summary>Declares the string discriminator column of a query-only single-table hierarchy.</summary>
    [AttributeUsage(AttributeTargets.Class, Inherited = false)]
    public sealed class DiscriminatorAttribute : Attribute
    {
        /// <summary>Initializes a discriminator mapping for a string column.</summary>
        /// <param name="columnName">The database column containing the string discriminator.</param>
        /// <exception cref="ArgumentException">The column name is null, empty, or whitespace.</exception>
        public DiscriminatorAttribute(string columnName)
        {
            if (string.IsNullOrWhiteSpace(columnName)) throw new ArgumentException("A discriminator column is required.", nameof(columnName));
            ColumnName = columnName;
        }
        /// <summary>Gets the mapped discriminator column name.</summary>
        public string ColumnName { get; }
    }

    /// <summary>Registers a concrete type and its discriminator value on the hierarchy root.</summary>
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = true, Inherited = false)]
    public sealed class DerivedTypeAttribute : Attribute
    {
        /// <summary>Registers a concrete type for the specified discriminator value.</summary>
        /// <param name="type">The concrete CLR type to register in the hierarchy.</param>
        /// <param name="discriminatorValue">The string stored in the discriminator column for this type.</param>
        /// <exception cref="ArgumentNullException"><paramref name="type"/> or <paramref name="discriminatorValue"/> is null.</exception>
        public DerivedTypeAttribute(Type type, string discriminatorValue)
        {
            Type = type ?? throw new ArgumentNullException(nameof(type));
            DiscriminatorValue = discriminatorValue ?? throw new ArgumentNullException(nameof(discriminatorValue));
        }
        /// <summary>Gets the concrete type to materialize for the registered discriminator value.</summary>
        public Type Type { get; }
        /// <summary>Gets the string value that identifies the registered type in the table.</summary>
        public string DiscriminatorValue { get; }
    }
}
