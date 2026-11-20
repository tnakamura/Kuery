using System;

namespace Kuery
{
    /// <summary>Declares the string discriminator column of a query-only single-table hierarchy.</summary>
    [AttributeUsage(AttributeTargets.Class, Inherited = false)]
    public sealed class DiscriminatorAttribute : Attribute
    {
        public DiscriminatorAttribute(string columnName)
        {
            if (string.IsNullOrWhiteSpace(columnName)) throw new ArgumentException("A discriminator column is required.", nameof(columnName));
            ColumnName = columnName;
        }
        public string ColumnName { get; }
    }

    /// <summary>Registers a concrete type and its discriminator value on the hierarchy root.</summary>
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = true, Inherited = false)]
    public sealed class DerivedTypeAttribute : Attribute
    {
        public DerivedTypeAttribute(Type type, string discriminatorValue)
        {
            Type = type ?? throw new ArgumentNullException(nameof(type));
            DiscriminatorValue = discriminatorValue ?? throw new ArgumentNullException(nameof(discriminatorValue));
        }
        public Type Type { get; }
        public string DiscriminatorValue { get; }
    }
}
