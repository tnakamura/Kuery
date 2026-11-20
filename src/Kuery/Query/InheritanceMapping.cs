using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace Kuery.Query
{
    // Read-side metadata is deliberately separate from TableMapping and existing CRUD mapping.
    internal sealed class InheritanceMapping
    {
        static readonly ConcurrentDictionary<Type, Lazy<InheritanceMapping>> Cache = new ConcurrentDictionary<Type, Lazy<InheritanceMapping>>();
        internal static InheritanceMapping Find(Type type) => Cache.GetOrAdd(type, t => new Lazy<InheritanceMapping>(() => Create(t))).Value;
        internal TableMapping Root { get; private set; }
        internal string DiscriminatorColumn { get; private set; }
        internal IReadOnlyList<Entry> Entries { get; private set; }
        internal IReadOnlyList<TableMapping.Column> Columns { get; private set; }
        internal sealed class Entry
        {
            internal TableMapping Mapping;
            internal string Value;
        }

        static InheritanceMapping Create(Type requested)
        {
            var root = requested;
            while (root != null && !root.IsDefined(typeof(DiscriminatorAttribute), false)) root = root.BaseType;
            if (root == null) return null;
            if (root.BaseType != null && Find(root.BaseType) != null) throw new NotSupportedException("Nested discriminator roots are not supported: " + root);
            var discriminator = root.GetCustomAttribute<DiscriminatorAttribute>(false).ColumnName;
            var registrations = root.GetCustomAttributes<DerivedTypeAttribute>(false).ToArray();
            foreach (var registration in registrations)
            {
                var type = registration.Type;
                if (!root.IsAssignableFrom(type) || type.IsAbstract || type.ContainsGenericParameters || type.GetConstructor(Type.EmptyTypes) == null)
                    throw new NotSupportedException("TPH types must be concrete, assignable and have a public parameterless constructor: " + type);
            }
            if (!root.IsAbstract && registrations.All(a => a.Type != root))
                throw new NotSupportedException("Register the concrete TPH root with a discriminator value: " + root);
            var entries = registrations.OrderBy(a => a.DiscriminatorValue, StringComparer.Ordinal)
                .Select(a => new Entry { Mapping = SqlMapper.GetMapping(a.Type), Value = a.DiscriminatorValue }).ToArray();
            if (entries.Length == 0 || entries.Select(e => e.Value).Distinct(StringComparer.Ordinal).Count() != entries.Length
                || entries.Select(e => e.Mapping.MappedType).Distinct().Count() != entries.Length)
                throw new NotSupportedException("TPH requires distinct registered types and discriminator values: " + root);
            foreach (var entry in entries)
            {
                var type = entry.Mapping.MappedType;
                var table = type.GetCustomAttribute<TableAttribute>(false);
                if (type != root && table != null && table.Name != SqlMapper.GetMapping(root).TableName)
                    throw new NotSupportedException("TPH types must use the root table: " + type);
            }
            if (!requested.IsAbstract && entries.All(e => e.Mapping.MappedType != requested))
                throw new NotSupportedException("Register every concrete TPH type, including a concrete root: " + requested);
            var columns = new Dictionary<string, TableMapping.Column>(StringComparer.Ordinal);
            foreach (var column in new[] { SqlMapper.GetMapping(root) }.Concat(entries.Select(e => e.Mapping)).SelectMany(m => m.Columns))
            {
                if (columns.TryGetValue(column.PropertyName, out var existing))
                {
                    if (existing.Name != column.Name || existing.PropertyInfo.PropertyType != column.PropertyInfo.PropertyType || existing.StoreAsText != column.StoreAsText)
                        throw new NotSupportedException("Incompatible TPH property: " + column.PropertyName);
                }
                else
                {
                    if (columns.Values.Any(c => string.Equals(c.Name, column.Name, StringComparison.OrdinalIgnoreCase)))
                        throw new NotSupportedException("TPH columns cannot map different properties to the same column: " + column.Name);
                    columns.Add(column.PropertyName, column);
                }
            }
            if (columns.Values.Any(c => string.Equals(c.Name, discriminator, StringComparison.OrdinalIgnoreCase) && c.PropertyInfo.PropertyType != typeof(string)))
                throw new NotSupportedException("The TPH discriminator must be a string column: " + discriminator);
            var rootMapping = SqlMapper.GetMapping(root);
            if (entries.Any(e => e.Mapping.PK?.Name != rootMapping.PK?.Name))
                throw new NotSupportedException("TPH types must share the root primary key.");
            return new InheritanceMapping { Root = rootMapping, DiscriminatorColumn = discriminator, Entries = entries, Columns = columns.Values.ToArray() };
        }
    }
}
