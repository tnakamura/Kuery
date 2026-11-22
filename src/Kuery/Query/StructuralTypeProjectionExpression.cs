using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Kuery.Query.SqlExpressions;

namespace Kuery.Query
{
    /// <summary>Maps entity properties, discriminator, and optional presence to projected SQL values.</summary>
    internal sealed class StructuralTypeProjectionExpression : Expression
    {
        /// <summary>Creates an entity projection with its mapped values and optional inheritance metadata.</summary>
        /// <param name="mapping">The entity mapping that supplies the table and projected members.</param>
        /// <param name="members">The projected expressions indexed by CLR property name.</param>
        /// <param name="presence">The SQL marker whose null value signals the absence of a row, if applicable.</param>
        /// <param name="hierarchy">The optional table-per-hierarchy metadata.</param>
        /// <param name="discriminator">The SQL expression that supplies the discriminator value, if needed.</param>
        /// <param name="concreteTypes">The concrete registrations permitted for this projection, or null to infer them from the mapping.</param>
        internal StructuralTypeProjectionExpression(TableMapping mapping, IReadOnlyDictionary<string, Expression> members,
            SqlExpression presence = null, InheritanceMapping hierarchy = null, SqlExpression discriminator = null,
            IReadOnlyList<InheritanceMapping.Entry> concreteTypes = null)
        {
            Mapping = mapping; Members = members; Presence = presence; Hierarchy = hierarchy; Discriminator = discriminator;
            ConcreteTypes = concreteTypes ?? hierarchy?.Entries.Where(e => mapping.MappedType.IsAssignableFrom(e.Mapping.MappedType)).ToArray();
        }
        /// <summary>Gets the entity mapping associated with this projection.</summary>
        internal TableMapping Mapping { get; }
        /// <summary>Gets the projected expressions keyed by CLR property name.</summary>
        internal IReadOnlyDictionary<string, Expression> Members { get; }
        /// <summary>Gets the SQL marker whose null value indicates an absent entity, if present.</summary>
        internal SqlExpression Presence { get; }
        /// <summary>Gets the table-per-hierarchy mapping, if one applies.</summary>
        internal InheritanceMapping Hierarchy { get; }
        /// <summary>Gets the SQL expression supplying the string discriminator.</summary>
        internal SqlExpression Discriminator { get; }
        /// <summary>Gets the concrete hierarchy mappings that this projection can materialize.</summary>
        internal IReadOnlyList<InheritanceMapping.Entry> ConcreteTypes { get; }
        /// <summary>Replaces the projected members, presence marker, and discriminator while retaining the entity mapping.</summary>
        /// <param name="members">The projected expressions indexed by CLR property name.</param>
        /// <param name="presence">The SQL marker whose null value signals the absence of a row, if applicable.</param>
        /// <param name="discriminator">The SQL expression that supplies the discriminator value, if needed.</param>
        /// <returns>The new structural projection.</returns>
        internal StructuralTypeProjectionExpression Update(IReadOnlyDictionary<string, Expression> members, SqlExpression presence, SqlExpression discriminator)
            => new StructuralTypeProjectionExpression(Mapping, members, presence, Hierarchy, discriminator, ConcreteTypes);
        /// <summary>Narrows a hierarchy projection to the specified CLR type and its compatible registrations.</summary>
        /// <param name="type">The registered CLR type to project.</param>
        /// <returns>The structural projection with the narrowed type.</returns>
        internal StructuralTypeProjectionExpression WithType(Type type)
            => new StructuralTypeProjectionExpression(SqlMapper.GetMapping(type), Members, Presence, Hierarchy, Discriminator,
                ConcreteTypes.Where(e => type.IsAssignableFrom(e.Mapping.MappedType)).ToArray());
        /// <summary>Gets the CLR type of the mapped entity projection.</summary>
        public override Type Type => Mapping.MappedType;
        /// <summary>Gets the extension node kind used to identify this expression.</summary>
        public override ExpressionType NodeType => ExpressionType.Extension;
        /// <summary>Returns this node because it exposes no visitable child expressions.</summary>
        /// <param name="visitor">The visitor to apply to the child expressions.</param>
        /// <returns>This expression.</returns>
        protected override Expression VisitChildren(ExpressionVisitor visitor) => this;
    }
}
