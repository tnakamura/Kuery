using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Kuery.Query.SqlExpressions;

namespace Kuery.Query
{
    internal sealed class StructuralTypeProjectionExpression : Expression
    {
        internal StructuralTypeProjectionExpression(TableMapping mapping, IReadOnlyDictionary<string, Expression> members,
            SqlExpression presence = null, InheritanceMapping hierarchy = null, SqlExpression discriminator = null,
            IReadOnlyList<InheritanceMapping.Entry> concreteTypes = null)
        {
            Mapping = mapping; Members = members; Presence = presence; Hierarchy = hierarchy; Discriminator = discriminator;
            ConcreteTypes = concreteTypes ?? hierarchy?.Entries.Where(e => mapping.MappedType.IsAssignableFrom(e.Mapping.MappedType)).ToArray();
        }
        internal TableMapping Mapping { get; }
        internal IReadOnlyDictionary<string, Expression> Members { get; }
        internal SqlExpression Presence { get; }
        internal InheritanceMapping Hierarchy { get; }
        internal SqlExpression Discriminator { get; }
        internal IReadOnlyList<InheritanceMapping.Entry> ConcreteTypes { get; }
        internal StructuralTypeProjectionExpression Update(IReadOnlyDictionary<string, Expression> members, SqlExpression presence, SqlExpression discriminator)
            => new StructuralTypeProjectionExpression(Mapping, members, presence, Hierarchy, discriminator, ConcreteTypes);
        internal StructuralTypeProjectionExpression WithType(Type type)
            => new StructuralTypeProjectionExpression(SqlMapper.GetMapping(type), Members, Presence, Hierarchy, Discriminator,
                ConcreteTypes.Where(e => type.IsAssignableFrom(e.Mapping.MappedType)).ToArray());
        public override Type Type => Mapping.MappedType;
        public override ExpressionType NodeType => ExpressionType.Extension;
        protected override Expression VisitChildren(ExpressionVisitor visitor) => this;
    }
}
