using System;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using Kuery.Query;
using Xunit;

namespace Kuery.Tests
{
    public abstract partial class QueryPipelineContract
    {
        IDbConnection OpenHierarchy()
        {
            var connection = Open();
            try
            {
                var dialect = SqlDialectFactory.Create(connection);
                string Q(string name) => dialect.EscapeIdentifier(name);
                connection.Execute("drop table if exists " + Q("kuery_tph_contract"));
                connection.Execute("create table " + Q("kuery_tph_contract") + " (" + Q("Id") + " integer primary key, "
                    + Q("Name") + " varchar(80), " + Q("Kind") + " varchar(40), " + Q("Lives") + " integer null, "
                    + Q("Level") + " integer null, " + Q("Breed") + " varchar(80) null)");
                var table = Q("kuery_tph_contract");
                connection.Execute("insert into " + table + " (" + Q("Id") + ", " + Q("Name") + ", " + Q("Kind") + ", " + Q("Lives") + ", " + Q("Level") + ", " + Q("Breed")
                    + ") values (1, 'Base', 'creature', null, null, null), (2, 'Cat', 'cat', 9, null, null), (3, 'Special', 'special', 7, 5, null), (4, 'Dog', 'dog', null, null, 'Hound')");
                return connection;
            }
            catch { connection.Dispose(); throw; }
        }

        [Fact]
        public async Task TphMaterializesConcreteTypesAndOfTypeIncludesDescendants()
        {
            using var connection = OpenHierarchy();
            var query = connection.Query<Creature>();
            var rows = query.OrderBy(x => x.Id).ToList();
            Assert.IsType<Creature>(rows[0]);
            Assert.Equal(9, Assert.IsType<Cat>(rows[1]).Lives);
            Assert.Equal(5, Assert.IsType<SpecialCat>(rows[2]).Level);
            Assert.Equal("Hound", Assert.IsType<Dog>(rows[3]).Breed);
            Assert.Equal(new[] { 2, 3 }, query.OfType<Cat>().OrderBy(x => x.Id).Select(x => x.Id));
            Assert.Equal(new[] { 9, 7 }, connection.Query<Cat>().OrderBy(x => x.Id).Select(x => x.Lives));
            Assert.Equal(2, await query.OfType<Cat>().CountAsync());
            Assert.IsType<SpecialCat>(await query.OfType<SpecialCat>().FirstAsync());
            Assert.Equal(4, query.OfType<object>().Count());
            Assert.Equal(2, query.OfType<Cat>().Cast<Creature>().Count());
            Assert.Empty(query.OfType<Cat>().Cast<Creature>().OfType<Dog>());
            Assert.Throws<InvalidCastException>(() => query.OrderBy(x => x.Id).Cast<Cat>().ToList());
        }

        [Fact]
        public void TphOfTypePreservesPagingJoinsAndSetOperations()
        {
            using var connection = OpenHierarchy();
            var query = connection.Query<Creature>();
            Assert.Empty(query.OrderBy(x => x.Id).Take(1).OfType<Cat>());
            Assert.Equal(new[] { 2 }, query.OrderBy(x => x.Id).Take(2).OfType<Cat>().Where(x => x.Lives > 0).Select(x => x.Id));
            Assert.Equal(new[] { 2, 3 }, query.OfType<Cat>().OrderBy(x => x.Lives).Reverse().Select(x => x.Id));
            Assert.Equal(2, query.OfType<Cat>().Cast<Creature>().Union(query.OfType<Cat>().Cast<Creature>()).Count());
            Assert.Equal(new[] { typeof(Cat), typeof(SpecialCat) }, query.OfType<Cat>().OrderBy(x => x.Id).Cast<Creature>().ToList().Select(x => x.GetType()));
            Assert.Equal(new[] { typeof(Cat), typeof(SpecialCat), typeof(Dog) }, query.OfType<Cat>().Cast<Creature>()
                .Union(query.OfType<Dog>().Cast<Creature>()).OrderBy(x => x.Id).ToList().Select(x => x.GetType()));
            Assert.Null(query.LeftJoin(query.Take(0), x => x.Id, x => x.Id, (left, right) => right).First());
            Assert.Empty(query.Take(0).DefaultIfEmpty().OfType<Cat>());
            Assert.True(query.Contains(new Cat { Id = 2 }));
            Assert.False(query.OfType<Dog>().Contains(new Dog { Id = 2 }));
            Assert.Throws<NotSupportedException>(() => query.OfType<UnregisteredCreature>().ToList());
            Assert.Throws<NotSupportedException>(() => query.Where(x => x.Id > 0).ExecuteDelete());
            Assert.Throws<NotSupportedException>(() => query.Where(x => x.Id > 0).ExecuteUpdate(s => s.SetProperty(x => x.Name, "updated")));
        }

        [Fact]
        public void UnknownDiscriminatorFailsDuringEntityMaterialization()
        {
            using var connection = OpenHierarchy();
            var dialect = SqlDialectFactory.Create(connection);
            connection.Execute("update " + dialect.EscapeIdentifier("kuery_tph_contract") + " set " + dialect.EscapeIdentifier("Kind") + " = 'unknown' where " + dialect.EscapeIdentifier("Id") + " = 1");
            var error = Assert.Throws<InvalidOperationException>(() => connection.Query<Creature>().First(x => x.Id == 1));
            Assert.Contains("discriminator", error.Message);
            Assert.Equal(2, connection.Query<Creature>().OfType<Cat>().Count());
        }

        [Table("kuery_tph_contract")]
        [Discriminator("Kind")]
        [DerivedType(typeof(Creature), "creature")]
        [DerivedType(typeof(Cat), "cat")]
        [DerivedType(typeof(SpecialCat), "special")]
        [DerivedType(typeof(Dog), "dog")]
        public class Creature
        {
            [PrimaryKey]
            public int Id { get; set; }
            public string Name { get; set; }
        }
        public class Cat : Creature { public int Lives { get; set; } }
        public class SpecialCat : Cat { public int Level { get; set; } }
        public class Dog : Creature { public string Breed { get; set; } }
        public class UnregisteredCreature : Creature { }
    }
}
