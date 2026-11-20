using System;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace Kuery.Tests
{
    public abstract partial class QueryPipelineContract
    {
        [Fact]
        public void OptionalScalarsComposeWithDistinctGroupingAndSetOperations()
        {
            using var connection = Open();
            var query = connection.Query<ContractRow>();
            var joined = query.Select(x => x.Id).LeftJoin(query.Where(x => x.Id == 1).Select(x => x.Id - 1), x => x - 1, x => x, (left, right) => right);
            Assert.Equal(new[] { 0 }, joined.Distinct());
            Assert.Equal(1, joined.GroupBy(x => x).Select(g => new { g.Key, Count = g.Count() }).Count());
            Assert.Equal(4, joined.GroupBy(x => x).Select(g => g.Count()).Single());
            Assert.Equal(new[] { 0 }, joined.Union(query.Take(0).Select(x => x.Id).DefaultIfEmpty()));
            Assert.Equal(new[] { 0 }, query.Take(0).Select(x => x.Id).DefaultIfEmpty().DefaultIfEmpty());
            Assert.Null(query.Take(0).DefaultIfEmpty().DefaultIfEmpty().Single());
            Assert.Empty(query.Take(0).DefaultIfEmpty().OfType<ContractRow>());
            var allNull = query.Where(x => x.Id == 1).Select(x => new { x.Name });
            var actual = query.LeftJoin(allNull, x => 1, x => 1, (left, right) => right).ToList();
            Assert.All(actual, x => { Assert.NotNull(x); Assert.Null(x.Name); });
            Assert.Equal(4, query.LeftJoin(allNull.Take(0), x => 1, x => 1, (left, right) => new { Missing = right == null }).Count(x => x.Missing));
            Assert.Null(query.Take(0).Select(x => new ContractDto(x.Id, x.Name)).DefaultIfEmpty().Single());
            Assert.Null(query.Take(0).Select(x => new ContractRow { Id = x.Id }).DefaultIfEmpty().Single());
        }

        [Fact]
        public void OuterJoinsMatchObjectsForCompositeKeysAndIndependentPaging()
        {
            using var connection = Open();
            var query = connection.Query<ContractRow>();
            var rows = query.ToArray();
            var left = query.OrderBy(x => x.Id).Take(3).Select(x => new { Key = new { x.Value, x.ParentId }, x.Id });
            var right = query.OrderByDescending(x => x.Id).Take(3).Select(x => new { Key = new { x.Value, x.ParentId }, x.Id });
            var actualLeft = left.LeftJoin(right, x => x.Key, x => x.Key, (a, b) => new { A = a.Id, B = b == null ? -1 : b.Id }).OrderBy(x => x.A).ThenBy(x => x.B).ToArray();
            var expectedLeft = rows.OrderBy(x => x.Id).Take(3).LeftJoin(rows.OrderByDescending(x => x.Id).Take(3), x => new { x.Value, x.ParentId }, x => new { x.Value, x.ParentId },
                (a, b) => new { A = a.Id, B = b == null ? -1 : b.Id }).OrderBy(x => x.A).ThenBy(x => x.B).ToArray();
            Assert.Equal(expectedLeft, actualLeft);
            var full = left.FullJoin(right, x => x.Key, x => x.Key, (a, b) => new { A = a == null ? -1 : a.Id, B = b == null ? -1 : b.Id });
            var unmatchedRight = rows.OrderByDescending(x => x.Id).Take(3).Where(b => !rows.OrderBy(x => x.Id).Take(3).Any(a => a.Value == b.Value && a.ParentId == b.ParentId))
                .Select(b => new { A = -1, B = b.Id });
            Assert.Equal(expectedLeft.Concat(unmatchedRight).OrderBy(x => x.A).ThenBy(x => x.B), full.OrderBy(x => x.A).ThenBy(x => x.B));
            Assert.Equal(1, query.Take(0).DefaultIfEmpty().Count());
        }

        public class ContractDto
        {
            public ContractDto(int id, string name) { Id = id; Name = name; }
            public int Id { get; }
            public string Name { get; }
        }

        [Fact]
        public void DirectOuterJoinsPreserveInputsAndUnmatchedEntities()
        {
            using var connection = Open();
            var query = connection.Query<ContractRow>();
            var rows = query.ToArray();
            var left = query.Where(x => x.Id < 4).LeftJoin(query.OrderBy(x => x.Id).Take(3), x => (int?)x.Id, x => x.ParentId,
                (parent, child) => new { Parent = parent.Id, Child = child == null ? -1 : child.Id });
            var expected = rows.Where(x => x.Id < 4).LeftJoin(rows.OrderBy(x => x.Id).Take(3), x => (int?)x.Id, x => x.ParentId,
                (parent, child) => new { Parent = parent.Id, Child = child == null ? -1 : child.Id });
            Assert.Equal(expected.OrderBy(x => x.Parent).ThenBy(x => x.Child), left.OrderBy(x => x.Parent).ThenBy(x => x.Child));
            var right = query.RightJoin(query, parent => (int?)parent.Id, child => child.ParentId,
                (parent, child) => new { Parent = parent == null ? -1 : parent.Id, Child = child.Id });
            Assert.Equal(4, right.Count());
            Assert.Equal(-1, right.Single(x => x.Child == 1).Parent);
            Assert.Equal(3, right.Where(x => x.Parent > 0).Count());
        }

        [Fact]
        public void FullJoinPreservesDuplicatesAndSupportsComposition()
        {
            using var connection = Open();
            var query = connection.Query<ContractRow>();
            var full = query.Where(x => x.Id <= 3).FullJoin(query.Where(x => x.Id >= 2), x => x.Value, x => x.Value,
                (left, right) => new { Left = left == null ? -1 : left.Id, Right = right == null ? -1 : right.Id });
            Assert.Equal(new[] { "-1:4", "1:-1", "2:2", "2:3", "3:2", "3:3" },
                full.OrderBy(x => x.Left).ThenBy(x => x.Right).AsEnumerable().Select(x => x.Left + ":" + x.Right));
            Assert.Equal(6, full.Count());
            Assert.Equal(4, full.Where(x => x.Left > 0 && x.Right > 0).Count());
            Assert.Equal(3, full.Select(x => x.Left).Distinct().Where(x => x > 0).Count());
            Assert.Equal(2, full.OrderBy(x => x.Left).Skip(1).Take(2).Count());
            Assert.Equal(4, query.FullJoin(query.Take(0), x => x.Id, x => x.Id, (left, right) => left).Count());
            Assert.Equal(4, query.Take(0).FullJoin(query, x => x.Id, x => x.Id, (left, right) => right).Count());
            Assert.Empty(query.Take(0).FullJoin(query.Take(0), x => x.Id, x => x.Id, (left, right) => left));
        }

        [Fact]
        public void ScalarAndObjectOuterJoinShapesUseWholeValueDefaults()
        {
            using var connection = Open();
            var query = connection.Query<ContractRow>();
            Assert.Equal(new[] { 0, 0, 0, 0 }, query.Select(x => x.Id).LeftJoin(query.Take(0).Select(x => x.Id), x => x, x => x, (left, right) => right));
            var objects = query.Select(x => new { x.Id }).LeftJoin(query.Take(0).Select(x => new { x.Id }), x => x.Id, x => x.Id,
                (left, right) => right).ToList();
            Assert.Equal(4, objects.Count);
            Assert.All(objects, x => Assert.Null(x));
            Assert.Equal(4, query.Select(x => new { x.Id }).FullJoin(query.Take(0).Select(x => new { x.Id }), x => x.Id, x => x.Id,
                (left, right) => left).Count());
        }

        [Fact]
        public void ReversePreservesPagingAndAllOrderingKeys()
        {
            using var connection = Open();
            var query = connection.Query<ContractRow>();
            var rows = query.ToArray();
            Assert.Equal(rows.OrderBy(x => x.ParentId ?? -1).ThenByDescending(x => x.Id).Reverse().Select(x => x.Id),
                query.OrderBy(x => x.ParentId ?? -1).ThenByDescending(x => x.Id).Reverse().Select(x => x.Id));
            Assert.Equal(new[] { 3, 2 }, query.OrderBy(x => x.Id).Take(3).Reverse().Take(2).Select(x => x.Id));
            Assert.Equal(new[] { 1, 2, 3, 4 }, query.OrderBy(x => x.Id).Reverse().Reverse().Select(x => x.Id));
            Assert.Throws<NotSupportedException>(() => query.Reverse().ToList());
        }

        [Fact]
        public void CastChangesTheShaperWithoutFilteringRows()
        {
            using var connection = Open();
            var query = connection.Query<ContractRow>().OrderBy(x => x.Id);
            Assert.Equal(new[] { 1, 2, 3, 4 }, query.Cast<object>().Cast<ContractRow>().Select(x => x.Id));
            Assert.All(query.Cast<object>(), x => Assert.IsType<ContractRow>(x));
            Assert.Equal(new[] { 1L, 2L, 3L, 4L }, query.Select(x => x.Id).Cast<long>());
            Assert.Throws<InvalidCastException>(() => query.Cast<object>().Cast<string>().ToList());
        }

        [Fact]
        public void TerminalContainsPreservesNullPagingAndEntityIdentity()
        {
            using var connection = Open();
            var query = connection.Query<ContractRow>();
            Assert.True(query.Select(x => (object)x.Id).Contains(2));
            Assert.True(query.Select(x => x.Name).Contains(null));
            Assert.False(query.Take(0).Select(x => x.Name).Contains(null));
            Assert.False(query.OrderBy(x => x.Id).Take(2).Select(x => x.Id).Contains(4));
            Assert.True(query.Select(x => x.Name).Distinct().Contains("Alpha"));
            Assert.True(query.Contains(new ContractRow { Id = 2, Name = "different" }));
            Assert.False(query.Contains(new ContractRow { Id = 20 }));
            Assert.False(query.Contains(null));
            var item = new ContractRow { Id = 2 };
            Assert.Equal(new[] { 1, 2, 3, 4 }, query.Where(x => query.Contains(item)).OrderBy(x => x.Id).Select(x => x.Id));
            item.Id = 20;
            Assert.Empty(query.Where(x => query.Contains(item)));
            Assert.Equal(new[] { 2, 3, 4 }, query.Where(x => query.Where(y => y.Id > 1).Contains(x)).OrderBy(x => x.Id).Select(x => x.Id));
        }

        [Fact]
        public async Task DefaultIfEmptyDistinguishesAbsenceFromActualNullAndZero()
        {
            using var connection = Open();
            var query = connection.Query<ContractRow>();
            Assert.Equal(new[] { 0 }, query.Take(0).Select(x => x.Id).DefaultIfEmpty());
            Assert.Equal(new[] { 1 }, query.Take(0).Select(x => x.Id).DefaultIfEmpty().Where(x => x == 0).Select(x => x + 1));
            Assert.Equal(new string[] { null }, query.Take(0).Select(x => x.Name).DefaultIfEmpty());
            Assert.Equal(new ContractRow[] { null }, query.Take(0).DefaultIfEmpty());
            Assert.Null(query.Take(0).Select(x => new { x.Id }).DefaultIfEmpty().Single());
            Assert.Equal(new[] { 4, 3, 2, 1 }, query.OrderByDescending(x => x.Id).DefaultIfEmpty().Select(x => x.Id));
            Assert.Equal(4, query.DefaultIfEmpty().Count());
            Assert.Equal(1, query.Take(0).DefaultIfEmpty().Count());
            Assert.True(query.Take(0).DefaultIfEmpty().Contains(null));
            Assert.Equal(new[] { 0 }, query.Where(x => x.Id == 1).Select(x => x.Id - 1).DefaultIfEmpty());
            Assert.Single(await query.Take(0).DefaultIfEmpty().ToListAsync());
            Assert.Equal(0, await query.Take(0).Select(x => x.Id).DefaultIfEmpty().FirstAsync());
            Assert.Throws<NotSupportedException>(() => query.Select(x => x.Id).DefaultIfEmpty(99).ToList());
        }
    }
}
