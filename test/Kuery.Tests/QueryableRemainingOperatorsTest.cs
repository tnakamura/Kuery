using System;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace Kuery.Tests
{
    public abstract partial class QueryPipelineContract
    {
        [Fact]
        public void ReversePreservesPagingAndAllOrderingKeys()
        {
            using var connection = Open();
            var query = connection.Query<ContractRow>();
            var rows = query.ToArray();
            Assert.Equal(rows.OrderBy(x => x.ParentId).ThenByDescending(x => x.Id).Reverse().Select(x => x.Id),
                query.OrderBy(x => x.ParentId).ThenByDescending(x => x.Id).Reverse().Select(x => x.Id));
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
