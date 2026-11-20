using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace Kuery.Tests
{
    public abstract partial class QueryPipelineContract
    {
        public static IEnumerable<object[]> OperatorCoverageCases()
        {
            var names = new[] { "Where", "Select", "OrderBy", "OrderByDescending", "ThenBy", "ThenByDescending", "Skip", "Take", "Distinct",
                "Count", "LongCount", "Sum", "Min", "Max", "Average", "Any", "All", "Contains", "First", "FirstOrDefault", "Single", "SingleOrDefault",
                "Last", "LastOrDefault", "ElementAt", "ElementAtOrDefault", "Join", "LeftJoin", "RightJoin", "FullJoin", "Union", "Concat", "Intersect", "Except",
                "GroupBy", "SelectMany", "Cast", "OfType", "DefaultIfEmpty", "Reverse" };
            var expected = new[] { 3, 20, 1, 4, 2, 4, 2, 2, 3, 4, 4, 10, 1, 4, 25, 1, 1, 1, 1, 0, 2, 0, 4, 0, 2, 0, 4, 4, 4, 4, 4, 5, 1, 1, 3, 16, 4, 4, 1, 4 };
            for (int i = 0; i < names.Length; i++) yield return new object[] { names[i], expected[i] };
        }

        [Theory]
        [MemberData(nameof(OperatorCoverageCases))]
        public void AllFortyOperatorFamiliesExecute(string name, int expected)
        {
            using var connection = Open();
            var query = connection.Query<ContractRow>();
            var result = name switch
            {
                "Where" => query.Where(x => x.Id > 1).Count(),
                "Select" => query.Select(x => x.Id * 2).Sum(),
                "OrderBy" => query.OrderBy(x => x.Id).First().Id,
                "OrderByDescending" => query.OrderByDescending(x => x.Id).First().Id,
                "ThenBy" => query.OrderBy(x => x.Id % 2).ThenBy(x => x.Id).First().Id,
                "ThenByDescending" => query.OrderBy(x => x.Id % 2).ThenByDescending(x => x.Id).First().Id,
                "Skip" => query.OrderBy(x => x.Id).Skip(2).Count(),
                "Take" => query.Take(2).Count(),
                "Distinct" => query.Select(x => x.Value).Distinct().Count(),
                "Count" => query.Count(),
                "LongCount" => (int)query.LongCount(),
                "Sum" => query.Sum(x => x.Id),
                "Min" => query.Min(x => x.Id),
                "Max" => query.Max(x => x.Id),
                "Average" => (int)(query.Average(x => x.Id) * 10),
                "Any" => query.Any() ? 1 : 0,
                "All" => query.All(x => x.Id > 0) ? 1 : 0,
                "Contains" => query.Contains(new ContractRow { Id = 2 }) ? 1 : 0,
                "First" => query.OrderBy(x => x.Id).First().Id,
                "FirstOrDefault" => query.FirstOrDefault(x => x.Id < 0)?.Id ?? 0,
                "Single" => query.Single(x => x.Id == 2).Id,
                "SingleOrDefault" => query.SingleOrDefault(x => x.Id < 0)?.Id ?? 0,
                "Last" => query.OrderBy(x => x.Id).Last().Id,
                "LastOrDefault" => query.OrderBy(x => x.Id).LastOrDefault(x => x.Id < 0)?.Id ?? 0,
                "ElementAt" => query.OrderBy(x => x.Id).ElementAt(1).Id,
                "ElementAtOrDefault" => query.OrderBy(x => x.Id).ElementAtOrDefault(99)?.Id ?? 0,
                "Join" => query.Join(query, x => x.Id, x => x.Id, (a, b) => a).Count(),
                "LeftJoin" => query.LeftJoin(query.Where(x => x.Id > 2), x => x.Id, x => x.Id, (a, b) => a).Count(),
                "RightJoin" => query.Where(x => x.Id > 2).RightJoin(query, x => x.Id, x => x.Id, (a, b) => b).Count(),
                "FullJoin" => query.Where(x => x.Id < 3).FullJoin(query.Where(x => x.Id > 1), x => x.Id, x => x.Id, (a, b) => a == null ? b.Id : a.Id).Count(),
                "Union" => query.Where(x => x.Id < 3).Union(query.Where(x => x.Id > 1)).Count(),
                "Concat" => query.Where(x => x.Id < 3).Concat(query.Where(x => x.Id > 1)).Count(),
                "Intersect" => query.Where(x => x.Id < 3).Intersect(query.Where(x => x.Id > 1)).Count(),
                "Except" => query.Where(x => x.Id < 3).Except(query.Where(x => x.Id > 1)).Count(),
                "GroupBy" => query.GroupBy(x => x.ParentId).Select(g => g.Key).Count(),
                "SelectMany" => query.SelectMany(x => query).Count(),
                "Cast" => query.Cast<object>().Count(),
                "OfType" => query.OfType<ContractRow>().Count(),
                "DefaultIfEmpty" => query.Take(0).DefaultIfEmpty().Count(),
                "Reverse" => query.OrderBy(x => x.Id).Reverse().First().Id,
                _ => throw new ArgumentOutOfRangeException(nameof(name)),
            };
            Assert.Equal(expected, result);
        }
    }
}
