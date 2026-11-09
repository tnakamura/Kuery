using System;
using System.Collections.Generic;
using System.Linq;
using BenchmarkDotNet.Attributes;
using Kuery;
using Kuery.Query;
using Kuery.Query.Internal;
using Kuery.Query.SqlExpressions;
using Microsoft.Data.Sqlite;

namespace KueryBenchmark
{
    [MemoryDiagnoser]
    public class QueryablePipeline
    {
        SqliteConnection _connection;
        IQueryable<PipelineItem> _rows;

        [GlobalSetup]
        public void Setup()
        {
            _connection = new SqliteConnection("Data Source=:memory:");
            _connection.Open();
            _connection.Execute("create table pipeline_item (Id integer primary key, ParentId integer, Name text, Amount real)");
            for (int i = 1; i <= 1000; i++)
                _connection.Insert(new PipelineItem { Id = i, ParentId = i / 2, Name = "Item" + i, Amount = i / 10d });
            _rows = _connection.Query<PipelineItem>();
            RepeatedExecution();
        }

        [GlobalCleanup]
        public void Cleanup() => _connection.Dispose();

        // Iteration setup forces a single invocation, keeping cache reset outside the measured operation.
        [IterationSetup(Target = nameof(FirstExecution))]
        public void ResetPlanCache() => CompiledQueryCache.Clear();

        [Benchmark]
        public List<PipelineItem> FirstExecution() => _rows.Where(x => x.Id > 990).ToList();

        [Benchmark(Baseline = true)]
        public List<PipelineItem> RepeatedExecution() => _rows.Where(x => x.Id > 990).ToList();

        [Benchmark]
        public int Count() => _rows.Where(x => x.Id > 500).Count();

        [Benchmark]
        public List<double> Projection() => _rows.Where(x => x.Id > 990).Select(x => x.Amount * 2).ToList();

        [Benchmark]
        public List<string> Join() => _rows.Where(x => x.Id > 990)
            .Join(_rows, child => child.ParentId, parent => parent.Id, (child, parent) => parent.Name).ToList();

        [Table("pipeline_item")]
        public class PipelineItem
        {
            [PrimaryKey]
            public int Id { get; set; }
            public int ParentId { get; set; }
            public string Name { get; set; }
            public double Amount { get; set; }
        }
    }
}
