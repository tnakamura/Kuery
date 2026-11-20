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
            _connection.Execute("create table pipeline_hierarchy (Id integer primary key, Kind text, Rating integer)");
            _connection.Execute("insert into pipeline_hierarchy select Id, case when Id % 2 = 0 then 'rated' else 'basic' end, Id from pipeline_item");
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

        [Benchmark]
        public List<string> LeftJoin() => _rows.Where(x => x.Id > 990)
            .LeftJoin(_rows, child => child.ParentId, parent => parent.Id, (child, parent) => parent == null ? null : parent.Name).ToList();

        [Benchmark]
        public List<int> FullJoin() => _rows.Where(x => x.Id > 990)
            .FullJoin(_rows.Where(x => x.Id > 990), left => left.Id, right => right.Id,
                (left, right) => left == null ? right.Id : left.Id).ToList();

        [Benchmark]
        public List<PipelineBase> TphMaterialization() => _connection.Query<PipelineBase>().Where(x => x.Id > 990).ToList();

        [Benchmark]
        public List<PipelineRated> OfType() => _connection.Query<PipelineBase>().OfType<PipelineRated>().Where(x => x.Id > 990).ToList();

        [Table("pipeline_hierarchy"), Discriminator("Kind")]
        [DerivedType(typeof(PipelineBase), "basic"), DerivedType(typeof(PipelineRated), "rated")]
        public class PipelineBase { [PrimaryKey] public int Id { get; set; } }
        public class PipelineRated : PipelineBase { public int Rating { get; set; } }

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
