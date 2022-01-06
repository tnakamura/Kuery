using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnosers;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Environments;
using BenchmarkDotNet.Analysers;
using BenchmarkDotNet.Exporters;
using BenchmarkDotNet.Exporters.Csv;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Loggers;

namespace KueryBenchmark
{
    internal class BenchmarkConfig : ManualConfig
    {
        public BenchmarkConfig() :
            base()
        {
            //var baseConfig = Job.Default;
            var baseConfig = Job.ShortRun.WithWarmupCount(5).WithIterationCount(5);
            AddJob(baseConfig.WithRuntime(CoreRuntime.Core31).WithJit(Jit.RyuJit).WithPlatform(Platform.X64));

            AddLogger(ConsoleLogger.Default);

            AddColumnProvider(
                DefaultColumnProviders.Instance);

            AddAnalyser(
                EnvironmentAnalyser.Default,
                OutliersAnalyser.Default,
                MinIterationTimeAnalyser.Default,
                MultimodalDistributionAnalyzer.Default,
                RuntimeErrorAnalyser.Default,
                ZeroMeasurementAnalyser.Default,
                BaselineCustomAnalyzer.Default);

            AddExporter(
                HtmlExporter.Default,
                CsvExporter.Default,
                MarkdownExporter.GitHub);

            AddDiagnoser(MemoryDiagnoser.Default);
        }
    }
}
