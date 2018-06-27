package sparktest.benchmark;

import java.util.Iterator;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.cas.CAS;
import org.apache.uima.collection.CollectionReaderDescription;

import sparktest.SharedUimaProcessor;

public class Benchmarks {
	private static final Logger LOGGER = Logger.getLogger(Benchmarks.class);

	public static BenchmarkResult benchmark(final CollectionReaderDescription reader,
			final AnalysisEngineDescription pipeline, final SparkConf configuration) {

		BenchmarkResult benchmark = new BenchmarkResult();
		LOGGER.info("Initialize Benchmark...");
		benchmark.startMeasurement("initialization");
		SharedUimaProcessor processor = new SharedUimaProcessor(configuration,
				Logger.getLogger(SharedUimaProcessor.class));
		benchmark.endMeasurement("initialization");
		LOGGER.info("Finished benchmark initialization. Starting analysis...");
		benchmark.startMeasurement("analysis");
		Iterator<CAS> results = processor.process(reader, pipeline);
		benchmark.endMeasurement("analysis");
		LOGGER.info("Finished analysis.");
		if (!results.hasNext()) {
			throw new RuntimeException("Failed to get any results back from the pipeline.");
		}
		int i = 0;

		int casSum = 0, docSum = 0;
		while (results.hasNext()) {
			CAS currentResult = results.next();
			++i;
			casSum = casSum + currentResult.size();
			docSum = docSum + currentResult.getDocumentText().length();
		}
		BenchmarkMetadata meta = new BenchmarkMetadata(i, casSum, docSum);

		benchmark.setMetadata(meta);
		return benchmark;
	}
}
