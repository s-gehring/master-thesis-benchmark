package gehring.uima.distributed.benchmark;

import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.cas.CAS;
import org.apache.uima.collection.CollectionReaderDescription;

import gehring.uima.distributed.AnalysisResult;
import gehring.uima.distributed.SerializedCAS;
import gehring.uima.distributed.SharedUimaProcessor;
import gehring.uima.distributed.compression.CompressionAlgorithm;

public class Benchmarks {
	private static final Logger LOGGER = Logger.getLogger(Benchmarks.class);

	public static BenchmarkResult benchmark(final CollectionReaderDescription reader,
			final AnalysisEngineDescription pipeline, final SparkConf configuration,
			final CompressionAlgorithm compression) {

		BenchmarkResult benchmark = new BenchmarkResult();

		LOGGER.info("Starting analysis...");

		benchmark.startMeasurement("analysis");
		int casSum = 0, docSum = 0, docNum = 0;
		try (JavaSparkContext sparkContext = new JavaSparkContext(configuration)) {

			SharedUimaProcessor processor = new SharedUimaProcessor(sparkContext, compression,
					Logger.getLogger(SharedUimaProcessor.class));

			AnalysisResult results = processor.process(reader, pipeline);
			benchmark.endMeasurement("analysis");
			LOGGER.info("Finished analysis.");

			benchmark.startMeasurement("collecting");
			for (int i = 0; i < results.getNumPartitions(); ++i) {
				benchmark.startMeasurement("collection [" + i + "]");
				List<CAS> casResultsPartition = results.collectPartitions(new int[]{i});
				benchmark.endMeasurement("collection [" + i + "]");
				for (CAS currentResult : casResultsPartition) {
					SerializedCAS compressedCas = new SerializedCAS(currentResult, compression);
					++docNum;
					casSum = casSum + compressedCas.size();
					docSum = docSum + currentResult.getDocumentText().length();
				}
			}

		}
		benchmark.endMeasurement("collecting");

		BenchmarkMetadata meta = new BenchmarkMetadata(docNum, casSum, docSum);

		benchmark.setMetadata(meta);
		return benchmark;
	}
}
