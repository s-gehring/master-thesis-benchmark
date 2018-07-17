package gehring.uima.distributed.benchmark;

import java.io.ByteArrayOutputStream;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.impl.XmiCasSerializer;
import org.apache.uima.collection.CollectionReaderDescription;
import org.apache.uima.fit.pipeline.JCasIterable;
import org.apache.uima.jcas.JCas;
import org.xml.sax.SAXException;

import gehring.uima.distributed.AnalysisResult;
import gehring.uima.distributed.SerializedCAS;
import gehring.uima.distributed.SharedUimaProcessor;
import gehring.uima.distributed.compression.CompressionAlgorithm;

public class Benchmarks {
	private static final Logger LOGGER = Logger.getLogger(Benchmarks.class);

	public static BenchmarkResult benchmarkSingle(final CollectionReaderDescription reader,
			final AnalysisEngineDescription pipeline) {
		BenchmarkResult benchmark = new BenchmarkResult();

		benchmark.startMeasurement("analysis");
		int docNum = 1;
		long casSum = 0, docSum = 0;
		for (JCas jcas : new JCasIterable(reader, pipeline)) {
			LOGGER.info("Done with " + docNum++ + " documents.");
			docSum += jcas.getDocumentText().length();

			try {
				ByteArrayOutputStream oStream = new ByteArrayOutputStream();
				XmiCasSerializer.serialize(jcas.getCas(), oStream);
				casSum += oStream.size();
			} catch (SAXException e) {
				throw new RuntimeException("Failed to estimate CAS size, because of an XML error.", e);
			}

		}
		benchmark.endMeasurement("analysis");
		BenchmarkMetadata meta = new BenchmarkMetadata(docNum, casSum, docSum, "Single");
		benchmark.setMetadata(meta);
		return benchmark;
	}

	public static BenchmarkResult benchmarkShared(final CollectionReaderDescription reader,
			final AnalysisEngineDescription pipeline, final SparkConf configuration,
			final CompressionAlgorithm compression) {

		BenchmarkResult benchmark = new BenchmarkResult();

		LOGGER.info("Starting analysis...");

		benchmark.startMeasurement("analysis");
		long casSum = 0, docSum = 0;
		int docNum = 0;
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

				benchmark.startMeasurement("compression [" + i + "]");
				for (CAS currentResult : casResultsPartition) {
					SerializedCAS compressedCas = new SerializedCAS(currentResult, compression);
					++docNum;
					casSum = casSum + compressedCas.size();
					docSum = docSum + currentResult.getDocumentText().length();
				}
				benchmark.endMeasurement("compression [" + i + "]");
			}

		}
		benchmark.endMeasurement("collecting");

		BenchmarkMetadata meta = new BenchmarkMetadata(docNum, casSum, docSum, "Shared");

		benchmark.setMetadata(meta);
		return benchmark;
	}
}
