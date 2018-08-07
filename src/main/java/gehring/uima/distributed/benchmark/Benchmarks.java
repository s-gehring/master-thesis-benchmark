package gehring.uima.distributed.benchmark;

import java.io.ByteArrayOutputStream;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.uima.analysis_engine.AnalysisEngine;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.impl.XmiCasSerializer;
import org.apache.uima.collection.CollectionReaderDescription;
import org.apache.uima.fit.factory.AnalysisEngineFactory;
import org.apache.uima.fit.pipeline.JCasIterable;
import org.apache.uima.fit.pipeline.SimplePipeline;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.xml.sax.SAXException;

import gehring.uima.distributed.AnalysisResult;
import gehring.uima.distributed.SerializedCAS;
import gehring.uima.distributed.SharedUimaProcessor;
import gehring.uima.distributed.compression.CompressionAlgorithm;
import gehring.uima.distributed.serialization.CasSerialization;

public class Benchmarks {
	private static final Logger LOGGER = Logger.getLogger(Benchmarks.class);

	public static BenchmarkResult benchmarkSingle(final CollectionReaderDescription reader,
			final AnalysisEngineDescription pipeline) {
		BenchmarkResult benchmark = new BenchmarkResult();

		benchmark.startMeasurement("analysis");
		int docNum = 0;
		long casSum = 0, docSum = 0;
		AnalysisEngine engine;
		try {
			engine = AnalysisEngineFactory.createEngine(pipeline);
		} catch (ResourceInitializationException e2) {
			throw new RuntimeException("Error instantiating pipeline.", e2);
		}
		int i = 0;
		for (JCas jcas : new JCasIterable(reader)) {
			LOGGER.info("Starting with document " + ++docNum + ".");
			benchmark.startMeasurement("analysis (" + i + ")");
			try {
				SimplePipeline.runPipeline(jcas, engine);
			} catch (AnalysisEngineProcessException e1) {
				throw new RuntimeException("The pipeline broke ;_;", e1);
			}
			benchmark.endMeasurement("analysis (" + i + ")");
			++i;
			LOGGER.info("Done with " + docNum + " documents.");
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
			final CompressionAlgorithm compression, final CasSerialization serialization) {

		BenchmarkResult benchmark = new BenchmarkResult();

		LOGGER.info("Starting analysis...");

		benchmark.startMeasurement("analysis");
		long casSum = 0, docSum = 0;
		int docNum = 0;
		try (JavaSparkContext sparkContext = new JavaSparkContext(configuration)) {
			SharedUimaProcessor processor = new SharedUimaProcessor(sparkContext, compression, serialization,
					Logger.getLogger(SharedUimaProcessor.class));

			AnalysisResult results = processor.process(reader, pipeline);
			benchmark.endMeasurement("analysis");
			LOGGER.info("Finished analysis.");

			// In case of empty document set.
			if (results != null) {
				Long resultLength = results.count();
				docNum = resultLength.intValue();
				benchmark.startMeasurement("collecting");
				int numParts = results.getNumPartitions();
				for (int i = 0; i <= numParts; ++i) {
					int limit = 20;
					if ((i + 1) * limit > numParts) {
						limit = numParts % limit;
					}
					int[] toCollect = new int[limit];
					for (int j = 0; j < limit; ++j) {
						int partNum = j + (20 * i);
						if (partNum > results.getNumPartitions()) {
							break;
						}
						toCollect[j] = j + (20 * i);
					}
					benchmark.startMeasurement("collection [" + i + "]");
					List<CAS> casResultsPartition = results.collectPartitions(toCollect);
					benchmark.endMeasurement("collection [" + i + "]");

					benchmark.startMeasurement("compression [" + i + "]");
					for (CAS currentResult : casResultsPartition) {
						SerializedCAS compressedCas = new SerializedCAS(currentResult, compression, serialization);
						casSum = casSum + compressedCas.size();
						docSum = docSum + currentResult.getDocumentText().length();
					}
					benchmark.endMeasurement("compression [" + i + "]");
				}
				benchmark.endMeasurement("collecting");
			}
		}

		BenchmarkMetadata meta = new BenchmarkMetadata(docNum, casSum, docSum, "Shared");

		benchmark.setMetadata(meta);
		return benchmark;
	}
}
