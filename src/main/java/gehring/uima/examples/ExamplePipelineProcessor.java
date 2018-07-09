package gehring.uima.examples;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.collection.CollectionReaderDescription;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

import gehring.uima.distributed.benchmark.BenchmarkResult;
import gehring.uima.distributed.benchmark.Benchmarks;
import gehring.uima.distributed.compression.ZLib;
import gehring.uima.examples.factories.SampleCollectionReaderFactory;
import gehring.uima.examples.factories.SamplePipelineFactory;

public class ExamplePipelineProcessor {

	private static final Logger LOGGER = Logger.getLogger(ExamplePipelineProcessor.class);

	private static SparkConf configuration = null;
	private synchronized static SparkConf getConfiguration(final String suffix) {
		if (configuration == null) {
			// @formatter:off
			configuration = new SparkConf().setMaster("spark://master:7077")
					.setAppName(ExamplePipelineProcessor.class.getSimpleName() + " (Spark Example) ["+suffix+"]")
					/*.set("spark.cores.max", "2")*/.set("spark.executor.memory", "10g")
					.set("spark.driver.maxResultSize", "4g");
			// @formatter:on
			LOGGER.info("Configured Spark.");
		} else {
			configuration
					.setAppName(ExamplePipelineProcessor.class.getSimpleName() + " (Spark Example) [" + suffix + "]");
		}
		return configuration;
	}

	private static void printBenchmark(final CollectionReaderDescription reader,
			final AnalysisEngineDescription pipeline, final String testName) {

		System.out.println("Starting to print benchmark. (STDOUT)");
		System.err.println("Starting to print benchmark. (STDERR)");

		LOGGER.info("Starting to print benchmark. (LOGGER INFO)");
		BenchmarkResult result;
		result = Benchmarks.benchmark(reader, pipeline, getConfiguration(testName), ZLib.getInstance());

		LOGGER.info("Benchmark returned. (" + result.toString() + ")");

		Gson gson = new GsonBuilder().setPrettyPrinting().create();
		JsonParser jp = new JsonParser();
		JsonElement je = jp.parse(result.toString());
		String prettyJsonString = gson.toJson(je);

		System.out.println("Benchmark results: " + prettyJsonString);
	}

	public static void main(final String[] args) {
		long startTime = System.currentTimeMillis();

		final int STEPS = 100;
		CollectionReaderDescription reader;
		AnalysisEngineDescription pipeline;

		pipeline = SamplePipelineFactory.getOpenNlpPipelineDescription();

		for (int i = 100; i <= STEPS; ++i) {
			long midTime = System.currentTimeMillis();
			Float percentage = new Float((.05 * i) / STEPS);
			System.out.println("Output for " + Math.round((100. * i) / STEPS) / 100. + "% (after "
					+ (midTime - startTime) / BenchmarkResult.TimeUnit.MILLI.toSecondsMultiplier()
					+ " seconds). Processing " + percentage + "% of documents.");
			reader = SampleCollectionReaderFactory.getGutenbergPartialReaderDescription(percentage);
			printBenchmark(reader, pipeline, "Gutenberg (" + Math.round((100. * percentage)) / 100. + "%)]["
					+ Math.round(3038 * percentage) + " documents");
		}
		long endTime = System.currentTimeMillis();
		System.out.println("Time needed for all steps: "
				+ (endTime - startTime) / BenchmarkResult.TimeUnit.MILLI.toSecondsMultiplier() + "s");
	}

}
