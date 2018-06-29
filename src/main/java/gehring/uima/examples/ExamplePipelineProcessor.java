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
	/*
	 * public static void exampleSimplePipeline() { CollectionReaderDescription
	 * reader = SampleCollectionReaderFactory.getSampleTextReaderDescription();
	 * AnalysisEngineDescription pipeline =
	 * SamplePipelineFactory.getNewPipelineDescription();
	 *
	 * SparkConf configuration = new
	 * SparkConf().setMaster("spark://master:7077")
	 * .setAppName(ExamplePipelineProcessor.class.getSimpleName() +
	 * " (Spark Example)") .set("spark.cores.max",
	 * "1").set("spark.executor.memory", "1g") /*
	 * .set("spark.submit.deployMode", "cluster") ;
	 *
	 * SharedUimaProcessor processor = new SharedUimaProcessor(configuration);
	 * Iterator<CAS> results = this.processor.process(reader, pipeline);
	 *
	 * if(!results.hasNext()) { System.exit(1); } int i =
	 * 0;while(results.hasNext()) { CAS currentResult = this.results.next();
	 * System.out.println("Result [" + (++this.i) + "]: " + currentResult); } }
	 */

	private static void printBenchmark(final CollectionReaderDescription reader,
			final AnalysisEngineDescription pipeline) {

		System.out.println("Starting to print benchmark. (STDOUT)");
		System.err.println("Starting to print benchmark. (STDERR)");

		LOGGER.info("Starting to print benchmark. (LOGGER INFO)");

		// @formatter:off
		SparkConf configuration = new SparkConf().setMaster("spark://master:7077")
				.setAppName(ExamplePipelineProcessor.class.getSimpleName() + " (Spark Example)")
				/*.set("spark.cores.max", "2")*/.set("spark.executor.memory", "2g");
		// @formatter:on
		LOGGER.info("Configured Spark.");

		BenchmarkResult result = Benchmarks.benchmark(reader, pipeline, configuration, ZLib.getInstance());

		LOGGER.info("Benchmark returned. (" + result.toString() + ")");

		Gson gson = new GsonBuilder().setPrettyPrinting().create();
		JsonParser jp = new JsonParser();
		JsonElement je = jp.parse(result.toString());
		String prettyJsonString = gson.toJson(je);

		System.out.println("Benchmark results: " + prettyJsonString);
	}

	public static void main(final String[] args) {
		long startTime = System.nanoTime();

		final int STEPS = 100;
		CollectionReaderDescription reader;
		AnalysisEngineDescription pipeline;

		pipeline = SamplePipelineFactory.getOpenNlpPipelineDescription();

		for (int i = 1; i <= STEPS; ++i) {
			long midTime = System.nanoTime();
			Float percentage = new Float((.05 * i) / STEPS);
			System.out.println("Output for " + Math.round((100. * i) / STEPS) / 100 + "% (after "
					+ (midTime - startTime) * BenchmarkResult.TimeUnit.NANO.toSecondsMultiplier()
					+ " seconds). Processing " + percentage + "% of documents.");
			reader = SampleCollectionReaderFactory.getGutenbergPartialReaderDescription(percentage);
			printBenchmark(reader, pipeline);
		}
		long endTime = System.nanoTime();
		System.out.println("Time needed for all steps: "
				+ (endTime - startTime) * BenchmarkResult.TimeUnit.NANO.toSecondsMultiplier() + "s");
	}

}
