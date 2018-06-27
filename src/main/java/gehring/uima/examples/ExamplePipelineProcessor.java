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

		BenchmarkResult result = Benchmarks.benchmark(reader, pipeline, configuration);

		LOGGER.info("Benchmark returned. (" + result.toString() + ")");

		Gson gson = new GsonBuilder().setPrettyPrinting().create();
		JsonParser jp = new JsonParser();
		JsonElement je = jp.parse(result.toString());
		String prettyJsonString = gson.toJson(je);

		System.out.println("Benchmark results: " + prettyJsonString);
	}

	public static void main(final String[] args) {
		CollectionReaderDescription reader;
		AnalysisEngineDescription pipeline;

		reader = SampleCollectionReaderFactory.getGutenbergReaderDescription();
		pipeline = SamplePipelineFactory.getNewPipelineDescription();

		printBenchmark(reader, pipeline);

	}

}
