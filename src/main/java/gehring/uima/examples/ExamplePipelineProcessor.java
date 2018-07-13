package gehring.uima.examples;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;

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
import gehring.uima.distributed.compression.CompressionAlgorithm;
import gehring.uima.distributed.compression.NoCompression;
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
			final AnalysisEngineDescription pipeline, final String testName, final CompressionAlgorithm compression) {

		System.out.println("Starting to print benchmark. (STDOUT)");
		System.err.println("Starting to print benchmark. (STDERR)");

		LOGGER.info("Starting to print benchmark. (LOGGER INFO)");
		BenchmarkResult result;
		result = Benchmarks.benchmarkShared(reader, pipeline, getConfiguration(testName), compression);

		LOGGER.info("Shared Benchmark returned. (" + result.toString() + ")");

		Gson gson = new GsonBuilder().setPrettyPrinting().create();
		JsonParser jp = new JsonParser();
		JsonElement je = jp.parse(result.toString());
		String prettyJsonString = gson.toJson(je);

		System.out.println("Benchmark results: " + prettyJsonString);

		result = Benchmarks.benchmarkSingle(reader, pipeline);
		LOGGER.info("Single Benchmark returned. (" + result.toString() + ")");

		gson = new GsonBuilder().setPrettyPrinting().create();
		jp = new JsonParser();
		je = jp.parse(result.toString());
		prettyJsonString = gson.toJson(je);

		System.out.println("Benchmark results: " + prettyJsonString);

	}

	private static Double getCliDouble(final Double defaultValue, final String[] args, final String command) {
		String cli = getCli(args, command);
		if (cli == null) {
			return defaultValue;
		}
		try {
			Double result = Double.parseDouble(cli);
			return result;
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException("Expected double after '" + command + "', but got '" + cli + "'.", e);
		}
	}
	private static String getCli(final String[] args, final String command) {
		for (int i = 0; i < args.length - 1; ++i) {
			if (args[i].equals(command)) {
				return args[i + 1];
			}
		}
		return null;
	}

	@SuppressWarnings("unused")
	private static int getCliInt(final int defaultValue, final String[] args, final String command) {
		String cli = getCli(args, command);
		if (cli == null) {
			return defaultValue;
		}
		try {
			Integer result = Integer.parseInt(cli);
			return result;
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException("Expected int after '" + command + "', but got '" + cli + "'.", e);
		}
	}
	private static String getCliString(final String defaultValue, final String[] args, final String command) {
		String result = getCli(args, command);
		return result == null ? defaultValue : result;
	}

	private static CompressionAlgorithm parseCompressionClassString(final String clazz) {
		try {
			Class<?> compressionUncasted = Class.forName(clazz);

			if (compressionUncasted.isInterface()) {
				throw new IllegalArgumentException(
						"Given compression class ('" + clazz + "') is not instantiable, because it is an interface.");
			}
			if (CompressionAlgorithm.class.isAssignableFrom(compressionUncasted)) {
				CompressionAlgorithm result;
				try {
					Method constructor = compressionUncasted.getMethod("getInstance");
					Object newInstance = constructor.invoke(null);
					result = (CompressionAlgorithm) (newInstance);
				} catch (IllegalAccessException e) {
					throw new RuntimeException(
							"The empty constructor is not visible for compression algorithm '" + clazz + "'.");
				} catch (NoSuchMethodException e) {
					throw new RuntimeException(
							"There is no 'getInstance' method for compression algorithm '" + clazz + "'.", e);
				} catch (SecurityException e) {
					throw new RuntimeException("Auto-Generated Runtime-Exception.", e);
				} catch (IllegalArgumentException e) {
					throw new RuntimeException("Wrong arguments given for compression algorithm '" + clazz + "'.", e);
				} catch (InvocationTargetException e) {
					throw new RuntimeException(
							"The getInstance method for compression algorithm '" + clazz + "' threw an exception.", e);
				}
				LOGGER.info("Successfully found and instanitated compression algorithm '" + clazz + "'.");
				return result;
			}
			throw new IllegalArgumentException(
					"Class '" + clazz + "' is not a '" + CompressionAlgorithm.class.getName() + "'.");

		} catch (ClassNotFoundException e) {
			throw new IllegalArgumentException("Failed to find compression algorithm '" + clazz + "'.", e);
		}

	}

	public static void main(final String[] args) {
		long startTime = System.currentTimeMillis();

		LOGGER.info("Command line arguments:\n\t" + Arrays.toString(args));

		final Double documentPercentage = getCliDouble(0.5, args, "-d");
		final String compressionClass = getCliString(NoCompression.class.getName(), args, "-c");

		CompressionAlgorithm compression = parseCompressionClassString(compressionClass);

		CollectionReaderDescription reader;
		AnalysisEngineDescription pipeline;

		pipeline = SamplePipelineFactory.getOpenNlpPipelineDescription();

		Float percentage = new Float((documentPercentage));
		reader = SampleCollectionReaderFactory.getGutenbergPartialReaderDescription(percentage);
		printBenchmark(reader, pipeline, "Gutenberg (" + Math.round((10000. * percentage)) / 100. + "%)]["
				+ Math.round(3038 * percentage) + " documents", compression);

		long endTime = System.currentTimeMillis();
		System.out.println("Time needed for all steps: "
				+ (endTime - startTime) / BenchmarkResult.TimeUnit.MILLI.toSecondsMultiplier() + "s");

	}

}
