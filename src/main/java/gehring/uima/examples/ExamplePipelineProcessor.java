package gehring.uima.examples;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.collection.CollectionReaderDescription;
import org.apache.uima.resource.ResourceCreationSpecifier;
import org.xml.sax.SAXException;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

import gehring.uima.distributed.benchmark.BenchmarkResult;
import gehring.uima.distributed.benchmark.Benchmarks;
import gehring.uima.distributed.compression.CompressionAlgorithm;
import gehring.uima.distributed.compression.NoCompression;
import gehring.uima.distributed.serialization.CasSerialization;
import gehring.uima.distributed.serialization.XmiCasSerialization;
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

	private static void printBenchmarkSingleInstance(final CollectionReaderDescription reader,
			final AnalysisEngineDescription pipeline, final String testName) {
		BenchmarkResult result;
		result = Benchmarks.benchmarkSingle(reader, pipeline);
		LOGGER.info("Single Benchmark returned. (" + result.toString() + ")");

		Gson gson = new GsonBuilder().setPrettyPrinting().create();
		JsonParser jp = new JsonParser();
		JsonElement je = jp.parse(result.toString());
		String prettyJsonString = gson.toJson(je);

		System.out.println("Benchmark results: " + prettyJsonString);

	}

	private static void toXML(final ResourceCreationSpecifier thingy, final String suffix) {
		String implName = thingy.getImplementationName();
		if (implName == null) {
			implName = thingy.getClass().getName();
		}
		String fullpath = implName + suffix;
		try (OutputStream output = new FileOutputStream(fullpath)) {
			// UTF-8 implicit.
			thingy.toXML(output);
		} catch (FileNotFoundException e) {
			throw new RuntimeException("Output file for resource ('" + fullpath + "') can not be created.", e);
		} catch (IOException | SAXException e) {
			throw new RuntimeException("Failed to serialize resource ('" + fullpath + "') to XML.", e);
		}

	}

	private static void printBenchmarkSharedInstance(final CollectionReaderDescription reader,
			final AnalysisEngineDescription pipeline, final String testName, final CompressionAlgorithm compression,
			final CasSerialization serialization) {

		System.out.println("Starting to print benchmark. (STDOUT)");
		System.err.println("Starting to print benchmark. (STDERR)");

		LOGGER.info("Starting to print benchmark. (LOGGER INFO)");
		BenchmarkResult result;
		result = Benchmarks.benchmarkShared(reader, pipeline, getConfiguration(testName), compression, serialization);

		LOGGER.info("Shared Benchmark returned. (" + result.toString() + ")");

		Gson gson = new GsonBuilder().setPrettyPrinting().create();
		JsonParser jp = new JsonParser();
		JsonElement je = jp.parse(result.toString());
		String prettyJsonString = gson.toJson(je);

		System.out.println("Benchmark results: " + prettyJsonString);

	}

	private static Double getCliDouble(final Double defaultValue, final String[] args, final String command) {
		String cli = getCliValue(args, command);
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
	private static String getCliValue(final String[] args, final String command) {
		for (int i = 0; i < args.length - 1; ++i) {
			if (args[i].equals(command)) {
				return args[i + 1];
			}
		}
		return null;
	}

	@SuppressWarnings("unused")
	private static int getCliInt(final int defaultValue, final String[] args, final String command) {
		String cli = getCliValue(args, command);
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

	private static long getCliLong(final long defaultValue, final String[] args, final String command) {
		String cli = getCliValue(args, command);
		if (cli == null) {
			return defaultValue;
		}
		try {
			return Long.parseLong(cli);

		} catch (NumberFormatException e) {
			throw new IllegalArgumentException("Expected long after '" + command + "', but got '" + cli + "'.", e);
		}
	}
	private static String getCliString(final String defaultValue, final String[] args, final String command) {
		String result = getCliValue(args, command);
		return result == null ? defaultValue : result;
	}

	private static CasSerialization parseSerializationClassString(final String clazz) {
		try {
			Class<?> serializationUncasted = Class.forName(clazz);

			if (serializationUncasted.isInterface()) {
				throw new IllegalArgumentException(
						"Given serialization class ('" + clazz + "') is not instantiable, because it is an interface.");
			}
			if (CasSerialization.class.isAssignableFrom(serializationUncasted)) {
				CasSerialization result;
				try {
					Method constructor = serializationUncasted.getMethod("getInstance");
					Object newInstance = constructor.invoke(null);
					result = (CasSerialization) (newInstance);
				} catch (IllegalAccessException e) {
					throw new RuntimeException(
							"The empty constructor is not visible for serialization algorithm '" + clazz + "'.");
				} catch (NoSuchMethodException e) {
					throw new RuntimeException(
							"There is no 'getInstance' method for serialization algorithm '" + clazz + "'.", e);
				} catch (SecurityException e) {
					throw new RuntimeException("Auto-Generated Runtime-Exception.", e);
				} catch (IllegalArgumentException e) {
					throw new RuntimeException("Wrong arguments given for serialization algorithm '" + clazz + "'.", e);
				} catch (InvocationTargetException e) {
					throw new RuntimeException(
							"The getInstance method for serialization algorithm '" + clazz + "' threw an exception.",
							e);
				}
				LOGGER.info("Successfully found and instanitated serialization algorithm '" + clazz + "'.");
				return result;
			}
			throw new IllegalArgumentException(
					"Class '" + clazz + "' is not a '" + CasSerialization.class.getName() + "'.");

		} catch (ClassNotFoundException e) {
			throw new IllegalArgumentException("Failed to find serialization algorithm '" + clazz + "'.", e);
		}

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

	private static boolean getCli(final String[] args, final String command) {
		for (String arg : args) {
			if (arg.equals(command)) {
				return true;
			}
		}
		return false;

	}

	public static void main(final String[] args) {
		long startTime = System.currentTimeMillis();

		LOGGER.info("Command line arguments:\n\t" + Arrays.toString(args));

		final Double documentPercentage = getCliDouble(0.5, args, "-d");
		final String compressionClass = getCliString(NoCompression.class.getName(), args, "-c");
		final String serializationClass = getCliString(XmiCasSerialization.class.getName(), args, "-s");

		final boolean singleInstance = getCli(args, "--single");
		final Long minDocSize = getCliLong(0, args, "--minSize");
		final Long maxDocSize = getCliLong(-1, args, "--maxSize");
		final Long pipelineId = getCliLong(0, args, "--pipeline");

		CompressionAlgorithm compression = parseCompressionClassString(compressionClass);
		CasSerialization serialization = parseSerializationClassString(serializationClass);

		CollectionReaderDescription reader;
		AnalysisEngineDescription pipeline;

		pipeline = SamplePipelineFactory.getPipelineDescriptionById(pipelineId.intValue());

		Float percentage = new Float((documentPercentage));

		reader = SampleCollectionReaderFactory.getGutenbergPartialReaderSizedDescription(percentage, minDocSize,
				maxDocSize);

		String instanceName = "Gutenberg (" + Math.round((10000. * percentage)) / 100. + "%)]["
				+ Math.round(3036 * percentage) + " documents";

		LOGGER.info("Current working directory '" + System.getProperty("user.dir") + "'.");

		toXML(reader, ".reader.xml");
		toXML(pipeline, ".pipeline.xml");

		if (singleInstance) {
			printBenchmarkSingleInstance(reader, pipeline, instanceName);
		} else {
			printBenchmarkSharedInstance(reader, pipeline, instanceName, compression, serialization);

		}

		long endTime = System.currentTimeMillis();
		System.out.println(
				"Time needed: " + (endTime - startTime) / BenchmarkResult.TimeUnit.MILLI.toSecondsMultiplier() + "s");

	}

}
