package gehring.uima.distributed.benchmark;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

import org.json.simple.JSONObject;

public class BenchmarkResult implements BenchmarkMetadataProvider {
	private BenchmarkMetadata metadata;
	private Map<String, Long> times = new HashMap<String, Long>();
	private List<String> mapKeys = new LinkedList<String>();
	public static enum TimeUnit {
		NANO(1000 * 1000 * 1000), MICRO(1000 * 1000), MILLI(1000), ONE(1);

		private long toSecondsMultiplier;

		private TimeUnit(final long toSecondsMultiplier) {
			this.toSecondsMultiplier = toSecondsMultiplier;
		}

		public long toSecondsMultiplier() {
			return this.toSecondsMultiplier;
		}
	}

	public BenchmarkResult() {
	}

	public void setMetadata(final BenchmarkMetadata meta) {
		this.metadata = meta;
	}

	public void startMeasurement(final String key) {
		this.mapKeys.add(key);
		this.times.put(key, System.nanoTime());

	}
	public void endMeasurement(final String key) {
		this.times.put(key, System.nanoTime() - this.times.get(key));
	}

	public BenchmarkMetadata getMetadata() {
		return this.metadata;
	}

	public double getTimeNeeded(final String key, final TimeUnit unit) {
		return this.times.get(key) * unit.toSecondsMultiplier();
	}

	/**
	 * In #docs per second.
	 *
	 * @return
	 */
	public double getCasThroughput(final String key) {
		return this.getNumberOfDocuments() / TimeUnit.ONE.toSecondsMultiplier();
	}

	/**
	 * In #bytes per second.
	 *
	 * @return
	 */
	public double getByteThroughput(final String key) {
		return this.getSumOfAllCasSizes() / TimeUnit.ONE.toSecondsMultiplier();
	}

	@Override
	public double getAvgCasSize() {
		return this.metadata.getAvgCasSize();
	}

	@Override
	public double getAvgDocumentSize() {
		return this.metadata.getAvgDocumentSize();
	}

	@Override
	public int getNumberOfDocuments() {
		return this.metadata.getNumberOfDocuments();
	}

	@Override
	public long getSumOfAllCasSizes() {
		return this.metadata.getSumOfAllCasSizes();
	}

	@Override
	public long getSumOfAllDocumentSizes() {
		return this.metadata.getSumOfAllDocumentSizes();
	}

	@SuppressWarnings("unchecked")
	@Override
	public String toString() {
		JSONObject result = new JSONObject();
		result.put("metadata", this.metadata.toJSONObject());
		result.put("times", new JSONObject(this.times));

		return result.toJSONString();
	}
	public String toCsvLine(final Integer RamBytes, final Integer cores, final Integer machines, final TimeUnit unit) {
		StringJoiner joiner = new StringJoiner(";", "", "");
		joiner.add("" + RamBytes);
		joiner.add("" + cores);
		joiner.add("" + machines);
		joiner.add("" + this.metadata.getSumOfAllDocumentSizes());
		joiner.add("" + this.metadata.getSumOfAllCasSizes());
		joiner.add("" + this.metadata.getNumberOfDocuments());
		for (String mapKey : this.mapKeys) {
			Double result = (this.times.get(mapKey) * 1. / unit.toSecondsMultiplier());
			joiner.add(result.toString());
		}
		return joiner.toString();
	}

	public String toCsvHead() {
		StringJoiner joiner = new StringJoiner(";", "", "");
		for (String mapKey : this.mapKeys) {
			joiner.add(mapKey);
		}
		return "Ram;Cores;Machines;DocsSize;CASsSize;NumDocs;" + joiner.toString();
	}

	public String toHumanReadableCsvHead() {
		StringJoiner joiner = new StringJoiner(";", "", "");
		for (String mapKey : this.mapKeys) {
			joiner.add("Time measurement (" + mapKey + ")");
		}
		return "RAM in Bytes;Number of Cores;Number of Machines;Sum of all Document Sizes;Sum of all CAS Sizes;Number of Documents;"
				+ joiner.toString();
	}

}
