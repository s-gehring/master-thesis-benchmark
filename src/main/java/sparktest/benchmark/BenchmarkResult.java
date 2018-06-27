package sparktest.benchmark;

import java.util.HashMap;
import java.util.Map;

import org.json.simple.JSONObject;

public class BenchmarkResult implements BenchmarkMetadataProvider {
	private static final long NANOSECONDS_TO_SECONDS_MULTIPLIER = 1000 * 1000 * 1000;
	private BenchmarkMetadata metadata;
	private Map<String, Long> times = new HashMap<String, Long>();

	public BenchmarkResult() {
	}

	public void setMetadata(final BenchmarkMetadata meta) {
		this.metadata = meta;
	}

	public void startMeasurement(final String key) {
		this.times.put(key, System.nanoTime());
	}
	public void endMeasurement(final String key) {
		this.times.put(key, System.nanoTime() - this.times.get(key));
	}

	public BenchmarkMetadata getMetadata() {
		return this.metadata;
	}

	public double getTimeNeededInSeconds(final String key) {
		return this.times.get(key) * NANOSECONDS_TO_SECONDS_MULTIPLIER;
	}

	/**
	 * In #docs per second.
	 *
	 * @return
	 */
	public double getCasThroughput(final String key) {
		return this.getNumberOfDocuments() / this.getTimeNeededInSeconds(key);
	}

	/**
	 * In #bytes per second.
	 *
	 * @return
	 */
	public double getByteThroughput(final String key) {
		return this.getSumOfAllCasSizes() / this.getTimeNeededInSeconds(key);
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
	public int getSumOfAllCasSizes() {
		return this.metadata.getSumOfAllCasSizes();
	}

	@Override
	public int getSumOfAllDocumentSizes() {
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

}
