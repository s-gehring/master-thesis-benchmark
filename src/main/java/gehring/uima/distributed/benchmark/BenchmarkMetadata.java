package gehring.uima.distributed.benchmark;

import org.json.simple.JSONObject;

public class BenchmarkMetadata implements BenchmarkMetadataProvider {
	private int numberOfDocs;
	private long sumAllCasSizes;
	private long sumAllDocSizes;
	private final String name;

	public BenchmarkMetadata(final int docNum, final long casSum, final long docSum, final String name) {
		this.setNumberOfDocuments(docNum);
		this.setSumOfAllCasSizes(casSum);
		this.setSumOfAllDocumentSizes(docSum);
		this.name = name;
	}

	@Override
	public double getAvgCasSize() {
		return this.sumAllCasSizes * 1. / this.numberOfDocs;
	}

	@Override
	public double getAvgDocumentSize() {
		return this.sumAllDocSizes * 1. / this.numberOfDocs;
	}

	@Override
	public int getNumberOfDocuments() {
		return this.numberOfDocs;
	}

	public void setNumberOfDocuments(final int numberOfDocs) {
		this.numberOfDocs = numberOfDocs;
	}

	@Override
	public long getSumOfAllCasSizes() {
		return this.sumAllCasSizes;
	}

	public void setSumOfAllCasSizes(final long sumAllCasSizes) {
		this.sumAllCasSizes = sumAllCasSizes;
	}

	@Override
	public long getSumOfAllDocumentSizes() {
		return this.sumAllDocSizes;
	}

	public void setSumOfAllDocumentSizes(final long sumAllDocSizes) {
		this.sumAllDocSizes = sumAllDocSizes;
	}

	@SuppressWarnings("unchecked")
	public JSONObject toJSONObject() {
		JSONObject result = new JSONObject();
		result.put("numberOfDocuments", this.numberOfDocs);
		result.put("sumOfAllDocumentSizes", this.sumAllDocSizes);
		result.put("sumOfAllCasSizes", this.sumAllCasSizes);
		return result;
	}

	@Override
	public String toString() {
		return this.toJSONObject().toJSONString();
	}

	public String getName() {
		return name;
	}

}
