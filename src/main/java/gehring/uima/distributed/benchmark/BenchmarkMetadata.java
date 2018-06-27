package gehring.uima.distributed.benchmark;

import org.json.simple.JSONObject;

public class BenchmarkMetadata implements BenchmarkMetadataProvider {
	private int numberOfDocs;
	private int sumAllCasSizes;
	private int sumAllDocSizes;

	public BenchmarkMetadata(final int docNum, final int casSum, final int docSum) {
		this.setNumberOfDocuments(docNum);
		this.setSumOfAllCasSizes(casSum);
		this.setSumOfAllDocumentSizes(docSum);
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
	public int getSumOfAllCasSizes() {
		return this.sumAllCasSizes;
	}

	public void setSumOfAllCasSizes(final int sumAllCasSizes) {
		this.sumAllCasSizes = sumAllCasSizes;
	}

	@Override
	public int getSumOfAllDocumentSizes() {
		return this.sumAllDocSizes;
	}

	public void setSumOfAllDocumentSizes(final int sumAllDocSizes) {
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

}
