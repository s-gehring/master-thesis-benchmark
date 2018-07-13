package gehring.uima.distributed.benchmark;

public interface BenchmarkMetadataProvider {
	public double getAvgCasSize();

	public double getAvgDocumentSize();

	public int getNumberOfDocuments();

	public long getSumOfAllCasSizes();

	public long getSumOfAllDocumentSizes();

}
