package sparktest.benchmark;

public interface BenchmarkMetadataProvider {
	public double getAvgCasSize();

	public double getAvgDocumentSize();

	public int getNumberOfDocuments();

	public int getSumOfAllCasSizes();

	public int getSumOfAllDocumentSizes();

}
