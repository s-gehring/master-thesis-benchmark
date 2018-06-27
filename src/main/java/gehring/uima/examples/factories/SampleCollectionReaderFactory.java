package gehring.uima.examples.factories;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

import org.apache.uima.collection.CollectionReaderDescription;
import org.apache.uima.fit.factory.CollectionReaderFactory;
import org.apache.uima.resource.ResourceInitializationException;
import org.dkpro.core.io.tika.TikaReader;

import gehring.uima.examples.DocumentServerCollectionReader;

public class SampleCollectionReaderFactory {
	private SampleCollectionReaderFactory() {
	}

	public static CollectionReaderDescription getGutenbergReaderDescription() {
		CollectionReaderDescription result;
		try {
			result = CollectionReaderFactory.createReaderDescription(DocumentServerCollectionReader.class,
					DocumentServerCollectionReader.PARAM_SERVER_URL, "http://document-provider");
		} catch (ResourceInitializationException e) {
			throw new RuntimeException("Error creating the Gutenberg reader description.", e);
		}

		return result;
	}

	public static CollectionReaderDescription getTestFileReaderDescription() {
		CollectionReaderDescription result;
		try {
			String path = SampleCollectionReaderFactory.class.getClassLoader().getResource("Digitale Teilhabe.pdf")
					.getPath();
			File testFile = new File(path);
			String testFolder = testFile.getParent();
			try {
				testFolder = URLDecoder.decode(testFolder, "UTF-8");
			} catch (UnsupportedEncodingException e) {
				throw new RuntimeException(e);
			}
			System.out.println(testFolder);
			// @formatter:off
			result = CollectionReaderFactory.createReaderDescription(
					TikaReader.class,
					TikaReader.PARAM_SOURCE_LOCATION, testFolder,
					TikaReader.PARAM_PATTERNS, "*.pdf");
			// @formatter:on
		} catch (ResourceInitializationException e) {
			throw new RuntimeException("Failed to create the collection reader description for test files.", e);
		}
		return result;
	}
}
