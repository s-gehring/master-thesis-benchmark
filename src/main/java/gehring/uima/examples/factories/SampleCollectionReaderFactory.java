package gehring.uima.examples.factories;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

import org.apache.uima.collection.CollectionReaderDescription;
import org.apache.uima.fit.factory.CollectionReaderFactory;
import org.apache.uima.resource.ResourceInitializationException;
import org.dkpro.core.io.tika.TikaReader;

import gehring.uima.examples.engines.readers.DocumentServerCollectionReader;

public class SampleCollectionReaderFactory {
	private SampleCollectionReaderFactory() {
	}

	public static CollectionReaderDescription getGutenbergPartialReaderSizedDescription(final Float percentage,
			final long min, final long max) {
		CollectionReaderDescription result;
		try {
			// @formatter:off
			result = CollectionReaderFactory.createReaderDescription(DocumentServerCollectionReader.class,
					DocumentServerCollectionReader.PARAM_SERVER_URL, "http://document-provider",
					DocumentServerCollectionReader.PARAM_PERCENTAGE, percentage,
					DocumentServerCollectionReader.PARAM_SIZE_MIN, min,
					DocumentServerCollectionReader.PARAM_SIZE_MAX, max);
			// @formatter:on
		} catch (ResourceInitializationException e) {
			throw new RuntimeException("Error creating the Gutenberg reader description.", e);
		}

		return result;
	}

	public static CollectionReaderDescription getGutenbergPartialReaderDescription(final Float percentage) {
		CollectionReaderDescription result;
		try {
			result = CollectionReaderFactory.createReaderDescription(DocumentServerCollectionReader.class,
					DocumentServerCollectionReader.PARAM_SERVER_URL, "http://document-provider",
					DocumentServerCollectionReader.PARAM_PERCENTAGE, percentage);
		} catch (ResourceInitializationException e) {
			throw new RuntimeException("Error creating the Gutenberg reader description.", e);
		}

		return result;
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
