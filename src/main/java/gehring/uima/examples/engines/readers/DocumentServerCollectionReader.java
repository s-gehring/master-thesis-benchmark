package gehring.uima.examples.engines.readers;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;

import org.apache.commons.io.IOUtils;
import org.apache.uima.UimaContext;
import org.apache.uima.cas.CAS;
import org.apache.uima.collection.CollectionException;
import org.apache.uima.fit.component.CasCollectionReader_ImplBase;
import org.apache.uima.fit.descriptor.ConfigurationParameter;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.Progress;
import org.apache.uima.util.ProgressImpl;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.google.common.net.UrlEscapers;

public class DocumentServerCollectionReader extends CasCollectionReader_ImplBase {

	private static final Logger LOGGER = Logger.getLogger(DocumentServerCollectionReader.class.getName());

	/**
	 * Document Server URL.
	 */
	public static final String PARAM_SERVER_URL = "serverUrl";
	@ConfigurationParameter(name = PARAM_SERVER_URL, mandatory = true)
	protected String serverUrl;

	/**
	 * Document Server URL.
	 */
	public static final String PARAM_PERCENTAGE = "percentage";
	@ConfigurationParameter(name = PARAM_PERCENTAGE, mandatory = false, defaultValue = "1.")
	protected Double percentage;

	/**
	 * Maximum document size (-1 = no limit). In Bytes. Exclusively.
	 */
	public static final String PARAM_SIZE_MAX = "maxSize";
	@ConfigurationParameter(name = PARAM_SIZE_MAX, mandatory = false, defaultValue = "-1")
	protected Long maxSize;

	/**
	 * Minimum document size (0=No limit). In Bytes. Inclusively.
	 */
	public static final String PARAM_SIZE_MIN = "minSize";
	@ConfigurationParameter(name = PARAM_SIZE_MIN, mandatory = false, defaultValue = "0")
	protected Long minSize;

	private LinkedList<URL> entries = new LinkedList<URL>();
	private int sizedIndexSize;
	private int completeIndexSize;
	private int wantedEntries;

	// From
	// https://stackoverflow.com/questions/12800588/how-to-calculate-a-file-size-from-url-in-java
	private static long getFileSize(final URL url) {
		URLConnection conn = null;
		try {
			conn = url.openConnection();
			if (conn instanceof HttpURLConnection) {
				((HttpURLConnection) conn).setRequestMethod("HEAD");
			}
			conn.getInputStream();
			return conn.getContentLengthLong();
		} catch (IOException e) {
			throw new RuntimeException("Failed to get HEAD (hehehe) for URL '" + url.toString() + "'.", e);
		} finally {
			if (conn instanceof HttpURLConnection) {
				((HttpURLConnection) conn).disconnect();
			}
		}
	}

	private JSONArray getIndex() throws ResourceInitializationException {
		URL indexUrl;
		try {
			indexUrl = new URL(this.serverUrl + "/index.json");
		} catch (MalformedURLException e) {
			throw new ResourceInitializationException(e);
		}
		String jsonString;
		try (InputStream indexStream = indexUrl.openStream()) {
			jsonString = IOUtils.toString(indexStream, "UTF-8");
		} catch (IOException e) {
			throw new ResourceInitializationException(e);
		}
		JSONArray index;
		try {
			index = (JSONArray) new JSONParser().parse(jsonString);
		} catch (ParseException e) {
			throw new ResourceInitializationException(e);
		}
		return index;
	}

	@Override
	public void initialize(final UimaContext aContext) throws ResourceInitializationException {
		JSONArray completeIndex = this.getIndex();

		this.completeIndexSize = completeIndex.size();
		for (int i = 0; i < this.completeIndexSize; ++i) {
			Object entry = completeIndex.get(i);
			if (!(entry instanceof String)) {
				throw new ResourceInitializationException(new IllegalArgumentException(
						"Entry of index.json is not of type string but of type '" + entry.getClass().getName() + "'."));
			}

			String nextEntry = UrlEscapers.urlPathSegmentEscaper().escape((String) entry);

			nextEntry = this.serverUrl + "/" + nextEntry;
			URL nextUrl;
			try {
				nextUrl = new URL(nextEntry);
			} catch (MalformedURLException e) {
				throw new RuntimeException(
						"Failed to parse build URL around " + entry + ". The built URL was '" + nextEntry + "'.", e);
			}

			long fileSize = getFileSize(nextUrl);

			if (fileSize < this.minSize) {
				// Not enough
			} else if (fileSize >= this.maxSize && this.maxSize >= 0) {
				// Too much
			} else {
				this.entries.add(nextUrl);
			}
		}
		this.sizedIndexSize = this.entries.size();
		System.out.println("We want a collection size of " + this.sizedIndexSize + "*" + this.percentage + "="
				+ this.sizedIndexSize * this.percentage + " (or ceiled "
				+ Math.ceil(this.sizedIndexSize * this.percentage) + ").");

		Long wantedSize = Math.round(Math.ceil(this.sizedIndexSize * this.percentage));
		this.wantedEntries = wantedSize.intValue();
		if (this.wantedEntries < 1) {
			throw new ResourceInitializationException(new IllegalArgumentException("Nothing to read in the index."));

		}
		System.out.println("After all, we got " + this.wantedEntries + " entries.");
	}

	/**
	 * From
	 * http://blog.mark-mclaren.info/2007/02/invalid-xml-characters-when-valid-utf8_5873.html
	 *
	 * This method ensures that the output String has only valid XML unicode
	 * characters as specified by the XML 1.0 standard. For reference, please
	 * see <a href="http://www.w3.org/TR/2000/REC-xml-20001006#NT-Char">the
	 * standard</a>. This method will return an empty String if the input is
	 * null or empty.
	 *
	 * @param in
	 *            The String whose non-valid characters we want to remove.
	 * @return The in String, stripped of non-valid characters.
	 */
	private static String stripInvalidXMLCharacters(final String in) {
		StringBuffer out = new StringBuffer(); // Used to hold the output.
		char current; // Used to reference the current character.

		if (in == null || ("".equals(in))) {
			return ""; // vacancy test.
		}
		for (int i = 0; i < in.length(); i++) {
			current = in.charAt(i); // NOTE: No IndexOutOfBoundsException caught
									// here; it should not happen.
			if ((current == 0x9) || (current == 0xA) || (current == 0xD) || ((current >= 0x20) && (current <= 0xD7FF))
					|| ((current >= 0xE000) && (current <= 0xFFFD))
					|| ((current >= 0x10000) && (current <= 0x10FFFF))) {
				out.append(current);
			}
		}
		return out.toString();
	}

	@Override
	public void getNext(final CAS aCAS) throws IOException, CollectionException {
		if (!this.hasNext()) {
			throw new IndexOutOfBoundsException();
		}
		URL nextUrl = this.entries.pop();

		try (InputStream contentStream = nextUrl.openStream()) {
			aCAS.setDocumentText(stripInvalidXMLCharacters(IOUtils.toString(contentStream)));
		}
	}

	@Override
	public boolean hasNext() throws IOException, CollectionException {
		return this.entries.size() > (this.sizedIndexSize - this.wantedEntries);

	}

	@Override
	public Progress[] getProgress() {
		List<Progress> result = new ArrayList<Progress>();

		Progress prog = new ProgressImpl(this.sizedIndexSize - this.entries.size(), this.wantedEntries,
				Progress.ENTITIES);

		result.add(prog);

		return (Progress[]) result.toArray();
	}

	@Override
	public void close() throws IOException {
		// Nix zu closen.
	}

}
