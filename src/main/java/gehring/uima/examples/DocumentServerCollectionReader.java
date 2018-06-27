package gehring.uima.examples;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

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

	/**
	 * Document Server URL.
	 */
	public static final String PARAM_SERVER_URL = "serverUrl";
	@ConfigurationParameter(name = PARAM_SERVER_URL, mandatory = true)
	protected String serverUrl;

	private LinkedList<String> entries = new LinkedList<String>();
	private int all;

	@Override
	public void initialize(final UimaContext aContext) throws ResourceInitializationException {
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
		this.all = index.size();
		for (int i = 0; i < this.all; ++i) {
			Object entry = index.get(i);
			if (!(entry instanceof String)) {
				throw new ResourceInitializationException(new IllegalArgumentException(
						"Entry of index.json is not of type string but of type '" + entry.getClass().getName() + "'."));
			}
			this.entries.add((String) entry);
		}
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
		String nextEntry = this.entries.pop();

		nextEntry = UrlEscapers.urlPathSegmentEscaper().escape(nextEntry);

		nextEntry = this.serverUrl + "/" + nextEntry;
		URL nextUrl = new URL(nextEntry);

		try (InputStream contentStream = nextUrl.openStream()) {
			aCAS.setDocumentText(stripInvalidXMLCharacters(IOUtils.toString(contentStream)));
		}

	}

	@Override
	public boolean hasNext() throws IOException, CollectionException {
		return this.entries.size() > 0;
	}

	@Override
	public Progress[] getProgress() {
		List<Progress> result = new ArrayList<Progress>();

		Progress prog = new ProgressImpl(this.entries.size(), this.all, Progress.ENTITIES);

		result.add(prog);

		return (Progress[]) result.toArray();
	}

	@Override
	public void close() throws IOException {
		// Nix zu closen.
	}

}
