package gehring.uima.examples;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.uima.cas.CAS;
import org.apache.uima.collection.CollectionException;
import org.apache.uima.collection.CollectionReader_ImplBase;
import org.apache.uima.util.Progress;
import org.apache.uima.util.ProgressImpl;

@Deprecated
public class SampleCollectionReader extends CollectionReader_ImplBase {

	/* @formatter:off */
	private static final String[] EXAMPLE_STRINGS = {
		"Ich hasse es, meine eigene Stimme zu hören.",
		"Warst du beim Friseur?",
		"Diesmal schenken wir uns aber nichts.",
		"Da wär‘ auch noch ein Parkplatz gewesen.",
		"Am meisten vermisse ich das deutsche Brot.",
		"Was denkst du gerade?",
		"Ich bin ja mit Namen ganz schlecht.",
		"Dir kann man nichts schenken, du hast ja schon alles.",
		"Ist da schon Salz drin?",
		"Das hab‘ ich auch. Das ist von IKEA, oder?",
		"Das kann man ja auch noch kalt essen.",
		"Kann ich so gehen?",
		"Ihr wart dann auf einmal irgendwie weg.",
		"Das Jahr ist wieder irgendwie so schnell vergangen.",
		"Das möchte ich gar nicht wissen.",
		"Lustig. Gerade habe ich an dich gedacht.",
		"Du, wir müssen ja auch gar nicht so lange bleiben.",
		"Gut, dass wir reserviert haben.",
		"Wo ist denn die Fernbedienung?",
		"Jetzt mal ganz ehrlich gesagt.",
		"Da hätte ich ein Problem mit.",
		"Die sind doch auch schon ewig zusammen.",
		"Ich hab‘ irgendwie noch gar keinen Hunger.",
		"Sieht irgendwie zu aus.",
		"Mir fehlen einfach die Berge.",
		"Ist hier rechts vor links?",
		"Störe ich gerade?",
		"Also morgen ist ja genau genommen schon heute.",
		"Das hab‘ ich akustisch nicht verstanden.",
		"Ich bin noch gar nicht in Weihnachtsstimmung."
	};
	/* @formatter:on */

	private int i = 0;

	@Override
	public void getNext(final CAS aCAS) throws IOException, CollectionException {
		if (!this.hasNext()) {
			throw new IndexOutOfBoundsException();
		}
		aCAS.setDocumentText(EXAMPLE_STRINGS[this.i++]);
		aCAS.setDocumentLanguage("de");
	}

	@Override
	public boolean hasNext() throws IOException, CollectionException {
		return EXAMPLE_STRINGS.length > this.i;
	}

	@Override
	public Progress[] getProgress() {
		List<Progress> result = new ArrayList<Progress>();

		Progress prog = new ProgressImpl(this.i, EXAMPLE_STRINGS.length, Progress.ENTITIES);

		result.add(prog);

		return null;
	}

	@Override
	public void close() throws IOException {
		// Nix zu closen.
	}

}
