package sparktest.example;

import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.fit.component.JCasAnnotator_ImplBase;
import org.apache.uima.fit.descriptor.ConfigurationParameter;
import org.apache.uima.jcas.JCas;

public class LanguageSetter extends JCasAnnotator_ImplBase {

	/**
	 * The feature path to create the annotation of.
	 */
	public static final String PARAM_LANGUAGE = "language";
	@ConfigurationParameter(name = PARAM_LANGUAGE, mandatory = false, defaultValue = "en")
	protected String language;

	@Override
	public void process(final JCas aJCas) throws AnalysisEngineProcessException {
		aJCas.setDocumentLanguage(this.language);

	}

}
