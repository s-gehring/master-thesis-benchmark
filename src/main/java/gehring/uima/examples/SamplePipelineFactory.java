package gehring.uima.examples;

import static org.apache.uima.fit.factory.AnalysisEngineFactory.createEngineDescription;

import org.apache.uima.UIMAException;
import org.apache.uima.analysis_engine.AnalysisEngine;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.fit.factory.AggregateBuilder;
import org.apache.uima.resource.ResourceInitializationException;

import de.tudarmstadt.ukp.dkpro.core.matetools.MateLemmatizer;
import de.tudarmstadt.ukp.dkpro.core.opennlp.OpenNlpNamedEntityRecognizer;
import de.tudarmstadt.ukp.dkpro.core.opennlp.OpenNlpPosTagger;
import de.tudarmstadt.ukp.dkpro.core.opennlp.OpenNlpSegmenter;

public class SamplePipelineFactory {
	private SamplePipelineFactory() {
	}

	private static AggregateBuilder getPipelineAggregate() throws UIMAException {
		AnalysisEngineDescription segmenter;
		AnalysisEngineDescription posTagger;
		AnalysisEngineDescription lemmatizer;
		AnalysisEngineDescription recognizer;
		AnalysisEngineDescription identifier;
		try {
			identifier = createEngineDescription(LanguageSetter.class);
			segmenter = createEngineDescription(OpenNlpSegmenter.class);
			lemmatizer = createEngineDescription(MateLemmatizer.class);
			posTagger = createEngineDescription(OpenNlpPosTagger.class);
			recognizer = createEngineDescription(OpenNlpNamedEntityRecognizer.class);

		} catch (ResourceInitializationException e) {
			throw new RuntimeException("Error defining engine descriptions.", e);
		}
		AggregateBuilder builder = new AggregateBuilder();
		builder.add(identifier);
		builder.add(segmenter);
		builder.add(lemmatizer);
		builder.add(posTagger);
		builder.add(recognizer);

		return builder;
	}

	public static AnalysisEngine getNewPipeline() {
		try {
			return getPipelineAggregate().createAggregate();
		} catch (UIMAException e) {
			throw new RuntimeException("Error while creating the UIMA aggregate engine.", e);
		}
	}

	public static AnalysisEngineDescription getNewPipelineDescription() {
		try {
			return getPipelineAggregate().createAggregateDescription();
		} catch (UIMAException e) {
			throw new RuntimeException("Error while creating the UIMA aggregate description.", e);
		}
	}
}
