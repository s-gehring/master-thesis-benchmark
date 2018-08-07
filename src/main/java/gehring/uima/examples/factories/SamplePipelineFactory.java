package gehring.uima.examples.factories;

import static org.apache.uima.fit.factory.AnalysisEngineFactory.createEngineDescription;

import org.apache.uima.UIMAException;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.fit.factory.AggregateBuilder;
import org.apache.uima.resource.ResourceInitializationException;

import de.tudarmstadt.ukp.dkpro.core.berkeleyparser.BerkeleyParser;
import de.tudarmstadt.ukp.dkpro.core.clearnlp.ClearNlpLemmatizer;
import de.tudarmstadt.ukp.dkpro.core.maltparser.MaltParser;
import de.tudarmstadt.ukp.dkpro.core.matetools.MateLemmatizer;
import de.tudarmstadt.ukp.dkpro.core.matetools.MatePosTagger;
import de.tudarmstadt.ukp.dkpro.core.opennlp.OpenNlpNamedEntityRecognizer;
import de.tudarmstadt.ukp.dkpro.core.opennlp.OpenNlpPosTagger;
import de.tudarmstadt.ukp.dkpro.core.opennlp.OpenNlpSegmenter;
import de.tudarmstadt.ukp.dkpro.core.stanfordnlp.StanfordNamedEntityRecognizer;
import de.tudarmstadt.ukp.dkpro.core.stanfordnlp.StanfordPosTagger;
import de.tudarmstadt.ukp.dkpro.core.stanfordnlp.StanfordSegmenter;
import gehring.uima.examples.engines.annotators.LanguageSetter;

public class SamplePipelineFactory {
	private SamplePipelineFactory() {
	}

	private static AggregateBuilder getMaltParserAggregate() throws UIMAException {
		// DKPro Recipe from
		// https://dkpro.github.io/dkpro-core/groovy/recipes/fully-mixed/
		AnalysisEngineDescription segmenter;
		AnalysisEngineDescription posTagger;
		AnalysisEngineDescription parser;
		AnalysisEngineDescription identifier;
		try {
			identifier = createEngineDescription(LanguageSetter.class);
			segmenter = createEngineDescription(StanfordSegmenter.class);
			posTagger = createEngineDescription(StanfordPosTagger.class);
			parser = createEngineDescription(MaltParser.class);

		} catch (ResourceInitializationException e) {
			throw new RuntimeException("Error defining engine descriptions.", e);
		}
		AggregateBuilder builder = new AggregateBuilder();
		builder.add(identifier);
		builder.add(segmenter);
		builder.add(posTagger);
		builder.add(parser);

		return builder;
	}

	private static AggregateBuilder getFullyMixedAggregate() throws UIMAException {
		// DKPro Recipe from
		// https://dkpro.github.io/dkpro-core/groovy/recipes/fully-mixed/
		AnalysisEngineDescription segmenter;
		AnalysisEngineDescription posTagger;
		AnalysisEngineDescription lemmatizer;
		AnalysisEngineDescription parser;
		AnalysisEngineDescription recognizer;
		AnalysisEngineDescription identifier;
		try {
			identifier = createEngineDescription(LanguageSetter.class);
			segmenter = createEngineDescription(OpenNlpSegmenter.class);
			lemmatizer = createEngineDescription(MatePosTagger.class);
			posTagger = createEngineDescription(ClearNlpLemmatizer.class);
			parser = createEngineDescription(BerkeleyParser.class, BerkeleyParser.PARAM_WRITE_PENN_TREE, true);
			recognizer = createEngineDescription(StanfordNamedEntityRecognizer.class);

		} catch (ResourceInitializationException e) {
			throw new RuntimeException("Error defining engine descriptions.", e);
		}
		AggregateBuilder builder = new AggregateBuilder();
		builder.add(identifier);
		builder.add(segmenter);
		builder.add(lemmatizer);
		builder.add(posTagger);
		builder.add(parser);
		builder.add(recognizer);

		return builder;
	}

	private static AggregateBuilder getOpenNlpAggregate() throws UIMAException {
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

	private static AnalysisEngineDescription getMaltParserPipelineDescription() {
		try {
			return getMaltParserAggregate().createAggregateDescription();
		} catch (UIMAException e) {
			throw new RuntimeException("Error while creating the UIMA aggregate description.", e);
		}
	}

	private static AnalysisEngineDescription getFullyMixedPipelineDescription() {
		try {
			return getFullyMixedAggregate().createAggregateDescription();
		} catch (UIMAException e) {
			throw new RuntimeException("Error while creating the UIMA aggregate description.", e);
		}
	}

	private static AnalysisEngineDescription getOpenNlpPipelineDescription() {
		try {
			return getOpenNlpAggregate().createAggregateDescription();
		} catch (UIMAException e) {
			throw new RuntimeException("Error while creating the UIMA aggregate description.", e);
		}
	}

	public static AnalysisEngineDescription getPipelineDescriptionById(final int id) {
		if (id == 0) {
			return getOpenNlpPipelineDescription();
		}
		if (id == 1) {
			return getFullyMixedPipelineDescription();
		}
		if (id == 2) {
			return getMaltParserPipelineDescription();
		}
		throw new IllegalArgumentException("Pipeline ID (" + id + ") is not in range.");
	}
}
