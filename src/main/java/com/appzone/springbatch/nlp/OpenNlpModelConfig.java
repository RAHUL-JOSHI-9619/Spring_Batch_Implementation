package com.appzone.springbatch.nlp;

import opennlp.tools.lemmatizer.LemmatizerME;
import opennlp.tools.lemmatizer.LemmatizerModel;
import opennlp.tools.postag.POSModel;
import opennlp.tools.postag.POSTaggerME;
import opennlp.tools.tokenize.TokenizerME;
import opennlp.tools.tokenize.TokenizerModel;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;

import java.io.IOException;
import java.io.InputStream;

/**
 * Loads the OpenNLP models used by ColumnNameNlpShortener, once, at startup.
 *
 * IMPORTANT: this only wires up the beans - it does not ship the actual model
 * (.bin) files, since those are large binaries. Download them and place them
 * on the classpath (e.g. src/main/resources/opennlp-models/) before running:
 *
 *   - en-token.bin            (SentenceDetector/Tokenizer)      - Apache OpenNLP model zoo
 *   - en-pos-ud-ewt.bin        (Universal Dependencies POS model, tags like NOUN/PROPN/VERB)
 *   - en-lemmatizer.bin        (statistical lemmatizer model)
 *
 * Model downloads: https://opennlp.apache.org/models.html
 *
 * (Only add the opennlp-tools dependency once to the project's pom.xml/build.gradle,
 * e.g. org.apache.opennlp:opennlp-tools:2.x - it is not added automatically here.)
 */
@Configuration
public class OpenNlpModelConfig {

    private static final String TOKENIZER_MODEL_PATH = "/opennlp-models/opennlp-en-ud-ewt-tokens-1.3-2.5.4.bin";
    private static final String POS_MODEL_PATH = "/opennlp-models/opennlp-en-ud-ewt-pos-1.3-2.5.4.bin";
    private static final String LEMMATIZER_MODEL_PATH = "/opennlp-models/opennlp-en-ud-ewt-lemmas-1.3-2.5.4.bin";

    @Bean
    public TokenizerME tokenizer() throws IOException {
        try (InputStream in = new ClassPathResource(TOKENIZER_MODEL_PATH).getInputStream()) {
            return new TokenizerME(new TokenizerModel(in));
        }
    }

    @Bean
    public POSTaggerME posTagger() throws IOException {
        try (InputStream in = new ClassPathResource(POS_MODEL_PATH).getInputStream()) {
            return new POSTaggerME(new POSModel(in));
        }
    }

    @Bean
    public LemmatizerME lemmatizer() throws IOException {
        try (InputStream in = new ClassPathResource(LEMMATIZER_MODEL_PATH).getInputStream()) {
            return new LemmatizerME(new LemmatizerModel(in));
        }
    }
}