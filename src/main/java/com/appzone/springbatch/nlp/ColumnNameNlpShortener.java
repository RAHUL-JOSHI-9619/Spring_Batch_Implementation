package com.appzone.springbatch.nlp;

import opennlp.tools.postag.POSTaggerME;
import opennlp.tools.tokenize.TokenizerME;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * No-AI replacement for the old ChatClient-based column name shortening. Also the final
 * safety net when the AI tier (AiColumnNameBatchShortener) is disabled, unreachable, or
 * couldn't resolve a particular name.
 *
 *   1. Tokenize + POS-tag, keep only nouns/proper nouns, drop stop words.
 *      If still too long: replace known business words with a curated abbreviation
 *      dictionary (customer -> cust, account -> acct, ...). Both of these count as the
 *      "NLP" tier for reporting purposes.
 *   2. If STILL too long: vowel-strip whatever words are left (keeping the first letter
 *      of each word). Reported as the "Vowel Removal" tier.
 *
 * If the result is STILL too long after both steps, this class deliberately returns that
 * best-effort value instead of hard-truncating it - HeaderProcessor's hash-based
 * fallback() is the final, last-resort step (tier: "Hash Fallback").
 */
@Service
public class ColumnNameNlpShortener {

    private static final Logger log = LoggerFactory.getLogger(ColumnNameNlpShortener.class);

    private static final Set<String> NOUN_TAGS = Set.of("NOUN", "PROPN");

    private static final Set<String> STOP_WORDS = Set.of(
            "a", "an", "the", "of", "in", "on", "at", "to", "for", "and", "or",
            "is", "are", "was", "were", "be", "been", "being", "with", "by",
            "from", "as", "that", "this", "these", "those", "it", "its", "into",
            "per", "than", "then", "so", "such", "up", "out", "off", "over"
    );

    private static final Map<String, String> KNOWN_ABBREVIATIONS = Map.ofEntries(
            Map.entry("customer", "cust"),
            Map.entry("customers", "cust"),
            Map.entry("account", "acct"),
            Map.entry("accounts", "acct"),
            Map.entry("number", "num"),
            Map.entry("amount", "amt"),
            Map.entry("monthly", "mthly"),
            Map.entry("premium", "prem"),
            Map.entry("created", "crtd"),
            Map.entry("during", "dur"),
            Map.entry("previous", "prev"),
            Map.entry("fiscal", "fy"),
            Map.entry("reporting", "rpt"),
            Map.entry("report", "rpt"),
            Map.entry("period", "prd"),
            Map.entry("region", "rgn"),
            Map.entry("regional", "rgnl"),
            Map.entry("business", "biz"),
            Map.entry("units", "unit"),
            Map.entry("department", "dept"),
            Map.entry("documentation", "doc"),
            Map.entry("repository", "repo"),
            Map.entry("system", "sys"),
            Map.entry("transaction", "txn"),
            Map.entry("transactions", "txn"),
            Map.entry("history", "hist"),
            Map.entry("compliance", "cmplnc"),
            Map.entry("regulatory", "reg"),
            Map.entry("average", "avg"),
            Map.entry("summary", "summ"),
            Map.entry("analysis", "anlys"),
            Map.entry("overview", "ovrvw"),
            Map.entry("detail", "dtl"),
            Map.entry("active", "actv"),
            Map.entry("total", "tot")
    );

    private final TokenizerME tokenizer;
    private final POSTaggerME posTagger;

    public ColumnNameNlpShortener(TokenizerME tokenizer, POSTaggerME posTagger) {
        this.tokenizer = tokenizer;
        this.posTagger = posTagger;
    }

    /** Reports both the shortened name and which tier (NLP / Vowel Removal / Not Needed) produced it. */
    public ShortenResult shortenWithTier(String columnName, int maxLength) {
        if (columnName == null || columnName.isBlank() || columnName.length() <= maxLength) {
            return new ShortenResult(columnName, ShortenResult.TIER_NOT_NEEDED);
        }

        String normalized = normalize(columnName);
        if (normalized.isBlank()) {
            return new ShortenResult(columnName, ShortenResult.TIER_NOT_NEEDED);
        }

        List<String> chosen;
        try {
            chosen = nlpFilterNouns(normalized);
        } catch (Exception e) {
            log.warn("OpenNLP noun-filtering failed for '{}', falling back to plain words: {}",
                    columnName, e.toString());
            chosen = List.of(normalized.split("\\s+"));
        }
        if (chosen.isEmpty()) {
            chosen = List.of(normalized.split("\\s+"));
        }

        String result = String.join("_", chosen);
        if (result.length() <= maxLength) {
            return new ShortenResult(result, ShortenResult.TIER_NLP);
        }

        // Known-abbreviation dictionary (still readable), leave unknown words as-is - still tier "NLP"
        List<String> abbreviated = chosen.stream()
                .map(w -> KNOWN_ABBREVIATIONS.getOrDefault(w, w))
                .toList();
        result = String.join("_", abbreviated);
        if (result.length() <= maxLength) {
            return new ShortenResult(result, ShortenResult.TIER_NLP);
        }

        // Last resort before HeaderProcessor's hash fallback
        return new ShortenResult(removeVowels(abbreviated), ShortenResult.TIER_VOWEL_REMOVAL);
    }

    private String normalize(String columnName) {
        return columnName
                .replaceAll("([a-z0-9])([A-Z])", "$1 $2")
                .replaceAll("[_\\-]+", " ")
                .trim()
                .toLowerCase();
    }

    private List<String> nlpFilterNouns(String normalized) {
        String[] tokens = tokenizer.tokenize(normalized);
        if (tokens.length == 0) {
            return List.of();
        }
        String[] posTags = posTagger.tag(tokens);

        List<String> nouns = new ArrayList<>();
        for (int i = 0; i < tokens.length; i++) {
            boolean isNoun = NOUN_TAGS.contains(posTags[i]);
            boolean isStopWord = STOP_WORDS.contains(tokens[i]);
            if (isNoun && !isStopWord) {
                nouns.add(tokens[i]);
            }
        }
        return nouns.isEmpty() ? List.of(tokens) : nouns;
    }

    private String removeVowels(List<String> words) {
        List<String> shortened = new ArrayList<>();
        for (String w : words) {
            if (w.length() <= 2) {
                shortened.add(w);
                continue;
            }
            char first = w.charAt(0);
            String rest = w.substring(1).replaceAll("[aeiouAEIOU]", "");
            shortened.add(first + rest);
        }
        return String.join("_", shortened);
    }
}