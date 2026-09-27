package com.appzone.springbatch.nlp;

/**
 * Which tier produced a shortened column name, plus the name itself. Used purely for
 * reporting - HeaderProcessor writes the tier into the mapping CSV's third column so
 * it's obvious, per column, whether AI, Ollama, NLP, vowel-stripping, or the hash
 * fallback did the work (or whether shortening wasn't needed at all).
 */
public record ShortenResult(String shortenedName, String tier) {
    public static final String TIER_AI_GEMINI = "AI (Gemini)";
   
    public static final String TIER_NLP = "NLP";
    public static final String TIER_VOWEL_REMOVAL = "Vowel Removal";
    public static final String TIER_HASH_FALLBACK = "Hash Fallback";
    public static final String TIER_NOT_NEEDED = "Not Needed (already short)";
}