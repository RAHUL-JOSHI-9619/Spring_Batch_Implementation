package com.appzone.springbatch.nlp;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * AI-assisted column-name shortening: an OPTIONAL tier that runs BEFORE the deterministic
 * pipeline in ColumnNameNlpShortener. Single provider: Gemini, bounded by a hard timeout.
 * Whatever Gemini doesn't resolve is left out of the returned map - the caller
 * (HeaderProcessor) runs those names through the NLP pipeline exactly as before.
 *
 * (Ollama support was removed - this used to have a two-provider Gemini-then-Ollama flow.)
 *
 * RESPONSE FORMAT: a POSITIONAL JSON ARRAY, not an object keyed by the input name.
 * An earlier object-keyed version ("{inputName: shortenedName}") occasionally lost
 * entries even when the model clearly tried to answer them, because the model doesn't
 * always echo a long input key back byte-for-byte (trimmed whitespace, a re-ordered
 * underscore, different casing) - so Map.get(originalName) silently returned null for
 * names the model DID shorten. A positional array sidesteps that entirely: output[i]
 * always corresponds to input[i], with no string matching involved. If the array length
 * doesn't match what was sent, the whole batch is treated as unresolved (safer than
 * guessing which entries are misaligned) and falls through to NLP.
 *
 * WHY A HARD TIMEOUT MATTERS: on a private VPN that permits general traffic but silently
 * black-holes calls to Gemini's endpoint, a plain HTTP client timeout can be unreliable.
 * Future.get(timeoutSeconds) in callWithTimeout guarantees we give up after aiTimeoutSeconds
 * no matter what the network is doing underneath.
 *
 * WHY TWO SEPARATE EXECUTORS: batchExecutor runs one task per batch; each of those tasks
 * itself calls Future.get() on a SEPARATE callExecutor for the actual AI call. Sharing one
 * pool for both would deadlock once enough batches are in flight simultaneously.
 */
@Service
public class AiColumnNameBatchShortener {

    private static final Logger log = LoggerFactory.getLogger(AiColumnNameBatchShortener.class);

    // Lowered from 25 to 20: smaller batches are less likely to hit an output-length /
    // truncation limit on smaller models (e.g. gemini-*-flash-lite).
    private static final int BATCH_SIZE = 20;
    private static final int BATCH_POOL_SIZE = 6;

    private final ChatClient geminiChatClient;
    private final ObjectMapper objectMapper = new ObjectMapper();

    private final ExecutorService batchExecutor =
            Executors.newFixedThreadPool(BATCH_POOL_SIZE, daemonThreadFactory("ai-shortener-batch"));
    private final ExecutorService callExecutor =
            Executors.newCachedThreadPool(daemonThreadFactory("ai-shortener-call"));

    @Value("${app.column-shortening.ai-enabled:true}")
    private boolean aiEnabled;

    @Value("${app.column-shortening.ai-timeout-seconds:15}")
    private int aiTimeoutSeconds;

    public AiColumnNameBatchShortener(ChatClient geminiChatClient) {
        this.geminiChatClient = geminiChatClient;
    }

    /**
     * @return a map of longName -> ShortenResult (shortened name + which AI tier produced it)
     *         for whichever names Gemini succeeded on. Any name NOT in the returned map should
     *         be run through the NLP pipeline by the caller. Never throws.
     */
    public Map<String, ShortenResult> shortenBatch(List<String> names, int maxLen) {
        Map<String, ShortenResult> results = new ConcurrentHashMap<>();

        if (!aiEnabled) {
            log.info("[AiColumnNameBatchShortener] AI shortening disabled (app.column-shortening.ai-enabled=false) - going straight to the NLP pipeline.");
            return results;
        }
        if (names.isEmpty()) {
            return results;
        }

        List<List<String>> batches = partition(names, BATCH_SIZE);
        log.info("[AiColumnNameBatchShortener] Submitting {} long name(s) in {} batch(es) of up to {} (Gemini only)...",
                names.size(), batches.size(), BATCH_SIZE);

        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (int i = 0; i < batches.size(); i++) {
            List<String> batch = batches.get(i);
            int batchNum = i + 1;
            futures.add(CompletableFuture.runAsync(
                    () -> processBatch(batch, batchNum, batches.size(), maxLen, results),
                    batchExecutor));
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        log.info("[AiColumnNameBatchShortener] AI shortening finished: {}/{} name(s) shortened by AI, {} will fall through to the NLP pipeline.",
                results.size(), names.size(), names.size() - results.size());

        return results;
    }

    private void processBatch(List<String> batch, int batchNum, int totalBatches, int maxLen,
                               Map<String, ShortenResult> results) {
        log.info("  [AI batch {}/{}] {} name(s) -> trying Gemini (timeout {}s)...",
                batchNum, totalBatches, batch.size(), aiTimeoutSeconds);

        Map<String, String> geminiRaw = callWithTimeout(
                () -> callProvider(geminiChatClient, batch, maxLen), "Gemini", aiTimeoutSeconds);
        Map<String, String> geminiAccepted = validate(geminiRaw, batch, maxLen);
        geminiAccepted.forEach((name, val) -> results.put(name, new ShortenResult(val, ShortenResult.TIER_AI_GEMINI)));

        int stillMissing = batch.size() - geminiAccepted.size();
        log.info("  [AI batch {}/{}] Gemini returned {}/{} usable name(s). {} name(s) will fall through to NLP.",
                batchNum, totalBatches, geminiAccepted.size(), batch.size(), stillMissing);
    }

    /**
     * Enforces a HARD timeout via Future.get(timeoutSeconds) regardless of what the network is
     * doing - this is what guarantees Gemini can never hang a batch indefinitely, VPN or no VPN.
     */
    private Map<String, String> callWithTimeout(Callable<Map<String, String>> call, String providerLabel,
                                                  int timeoutSeconds) {
        Future<Map<String, String>> future = callExecutor.submit(call);
        try {
            return future.get(timeoutSeconds, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            future.cancel(true);
            log.warn("  {} call TIMED OUT after {}s (possible causes: no internet, or a VPN silently dropping the request) - falling through.",
                    providerLabel, timeoutSeconds);
            return Map.of();
        } catch (Exception e) {
            log.warn("  {} call FAILED: {} - falling through.", providerLabel, e.toString());
            return Map.of();
        }
    }

    /** One prompt, one JSON-in/JSON-array-out round trip for an entire batch. See class javadoc for why an array, not an object. */
    private Map<String, String> callProvider(ChatClient client, List<String> names, int maxLen) throws Exception {
        String namesJson = objectMapper.writeValueAsString(names);
        int n = names.size();

        String prompt = """
                You are shortening database column names so they fit within %d characters
                while staying as human-readable as possible.

                Rules:
                - Each output name must be lowercase, use underscores between words, contain
                  only letters a-z, digits 0-9 and underscores, and be AT MOST %d characters long.
                - Prefer meaningful abbreviation of the most important words over abbreviating
                  every single word.
                - Keep the word(s) that carry the actual meaning of the column; it is fine to
                  drop filler words (the, of, during, across, all, previous) entirely.
                - Do not invent words that are not implied by the input name.
                - Respond with ONLY a JSON array of EXACTLY %d strings, in the SAME ORDER as
                  the input array below (output[i] is the shortened version of input[i]).
                  If you cannot shorten a particular name, put an empty string "" at that
                  position - do NOT skip it, do NOT omit it, the array length must always
                  equal %d. No markdown code fences, no explanation, no extra text.

                Input names (JSON array, %d items):
                %s
                """.formatted(maxLen, maxLen, n, n, n, namesJson);

        String response = client.prompt()
                .user(prompt)
                .call()
                .content();

        String cleaned = response == null ? "[]" : response.trim()
                .replaceAll("(?i)^```json", "")
                .replaceAll("^```", "")
                .replaceAll("```$", "")
                .trim();

        List<String> parsed;
        try {
            parsed = objectMapper.readValue(cleaned,
                    objectMapper.getTypeFactory().constructCollectionType(List.class, String.class));
        } catch (Exception e) {
            log.warn("  Could not parse AI response as a JSON array of {} strings: {}", n, e.toString());
            return Map.of();
        }

        if (parsed.size() != n) {
            log.warn("  AI returned {} name(s) but {} were sent - array length mismatch, treating whole batch as unresolved.",
                    parsed.size(), n);
            return Map.of();
        }

        Map<String, String> byName = new LinkedHashMap<>();
        for (int i = 0; i < n; i++) {
            String candidate = parsed.get(i);
            if (candidate != null && !candidate.isBlank()) {
                byName.put(names.get(i), candidate);
            }
        }
        return byName;
    }

    /** Never trust AI output blindly - same lesson as the earlier lemmatizer bug. */
    private Map<String, String> validate(Map<String, String> raw, List<String> expectedNames, int maxLen) {
        Map<String, String> valid = new LinkedHashMap<>();
        for (String name : expectedNames) {
            String candidate = raw.get(name);
            if (candidate == null || candidate.isBlank()) {
                log.info("    REJECTED (blank/missing) for '{}'", name);
                continue;
            }
            String sanitized = candidate.trim().toLowerCase()
                    .replaceAll("[^a-z0-9]+", "_")
                    .replaceAll("^_+|_+$", "");
            if (sanitized.isEmpty() || sanitized.length() > maxLen) {
                log.info("    REJECTED (too long: {} chars, limit {}) for '{}' -> raw AI output was: '{}'",
                        sanitized.length(), maxLen, name, candidate);
                continue;
            }
            valid.put(name, sanitized);
        }
        return valid;
    }

    private static List<List<String>> partition(List<String> list, int size) {
        List<List<String>> result = new ArrayList<>();
        for (int i = 0; i < list.size(); i += size) {
            result.add(list.subList(i, Math.min(i + size, list.size())));
        }
        return result;
    }

    private static java.util.concurrent.ThreadFactory daemonThreadFactory(String namePrefix) {
        java.util.concurrent.atomic.AtomicInteger counter = new java.util.concurrent.atomic.AtomicInteger(1);
        return r -> {
            Thread t = new Thread(r, namePrefix + "-" + counter.getAndIncrement());
            t.setDaemon(true);
            return t;
        };
    }

    @PreDestroy
    public void shutdown() {
        batchExecutor.shutdownNow();
        callExecutor.shutdownNow();
    }
}