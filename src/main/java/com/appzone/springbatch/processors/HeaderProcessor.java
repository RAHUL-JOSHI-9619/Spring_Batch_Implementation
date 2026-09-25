package com.appzone.springbatch.processors;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.ai.chat.client.ChatClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.stereotype.Component;

import com.appzone.springbatch.DTO.CsvHeaderMetadata;

@Component
public class HeaderProcessor {

    // NOTE: The maximum column name length is NOT a constant any more.
    // It depends on the database and its version, so it is passed in as "maxLen".
    // (This class is a singleton, so we pass it as a parameter instead of saving it in a field.)

    // How many long names we send to the AI in one request
    private static final int BATCH_SIZE = 40;

    // How many AI requests can run at the same time
    // (if you get "too many requests" errors, reduce this to 2)
    private static final int THREAD_COUNT = 4;

    // How many column names we show as a preview in the console
    private static final int PREVIEW_COUNT = 10;

    @Autowired
    ChatClient chatClient;

    // ------------------------------------------------------------------
    // Step 1: read the first line of the CSV file and split it by comma
    // ------------------------------------------------------------------
    private String[] getRawHeaders(String csvFilePath) throws Exception {
        if (csvFilePath == null || csvFilePath.isBlank()) {
            throw new IllegalArgumentException("Job parameter 'csvFilePath' is required.");
        }

        String headerLine;
        try (BufferedReader br = new BufferedReader(new FileReader(csvFilePath))) {
            headerLine = br.readLine();
        }

        // Remove the hidden BOM character that Excel sometimes adds at the start of the file
        if (headerLine != null && headerLine.startsWith("\uFEFF")) {
            headerLine = headerLine.substring(1);
        }

        if (headerLine == null || headerLine.trim().isEmpty()) {
            throw new IllegalArgumentException("CSV file is empty or missing a header row.");
        }

        return headerLine.split(",", -1);
    }

    // ------------------------------------------------------------------
    // Step 2: clean all names, shorten the long ones, make all names unique
    // ------------------------------------------------------------------
    private List<String> processHeaders(String csvFilePath, int maxLen) throws Exception {
        long totalStart = System.currentTimeMillis();

        log("==================================================");
        log("Column name processing started for file: " + csvFilePath);
        log("Maximum column name length allowed by the database: " + maxLen);

        // ---------- Read the header row ----------
        log("Reading the header row from the CSV file...");
        String[] rawHeaders = getRawHeaders(csvFilePath);
        log("Total number of columns detected: " + rawHeaders.length);
        logPreview(rawHeaders);

        // ---------- Pass 1: clean the names only (no AI, very fast) ----------
        log("PHASE 1 started: cleaning column names (no AI used)...");
        long phase1Start = System.currentTimeMillis();

        List<String> cleaned = new ArrayList<>();
        Map<String, Integer> columnCounts = new HashMap<>();
        int emptyCount = 0;
        int repeatedCount = 0;

        for (String raw : rawHeaders) {
            String clean = sanitize(raw);

            if (clean.isEmpty()) {
                clean = "unnamed_column";
                emptyCount++;
            }

            if (columnCounts.containsKey(clean)) {
                int count = columnCounts.get(clean) + 1;
                columnCounts.put(clean, count);
                clean = clean + "_" + count;
                repeatedCount++;
            } else {
                columnCounts.put(clean, 0);
            }

            cleaned.add(clean);
        }

        List<String> longNames = cleaned.stream()
                .filter(n -> n.length() > maxLen)
                .distinct()
                .toList();

        long longColumnCount = cleaned.stream().filter(n -> n.length() > maxLen).count();

        log("PHASE 1 finished in " + secondsSince(phase1Start) + " sec: "
                + rawHeaders.length + " names cleaned, "
                + emptyCount + " empty names renamed to 'unnamed_column', "
                + repeatedCount + " repeated names got a number.");
        log("Columns with a name longer than " + maxLen + " characters: " + longColumnCount);

        // ---------- Pass 2: shorten the long names (AI, in groups, in parallel) ----------
        Map<String, String> shortMap = new ConcurrentHashMap<>();
        AtomicInteger failedBatches = new AtomicInteger(0);
        int totalBatches = (longNames.size() + BATCH_SIZE - 1) / BATCH_SIZE;

        if (longNames.isEmpty()) {
            log("PHASE 2 skipped: no column name is longer than " + maxLen + " characters, so AI is not needed.");
        } else {
            log("PHASE 2 started: shortening " + longNames.size() + " long names using AI ("
                    + totalBatches + " AI calls, up to " + BATCH_SIZE + " names per call, "
                    + THREAD_COUNT + " calls at the same time)...");
            long phase2Start = System.currentTimeMillis();

            ExecutorService pool = Executors.newFixedThreadPool(THREAD_COUNT);
            try {
                List<Future<?>> futures = new ArrayList<>();

                for (int i = 0; i < longNames.size(); i += BATCH_SIZE) {
                    List<String> batch = longNames.subList(i, Math.min(i + BATCH_SIZE, longNames.size()));
                    int batchNo = (i / BATCH_SIZE) + 1;
                    futures.add(pool.submit(() ->
                            shortMap.putAll(shortenBatch(batch, maxLen, batchNo, totalBatches, failedBatches))));
                }

                // Wait until every group is finished
                for (Future<?> f : futures) {
                    f.get();
                }
            } finally {
                pool.shutdown();
            }

            log("PHASE 2 finished in " + secondsSince(phase2Start) + " sec: AI was called "
                    + totalBatches + " times for " + longNames.size() + " long names ("
                    + failedBatches.get() + " calls failed, " + shortMap.size() + " answers received).");
        }

        // ---------- Pass 3: apply results, check them, and make sure no name repeats ----------
        log("PHASE 3 started: checking the final names and fixing repeated names...");
        long phase3Start = System.currentTimeMillis();

        Set<String> used = new HashSet<>();
        List<String> result = new ArrayList<>();
        int aiAccepted = 0;
        int fallbackUsed = 0;
        int suffixFixed = 0;

        for (String name : cleaned) {
            String finalName = name;

            if (name.length() > maxLen) {
                finalName = sanitize(shortMap.getOrDefault(name, ""));

                // AI gave nothing, or the answer is still too long -> use the no-AI fallback
                if (finalName.isEmpty() || finalName.length() > maxLen) {
                    finalName = fallback(name, maxLen);
                    fallbackUsed++;
                } else {
                    aiAccepted++;
                }
            }

            // If the name is already taken, add _2, _3, ... (keeping the total length <= maxLen)
            int n = 1;
            String candidate = finalName;
            while (!used.add(candidate)) {
                String suffix = "_" + (++n);
                candidate = finalName.substring(0, Math.min(finalName.length(), maxLen - suffix.length())) + suffix;
            }
            if (!candidate.equals(finalName)) {
                suffixFixed++;
            }

            result.add(candidate);
        }

        log("PHASE 3 finished in " + secondsSince(phase3Start) + " sec: "
                + aiAccepted + " names shortened by AI, "
                + fallbackUsed + " names shortened by the no-AI fallback, "
                + suffixFixed + " names got an extra number to avoid repeats.");

        writeMappingFile(csvFilePath, rawHeaders, result);

        log("DONE: all " + result.size() + " column names are ready. Total time: "
                + secondsSince(totalStart) + " sec.");
        log("==================================================");

        return result;
    }

    // ------------------------------------------------------------------
    // Ask the AI to shorten a whole group of names in ONE request
    // ------------------------------------------------------------------
    private Map<String, String> shortenBatch(List<String> names, int maxLen, int batchNo,
                                             int totalBatches, AtomicInteger failedBatches) {
        String prompt = """
                Shorten each database column name below to at most %d characters,
                preserving its meaning. Use lower_snake_case with only a-z, 0-9 and underscores.
                Return ONLY a JSON object that maps each original name to its shortened name.
                No explanations.
                Names:
                """.formatted(maxLen) + String.join("\n", names);

        Map<String, String> safeResult = new HashMap<>();
        long start = System.currentTimeMillis();
        log("  AI call " + batchNo + "/" + totalBatches + " started (" + names.size() + " names)...");

        try {
            Map<String, String> res = chatClient.prompt()
                    .user(prompt)
                    .call()
                    .entity(new ParameterizedTypeReference<Map<String, String>>() {
                    });

            if (res != null) {
                // ConcurrentHashMap does not allow null, so skip any null key or value
                for (Map.Entry<String, String> e : res.entrySet()) {
                    if (e.getKey() != null && e.getValue() != null) {
                        safeResult.put(e.getKey(), e.getValue());
                    }
                }
            }

            log("  AI call " + batchNo + "/" + totalBatches + " finished in "
                    + secondsSince(start) + " sec: received " + safeResult.size() + " answers.");
        } catch (Exception e) {
            // If this group fails, Pass 3 will use fallback() for these names
            failedBatches.incrementAndGet();
            log("  AI call " + batchNo + "/" + totalBatches + " FAILED after "
                    + secondsSince(start) + " sec (" + e.getMessage()
                    + "). The fallback will be used for these names.");
        }
        return safeResult;
    }

    // ------------------------------------------------------------------
    // Small helper methods
    // ------------------------------------------------------------------

    // Lower case, keep only a-z, 0-9 and single underscores, remove _ at start and end
    private String sanitize(String s) {
        if (s == null) {
            return "";
        }
        return s.toLowerCase()
                .trim()
                .replaceAll("[^a-z0-9]+", "_")
                .replaceAll("^_+|_+$", "");
    }

    // No-AI shortening: cut the name and add a small code made from the name
    // Same name always gives the same result.
    private String fallback(String name, int maxLen) {
        String hash = Integer.toHexString(name.hashCode() & 0xFFFF); // up to 4 characters
        return name.substring(0, maxLen - hash.length() - 1) + "_" + hash;
    }

    // Put quotes around a value if it contains a comma, a quote, or a new line
    private String csvEscape(String value) {
        if (value == null) {
            return "";
        }
        if (value.contains(",") || value.contains("\"") || value.contains("\n") || value.contains("\r")) {
            return "\"" + value.replace("\"", "\"\"") + "\"";
        }
        return value;
    }

    // Print one line to the console
    private void log(String message) {
        System.out.println("[HeaderProcessor] " + message);
    }

    // Time passed since "startMs", in seconds, with 2 decimals
    private String secondsSince(long startMs) {
        return String.format("%.2f", (System.currentTimeMillis() - startMs) / 1000.0);
    }

    // Show the first few raw column names, so you can see what was read from the file
    private void logPreview(String[] rawHeaders) {
        int show = Math.min(PREVIEW_COUNT, rawHeaders.length);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < show; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append("[").append(rawHeaders[i]).append("]");
        }
        if (rawHeaders.length > show) {
            sb.append(" ... and ").append(rawHeaders.length - show).append(" more");
        }
        log("Column names found: " + sb);
    }

    // ------------------------------------------------------------------
    // Create the mapping CSV in Downloads/Column_name_matchings/<csv_file_name>/
    // ------------------------------------------------------------------
    private void writeMappingFile(String csvFilePath, String[] rawHeaders, List<String> cleanHeaders)
            throws Exception {

        // 1. Get user's Downloads folder
        String userHome = System.getProperty("user.home");
        Path downloadsFolder = Paths.get(userHome, "Downloads");

        // 2. Create Column_name_matchings folder
        Path matchingFolder = downloadsFolder.resolve("Column_name_matchings");
        Files.createDirectories(matchingFolder);

        // 3. Get CSV file name without extension
        Path csvPath = Paths.get(csvFilePath);
        String csvFileName = csvPath.getFileName().toString();
        int dotIndex = csvFileName.lastIndexOf('.');
        String folderName = (dotIndex > 0) ? csvFileName.substring(0, dotIndex) : csvFileName;

        // 4. Create folder using CSV file name
        Path csvMatchingFolder = matchingFolder.resolve(folderName);
        Files.createDirectories(csvMatchingFolder);

        // 5. Create mapping CSV file
        Path mappingFile = csvMatchingFolder.resolve("column_name_mappings_" + folderName + ".csv");

        // 6. Write mappings
        try (BufferedWriter writer = Files.newBufferedWriter(mappingFile)) {
            writer.write("raw_column_name,cleaned_column_name");
            writer.newLine();

            for (int i = 0; i < rawHeaders.length; i++) {
                writer.write(csvEscape(rawHeaders[i]) + "," + csvEscape(cleanHeaders.get(i)));
                writer.newLine();
            }
        }

        log("Mapping file created at: " + mappingFile);
    }

    public CsvHeaderMetadata extractMetadata(String csvFilePath, int maxLen) throws Exception {
        List<String> headers = processHeaders(csvFilePath, maxLen);
        return new CsvHeaderMetadata(headers);
    }

}