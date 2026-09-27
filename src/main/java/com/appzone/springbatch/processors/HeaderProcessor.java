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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.appzone.springbatch.DTO.CsvHeaderMetadata;
import com.appzone.springbatch.nlp.AiColumnNameBatchShortener;
import com.appzone.springbatch.nlp.ColumnNameNlpShortener;
import com.appzone.springbatch.nlp.ShortenResult;
import com.appzone.springbatch.util.ReservedSqlKeywords;

@Component
public class HeaderProcessor {

    private static final int PREVIEW_COUNT = 10;
    private static final String RESERVED_WORD_SUFFIX = "_col";

    @Autowired
    ColumnNameNlpShortener columnNameNlpShortener;

    @Autowired
    AiColumnNameBatchShortener aiColumnNameBatchShortener;

    private String[] getRawHeaders(String csvFilePath) throws Exception {
        if (csvFilePath == null || csvFilePath.isBlank()) {
            throw new IllegalArgumentException("Job parameter 'csvFilePath' is required.");
        }

        String headerLine;
        try (BufferedReader br = new BufferedReader(new FileReader(csvFilePath))) {
            headerLine = br.readLine();
        }

        if (headerLine != null && headerLine.startsWith("\uFEFF")) {
            headerLine = headerLine.substring(1);
        }

        if (headerLine == null || headerLine.trim().isEmpty()) {
            throw new IllegalArgumentException("CSV file is empty or missing a header row.");
        }

        return headerLine.split(",", -1);
    }

    private List<String> processHeaders(String csvFilePath, int maxLen) throws Exception {
        long totalStart = System.currentTimeMillis();

        log("==================================================");
        log("Column name processing started for file: " + csvFilePath);
        log("Maximum column name length allowed by the database: " + maxLen);

        log("Reading the header row from the CSV file...");
        String[] rawHeaders = getRawHeaders(csvFilePath);
        log("Total number of columns detected: " + rawHeaders.length);
        logPreview(rawHeaders);

        log("PHASE 1 started: cleaning column names and checking for reserved SQL keywords (no AI used)...");
        long phase1Start = System.currentTimeMillis();

        List<String> cleaned = new ArrayList<>();
        Map<String, Integer> columnCounts = new HashMap<>();
        int emptyCount = 0;
        int repeatedCount = 0;
        int reservedWordCount = 0;

        for (String raw : rawHeaders) {
            String clean = sanitize(raw);

            if (clean.isEmpty()) {
                clean = "unnamed_column";
                emptyCount++;
            }

            if (ReservedSqlKeywords.isReserved(clean)) {
                clean = clean + RESERVED_WORD_SUFFIX;
                reservedWordCount++;
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
                + reservedWordCount + " reserved-keyword names got a '" + RESERVED_WORD_SUFFIX + "' suffix, "
                + repeatedCount + " repeated names got a number.");
        log("Columns with a name longer than " + maxLen + " characters: " + longColumnCount);

        // ---------- Pass 2: shorten the long names ----------
        // Tier 1 (optional): AI, batched, Gemini then Ollama, each call bounded by a hard timeout.
        // Tier 2: the existing no-AI NLP pipeline, used for whatever AI did not resolve.
        Map<String, ShortenResult> shortMap = new ConcurrentHashMap<>();
        AtomicInteger nlpFailures = new AtomicInteger(0);

        if (longNames.isEmpty()) {
            log("PHASE 2 skipped: no column name is longer than " + maxLen + " characters, so shortening is not needed.");
        } else {
            log("PHASE 2 started: shortening " + longNames.size() + " long names (AI tier first, NLP tier for the rest)...");
            long phase2Start = System.currentTimeMillis();

            Map<String, ShortenResult> aiShortened = aiColumnNameBatchShortener.shortenBatch(longNames, maxLen);
            shortMap.putAll(aiShortened);

            List<String> stillLong = longNames.stream()
                    .filter(n -> !shortMap.containsKey(n))
                    .toList();

            if (!stillLong.isEmpty()) {
                log("  " + stillLong.size() + " name(s) not resolved by AI -> running the NLP pipeline on them...");
                stillLong.parallelStream().forEach(name -> {
                    try {
                        ShortenResult r = columnNameNlpShortener.shortenWithTier(name, maxLen);
                        if (r != null && r.shortenedName() != null && !r.shortenedName().isBlank()) {
                            shortMap.put(name, r);
                        }
                    } catch (Exception e) {
                        nlpFailures.incrementAndGet();
                        log("  NLP shortening FAILED for '" + name + "' (" + e.getMessage()
                                + "). The hash fallback will be used for this name.");
                    }
                });
            }

            log("PHASE 2 finished in " + secondsSince(phase2Start) + " sec: " + longNames.size()
                    + " long name(s) processed (" + aiShortened.size() + " via AI, "
                    + (shortMap.size() - aiShortened.size()) + " via NLP, "
                    + nlpFailures.get() + " NLP failures, "
                    + (longNames.size() - shortMap.size()) + " will use the hash fallback).");
        }

        log("PHASE 3 started: checking the final names and fixing repeated names...");
        long phase3Start = System.currentTimeMillis();

        Set<String> used = new HashSet<>();
        List<String> result = new ArrayList<>();
        List<String> tiers = new ArrayList<>();
        int suffixFixed = 0;

        for (String name : cleaned) {
            String finalName = name;
            String tier;

            if (name.length() > maxLen) {
                ShortenResult sr = shortMap.get(name);
                finalName = sanitize(sr != null ? sr.shortenedName() : "");

                if (finalName.isEmpty() || finalName.length() > maxLen) {
                    finalName = fallback(name, maxLen);
                    tier = ShortenResult.TIER_HASH_FALLBACK;
                } else {
                    tier = sr.tier();
                }
            } else {
                tier = ShortenResult.TIER_NOT_NEEDED;
            }

            if (ReservedSqlKeywords.isReserved(finalName)) {
                int budget = Math.max(0, maxLen - RESERVED_WORD_SUFFIX.length());
                finalName = finalName.substring(0, Math.min(finalName.length(), budget)) + RESERVED_WORD_SUFFIX;
            }

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
            tiers.add(tier);
        }

        Map<String, Long> tierCounts = tiers.stream()
                .collect(Collectors.groupingBy(t -> t, Collectors.counting()));
        String tierBreakdown = tierCounts.entrySet().stream()
                .map(e -> e.getKey() + ": " + e.getValue())
                .collect(Collectors.joining(", "));

        log("PHASE 3 finished in " + secondsSince(phase3Start) + " sec: "
                + suffixFixed + " names got an extra number to avoid repeats. Tier breakdown -> " + tierBreakdown);

        writeMappingFile(csvFilePath, rawHeaders, result, tiers);

        log("DONE: all " + result.size() + " column names are ready. Total time: "
                + secondsSince(totalStart) + " sec.");
        log("==================================================");

        return result;
    }

    private String sanitize(String s) {
        if (s == null) {
            return "";
        }
        return s
                .replaceAll("([a-z0-9])([A-Z])", "$1_$2")
                .toLowerCase()
                .trim()
                .replaceAll("[^a-z0-9]+", "_")
                .replaceAll("^_+|_+$", "");
    }

    private String fallback(String name, int maxLen) {
        String hash = Integer.toHexString(name.hashCode() & 0xFFFF);
        return name.substring(0, maxLen - hash.length() - 1) + "_" + hash;
    }

    private String csvEscape(String value) {
        if (value == null) {
            return "";
        }
        if (value.contains(",") || value.contains("\"") || value.contains("\n") || value.contains("\r")) {
            return "\"" + value.replace("\"", "\"\"") + "\"";
        }
        return value;
    }

    private void log(String message) {
        System.out.println("[HeaderProcessor] " + message);
    }

    private String secondsSince(long startMs) {
        return String.format("%.2f", (System.currentTimeMillis() - startMs) / 1000.0);
    }

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

    private void writeMappingFile(String csvFilePath, String[] rawHeaders, List<String> cleanHeaders, List<String> tiers)
            throws Exception {

        String userHome = System.getProperty("user.home");
        Path downloadsFolder = Paths.get(userHome, "Downloads");

        Path matchingFolder = downloadsFolder.resolve("Column_name_matchings");
        Files.createDirectories(matchingFolder);

        Path csvPath = Paths.get(csvFilePath);
        String csvFileName = csvPath.getFileName().toString();
        int dotIndex = csvFileName.lastIndexOf('.');
        String folderName = (dotIndex > 0) ? csvFileName.substring(0, dotIndex) : csvFileName;

        Path csvMatchingFolder = matchingFolder.resolve(folderName);
        Files.createDirectories(csvMatchingFolder);

        Path mappingFile = csvMatchingFolder.resolve("column_name_mappings_" + folderName + ".csv");

        try (BufferedWriter writer = Files.newBufferedWriter(mappingFile)) {
            writer.write("raw_column_name,cleaned_column_name,shortening_method");
            writer.newLine();

            for (int i = 0; i < rawHeaders.length; i++) {
                writer.write(csvEscape(rawHeaders[i]) + "," + csvEscape(cleanHeaders.get(i))
                        + "," + csvEscape(tiers.get(i)));
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