package com.appzone.springbatch.processors;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.ai.chat.client.ChatClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class HeaderProcessor {
	@Autowired
	ChatClient chatClient;
	
	private String[] getRawHeaders(String csvFilePath) throws Exception {
	if (csvFilePath == null || csvFilePath.isBlank()) {
        throw new IllegalArgumentException("Job parameter 'csvFilePath' is required.");
    }

    String headerLine;
    try (BufferedReader br = new BufferedReader(new FileReader(csvFilePath))) {
        headerLine = br.readLine();
    }

    if (headerLine == null || headerLine.trim().isEmpty()) {
        throw new IllegalArgumentException("CSV file is empty or missing a header row.");
    }

    String[] rawHeaders = headerLine.split(",",-1);
    
    return rawHeaders;
	}
	
	
    public List<String> processHeaders(String csvFilePath) throws Exception {
    	String[] rawHeaders = getRawHeaders(csvFilePath);
        List<String> cleanHeaders = new ArrayList<>();
        Map<String, Integer> columnCounts = new HashMap<>();

        for (String raw : rawHeaders) {
            String clean = raw.toLowerCase()
                             .trim()
                             .replaceAll("[^a-z0-9]+", "_")
                             .replaceAll("^_+|_+$", "");

            if (clean.isEmpty()) {
                clean = "unnamed_column";
            }

            if (columnCounts.containsKey(clean)) {
                int count = columnCounts.get(clean) + 1;
                columnCounts.put(clean, count);
                clean = clean + "_" + count;
            } else {
                columnCounts.put(clean, 0);
            }
            	if (clean.length() > 64) {
				clean = shortenWithAI(clean);
				}
            cleanHeaders.add(clean);
        }

        return cleanHeaders;
    }
    
    public List<String> processHeaders(String[] rawHeaders) {
    	
        List<String> cleanHeaders = new ArrayList<>();
        Map<String, Integer> columnCounts = new HashMap<>();

        for (String raw : rawHeaders) {
            String clean = raw.toLowerCase()
                             .trim()
                             .replaceAll("[^a-z0-9]+", "_")
                             .replaceAll("^_+|_+$", "");

            if (clean.isEmpty()) {
                clean = "unnamed_column";
            }

            if (columnCounts.containsKey(clean)) {
                int count = columnCounts.get(clean) + 1;
                columnCounts.put(clean, count);
                clean = clean + "_" + count;
            } else {
                columnCounts.put(clean, 0);
            }
            	if (clean.length() > 64) {
				clean = shortenWithAI(clean);
				}
            cleanHeaders.add(clean);
        }

        return cleanHeaders;
    }

    private String shortenWithAI(String longColumnName) {
        try {
            String prompt = "Shorten the following database column name to under 50 characters while preserving its meaning. " +
                            "Use lower_snake_case with only alphanumeric characters and underscores. " +
                            "Return ONLY the shortened string with no explanations or punctuation.\n" +
                            "Column name: " + longColumnName;

            String response = chatClient.prompt()
                    .user(prompt)
                    .call()
                    .content();

            if (response != null && !response.isBlank()) {
                // Sanitize the AI response to ensure valid SQL column format
                String shortened = response.toLowerCase()
                                          .trim()
                                          .replaceAll("[^a-z0-9]+", "_")
                                          .replaceAll("^_+|_+$", "");
                
                // Hard fallback safeguard just in case AI returns > 64 chars
                return shortened.length() > 64 ? shortened.substring(0, 64) : shortened;
            }
        } catch (Exception e) {
            // Fallback gracefully to basic substring if AI service call fails/times out
            System.err.println("AI call failed for column shortening, using substring fallback: " + e.getMessage());
        }

        return longColumnName.substring(0, 64);
    }
	
}
