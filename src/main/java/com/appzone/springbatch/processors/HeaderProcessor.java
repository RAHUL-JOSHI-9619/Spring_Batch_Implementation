package com.appzone.springbatch.processors;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.ai.chat.client.ChatClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.appzone.springbatch.DTO.CsvHeaderMetadata;

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
	
	
    private List<String> processHeaders(String csvFilePath) throws Exception {
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
        
        //added code for creating the mapping CSV file in Downloads/Column_name_matchings/<csv_file_name>/column_name_mappings.csv
        
        
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

           String folderName;

           if (dotIndex > 0) {
               folderName = csvFileName.substring(0, dotIndex);
           } else {
               folderName = csvFileName;
           }

           // 4. Create folder using CSV file name
           Path csvMatchingFolder = matchingFolder.resolve(folderName);

           Files.createDirectories(csvMatchingFolder);

           // 5. Create mapping CSV file
           Path mappingFile = csvMatchingFolder.resolve("column_name_mappings_"+folderName+".csv");

           // 6. Write mappings
           try (BufferedWriter writer = Files.newBufferedWriter(mappingFile)) {

               // Header row
               writer.write("raw_column_name,cleaned_column_name");
               writer.newLine();

               // Mapping rows
               for (int i = 0; i < rawHeaders.length; i++) {

                   String rawColumn = rawHeaders[i];
                   String cleanColumn = cleanHeaders.get(i);

                   writer.write(
                           rawColumn
                           + ","
                           + cleanColumn
                   );

                   writer.newLine();
               }
           }

           System.out.println("Mapping file created at:");
           System.out.println(mappingFile);
           
           
           
           
           //up to here it is added code.
           

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
    
    
    
    public CsvHeaderMetadata extractMetadata(String csvFilePath) throws Exception {
        List<String> headers = processHeaders(csvFilePath);
        return new CsvHeaderMetadata(headers);
    }
	
}
