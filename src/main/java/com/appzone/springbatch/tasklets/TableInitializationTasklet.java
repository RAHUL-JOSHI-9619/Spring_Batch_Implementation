package com.appzone.springbatch.tasklets;

import org.springframework.batch.core.step.StepContribution;

import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.infrastructure.repeat.RepeatStatus;
import org.springframework.jdbc.core.JdbcTemplate;

import com.appzone.springbatch.processors.HeaderProcessor;


import java.util.*;

public class TableInitializationTasklet implements Tasklet {

    private final String tableName;
    private final JdbcTemplate jdbcTemplate;
    
    private final List<String> SanitizedHeaders;

    public TableInitializationTasklet(String tableName, JdbcTemplate jdbcTemplate, List<String> SanitizedHeaders) {
        this.tableName = tableName;
        this.jdbcTemplate = jdbcTemplate;
        
        this.SanitizedHeaders = SanitizedHeaders;
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        String csvFilePath = contribution.getStepExecution()
                                         .getJobParameters()
                                         .getString("csvFilePath");

//        if (csvFilePath == null || csvFilePath.isBlank()) {
//            throw new IllegalArgumentException("Job parameter 'csvFilePath' is required.");
//        }
//
//        String headerLine;
//        try (BufferedReader br = new BufferedReader(new FileReader(csvFilePath))) {
//            headerLine = br.readLine();
//        }
//
//        if (headerLine == null || headerLine.trim().isEmpty()) {
//            throw new IllegalArgumentException("CSV file is empty or missing a header row.");
//        }
//
//        String[] rawHeaders = headerLine.split(",");
        
		List<String> sanitizedHeaders = SanitizedHeaders;

        // Pass headers to chunk step via JobExecutionContext
        chunkContext.getStepContext()
                    .getStepExecution()
                    .getJobExecution()
                    .getExecutionContext()
                    .put("cleanHeaders", sanitizedHeaders);

        // Execute dynamic DDL
        jdbcTemplate.execute("DROP TABLE IF EXISTS `" + tableName + "`");
        String createTableSql = buildCreateTableSql(tableName, sanitizedHeaders);
        jdbcTemplate.execute(createTableSql);

        return RepeatStatus.FINISHED;
    }

//    private List<String> processHeaders(String[] rawHeaders) {
//        List<String> cleanHeaders = new ArrayList<>();
//        Map<String, Integer> columnCounts = new HashMap<>();
//
//        for (String raw : rawHeaders) {
//            String clean = raw.toLowerCase()
//                             .trim()
//                             .replaceAll("[^a-z0-9]+", "_")
//                             .replaceAll("^_+|_+$", "");
//
//            if (clean.isEmpty()) {
//                clean = "unnamed_column";
//            }
//
//            if (columnCounts.containsKey(clean)) {
//                int count = columnCounts.get(clean) + 1;
//                columnCounts.put(clean, count);
//                clean = clean + "_" + count;
//            } else {
//                columnCounts.put(clean, 0);
//            }
//            	if (clean.length() > 64) {
//				clean = shortenWithAI(clean);
//				}
//            cleanHeaders.add(clean);
//        }
//
//        return cleanHeaders;
//    }
//
//    private String shortenWithAI(String longColumnName) {
//        try {
//            String prompt = "Shorten the following database column name to under 50 characters while preserving its meaning. " +
//                            "Use lower_snake_case with only alphanumeric characters and underscores. " +
//                            "Return ONLY the shortened string with no explanations or punctuation.\n" +
//                            "Column name: " + longColumnName;
//
//            String response = chatClient.prompt()
//                    .user(prompt)
//                    .call()
//                    .content();
//
//            if (response != null && !response.isBlank()) {
//                // Sanitize the AI response to ensure valid SQL column format
//                String shortened = response.toLowerCase()
//                                          .trim()
//                                          .replaceAll("[^a-z0-9]+", "_")
//                                          .replaceAll("^_+|_+$", "");
//                
//                // Hard fallback safeguard just in case AI returns > 64 chars
//                return shortened.length() > 64 ? shortened.substring(0, 64) : shortened;
//            }
//        } catch (Exception e) {
//            // Fallback gracefully to basic substring if AI service call fails/times out
//            System.err.println("AI call failed for column shortening, using substring fallback: " + e.getMessage());
//        }
//
//        return longColumnName.substring(0, 64);
//    }

	private String buildCreateTableSql(String tableName, List<String> headers) {
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE IF NOT EXISTS `").append(tableName).append("` (\n");
        

        for (int i = 0; i < headers.size(); i++) {
            sql.append("  `").append(headers.get(i)).append("` TEXT");
            if (i < headers.size() - 1) {
                sql.append(",\n");
            }
        }
        sql.append("\n);");
        return sql.toString();
    }
}
