package com.appzone.springbatch.tasklets;

import org.springframework.batch.core.step.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.infrastructure.repeat.RepeatStatus;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.DatabaseMetaData;
import java.util.List;

public class TableInitializationTasklet implements Tasklet {

    private final String tableName;
    private final JdbcTemplate jdbcTemplate;
    private final List<String> sanitizedHeaders;

    public TableInitializationTasklet(String tableName, JdbcTemplate jdbcTemplate, List<String> sanitizedHeaders) {
        this.tableName = tableName;
        this.jdbcTemplate = jdbcTemplate;
        this.sanitizedHeaders = sanitizedHeaders;
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {

        // Pass headers to chunk step via JobExecutionContext
        chunkContext.getStepContext()
                    .getStepExecution()
                    .getJobExecution()
                    .getExecutionContext()
                    .put("cleanHeaders", sanitizedHeaders);

        // Fetch DB metadata for dialect handling
        DatabaseInfo dbInfo = jdbcTemplate.execute((java.sql.Connection con) -> {
            DatabaseMetaData metaData = con.getMetaData();
            String productName = metaData.getDatabaseProductName().toLowerCase();
            String quote = metaData.getIdentifierQuoteString();

            if (quote == null || quote.trim().isEmpty()) {
                quote = "\"";
            }

            return new DatabaseInfo(productName, quote);
        });

        // 1. Drop existing table if present (dialect aware)
        dropTableIfExists(tableName, dbInfo);

        // 2. Build and execute CREATE TABLE statement
        String createTableSql = buildCreateTableSql(tableName, sanitizedHeaders, dbInfo);
        jdbcTemplate.execute(createTableSql);

        return RepeatStatus.FINISHED;
    }

    private void dropTableIfExists(String tableName, DatabaseInfo dbInfo) {
        String quotedTableName = quoteIdentifier(tableName, dbInfo);

        if (dbInfo.isOracle()) {
            // Oracle safe table drop
            try {
                jdbcTemplate.execute("DROP TABLE " + quotedTableName);
            } catch (Exception e) {
                // Ignore exception if the table doesn't exist in Oracle
            }
        } else if (dbInfo.isPostgres() || dbInfo.isMysql()) {
            jdbcTemplate.execute("DROP TABLE IF EXISTS " + quotedTableName);
        } else {
            // Generic fallback
            try {
                jdbcTemplate.execute("DROP TABLE " + quotedTableName);
            } catch (Exception e) {
                // Table probably did not exist
            }
        }
    }

    private String buildCreateTableSql(String tableName, List<String> headers, DatabaseInfo dbInfo) {
        StringBuilder sql = new StringBuilder();
        String quotedTableName = quoteIdentifier(tableName, dbInfo);
        String textDataType = getTextDataType(dbInfo);

        sql.append("CREATE TABLE ").append(quotedTableName).append(" (\n");

        for (int i = 0; i < headers.size(); i++) {
            String quotedColumnHeader = quoteIdentifier(headers.get(i), dbInfo);
            sql.append("  ").append(quotedColumnHeader).append(" ").append(textDataType);
            if (i < headers.size() - 1) {
                sql.append(",\n");
            }
        }
        sql.append("\n)");

        return sql.toString();
    }

    private String quoteIdentifier(String identifier, DatabaseInfo dbInfo) {
        if (dbInfo.isOracle() && !identifier.contains(" ")) {
            // Oracle converts unquoted identifiers to UPPERCASE
            return identifier.toUpperCase();
        }
        return dbInfo.quoteString + identifier + dbInfo.quoteString;
    }

    private String getTextDataType(DatabaseInfo dbInfo) {
        if (dbInfo.isOracle()) {
            return "VARCHAR2(4000)"; // Or "CLOB" if your CSV cells exceed 4000 chars
        } else if (dbInfo.isSqlServer()) {
            return "NVARCHAR(MAX)";
        }
        return "TEXT"; // Standard for MySQL and PostgreSQL
    }

    // Helper holder class for DB properties
    private static class DatabaseInfo {
        final String productName;
        final String quoteString;

        DatabaseInfo(String productName, String quoteString) {
            this.productName = productName;
            this.quoteString = quoteString;
        }

        boolean isOracle() {
            return productName.contains("oracle");
        }

        boolean isMysql() {
            return productName.contains("mysql") || productName.contains("mariadb");
        }

        boolean isPostgres() {
            return productName.contains("postgresql");
        }

        boolean isSqlServer() {
            return productName.contains("microsoft") || productName.contains("sql server");
        }
    }
}