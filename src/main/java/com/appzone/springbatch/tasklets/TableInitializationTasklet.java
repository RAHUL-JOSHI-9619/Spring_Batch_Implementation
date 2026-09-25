package com.appzone.springbatch.tasklets;

import org.springframework.batch.core.step.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.infrastructure.repeat.RepeatStatus;
import org.springframework.jdbc.core.JdbcTemplate;

import com.appzone.springbatch.Resolvers.TableSplitPlan;

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

        chunkContext.getStepContext()
                    .getStepExecution()
                    .getJobExecution()
                    .getExecutionContext()
                    .put("cleanHeaders", sanitizedHeaders);

        DatabaseInfo dbInfo = jdbcTemplate.execute((java.sql.Connection con) -> {
            DatabaseMetaData metaData = con.getMetaData();
            String productName = metaData.getDatabaseProductName().toLowerCase();
            String quote = metaData.getIdentifierQuoteString();

            if (quote == null || quote.trim().isEmpty()) {
                quote = "\"";
            }

            return new DatabaseInfo(productName, quote);
        });

        int columnCount = sanitizedHeaders.size();

        // 1. Drop existing table (or view, from a previous MySQL-style merge) if present
        //    (in split mode this also removes an old single table/view with the same name)
        dropViewIfExists(tableName, dbInfo);
        dropTableIfExists(tableName, dbInfo);

        if (!TableSplitPlan.needsSplit(columnCount)) {

            // 2. Normal case: ONE table, exactly like before
            String createTableSql = buildCreateTableSql(tableName, sanitizedHeaders, dbInfo, false);
            jdbcTemplate.execute(createTableSql);

        } else {

            // 2. Wide CSV: create several smaller tables
            int parts = TableSplitPlan.partCount(columnCount);

            System.out.println("[TableInitializationTasklet] The CSV has " + columnCount
                    + " columns, which is more than the limit of " + TableSplitPlan.MAX_COLUMNS_PER_TABLE
                    + " columns per table. So the table is being DIVIDED into " + parts + " part tables.");

            for (int p = 0; p < parts; p++) {
                int start = TableSplitPlan.startIndex(columnCount, p);
                int end = TableSplitPlan.endIndex(columnCount, p);

                String partTableName = TableSplitPlan.partTableName(tableName, p);
                List<String> partHeaders = sanitizedHeaders.subList(start, end);

                dropTableIfExists(partTableName, dbInfo);
                jdbcTemplate.execute(buildCreateTableSql(partTableName, partHeaders, dbInfo, true));

                System.out.println("[TableInitializationTasklet] Created table " + partTableName
                        + " with " + partHeaders.size() + " columns (CSV columns " + (start + 1) + " to " + end + ")");
            }

            System.out.println("[TableInitializationTasklet] Every part table has the key column '"
                    + TableSplitPlan.ROW_ID_COLUMN + "'. After the data is loaded, the parts will be joined "
                    + "into the single table '" + tableName + "' by the next step.");
        }

        return RepeatStatus.FINISHED;
    }

    // A previous run on a wide CSV may have created a VIEW (not a table) with this name
    // (see TableMergeTasklet). Try to remove it too, so this run starts clean.
    private void dropViewIfExists(String tableName, DatabaseInfo dbInfo) {
        String quotedTableName = quoteIdentifier(tableName, dbInfo);
        try {
            jdbcTemplate.execute("DROP VIEW IF EXISTS " + quotedTableName);
        } catch (Exception e) {
            // Either no view existed, or this database needs a plain DROP VIEW (e.g. Oracle)
            try {
                jdbcTemplate.execute("DROP VIEW " + quotedTableName);
            } catch (Exception ignore) {
                // No view existed - nothing to do
            }
        }
    }

    private void dropTableIfExists(String tableName, DatabaseInfo dbInfo) {
        String quotedTableName = quoteIdentifier(tableName, dbInfo);

        if (dbInfo.isSqlServer()) {
            // SQL Server 2016+ supports DROP TABLE IF EXISTS
            jdbcTemplate.execute("DROP TABLE IF EXISTS " + quotedTableName);
        } else if (dbInfo.isOracle()) {
            try {
                jdbcTemplate.execute("DROP TABLE " + quotedTableName);
            } catch (Exception e) {
                // Table did not exist
            }
        } else if (dbInfo.isPostgres() || dbInfo.isMysql()) {
            jdbcTemplate.execute("DROP TABLE IF EXISTS " + quotedTableName);
        } else {
            try {
                jdbcTemplate.execute("DROP TABLE " + quotedTableName);
            } catch (Exception e) {
                // Table did not exist
            }
        }
    }

    private String buildCreateTableSql(String tableName, List<String> headers, DatabaseInfo dbInfo, boolean withRowId) {
        StringBuilder sql = new StringBuilder();
        String quotedTableName = quoteIdentifier(tableName, dbInfo);
        String textDataType = getTextDataType(dbInfo);

        sql.append("CREATE TABLE ").append(quotedTableName).append(" (\n");

        // Only for split tables: a key column so the parts can be joined together
        if (withRowId) {
            sql.append("  ")
               .append(quoteIdentifier(TableSplitPlan.ROW_ID_COLUMN, dbInfo))
               .append(" ")
               .append(getRowIdDataType(dbInfo))
               .append(" PRIMARY KEY");
            if (!headers.isEmpty()) {
                sql.append(",\n");
            }
        }

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
        if (dbInfo.isSqlServer()) {
            return "[" + identifier + "]";
        }
        if (dbInfo.isOracle() && !identifier.contains(" ")) {
            return identifier.toUpperCase();
        }
        return dbInfo.quoteString + identifier + dbInfo.quoteString;
    }

    private String getTextDataType(DatabaseInfo dbInfo) {
        if (dbInfo.isSqlServer()) {
            return "NVARCHAR(MAX)";
        } else if (dbInfo.isOracle()) {
            return "VARCHAR2(4000)";
        }
        return "TEXT";
    }

    private String getRowIdDataType(DatabaseInfo dbInfo) {
        if (dbInfo.isOracle()) {
            return "NUMBER(19)";
        }
        return "BIGINT";
    }

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