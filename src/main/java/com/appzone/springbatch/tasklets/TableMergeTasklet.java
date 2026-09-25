package com.appzone.springbatch.tasklets;

import org.springframework.batch.core.step.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.infrastructure.repeat.RepeatStatus;
import org.springframework.jdbc.core.JdbcTemplate;

import com.appzone.springbatch.Resolvers.TableSplitPlan;

import java.sql.DatabaseMetaData;

/**
 * Runs AFTER the data is loaded. Only used when the CSV was divided into part tables.
 *
 * It joins the part tables back into ONE table with the name the user gave:
 *
 *   CREATE TABLE long_table AS
 *   SELECT * FROM long_table_part1
 *   JOIN long_table_part2 USING (csv_row_id)
 *   JOIN long_table_part3 USING (csv_row_id)
 *
 * "USING" is used (not "ON") so the key column csv_row_id appears only ONCE in the new table.
 * This syntax works on Oracle, MySQL and PostgreSQL.
 *
 * If the database cannot create such a wide table, the data is NOT lost:
 * the part tables are kept and a clear message is printed.
 */
public class TableMergeTasklet implements Tasklet {

    private final String tableName;
    private final JdbcTemplate jdbcTemplate;
    private final int columnCount;

    public TableMergeTasklet(String tableName, JdbcTemplate jdbcTemplate, int columnCount) {
        this.tableName = tableName;
        this.jdbcTemplate = jdbcTemplate;
        this.columnCount = columnCount;
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {

        // Normal CSV (one table): nothing to join
        if (!TableSplitPlan.needsSplit(columnCount)) {
            return RepeatStatus.FINISHED;
        }

        DbInfo db = jdbcTemplate.execute((java.sql.Connection con) -> {
            DatabaseMetaData metaData = con.getMetaData();
            String quote = metaData.getIdentifierQuoteString();
            if (quote == null || quote.trim().isEmpty()) {
                quote = "\"";
            }
            return new DbInfo(metaData.getDatabaseProductName().toLowerCase(), quote);
        });

        int parts = TableSplitPlan.partCount(columnCount);

        if (!(db.isOracle() || db.isMysql() || db.isPostgres())) {
            log("This database type is not supported for joining the parts. "
                    + "The data is saved in the " + parts + " part tables ("
                    + TableSplitPlan.partTableName(tableName, 0) + " ...). "
                    + "Join them with the key column '" + TableSplitPlan.ROW_ID_COLUMN + "'.");
            return RepeatStatus.FINISHED;
        }

        String finalTable = quote(tableName, db);
        String rowId = quote(TableSplitPlan.ROW_ID_COLUMN, db);

        // ---- Build: CREATE TABLE <name> AS SELECT * FROM p1 JOIN p2 USING (key) JOIN p3 USING (key) ----
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE ").append(finalTable)
           .append(" AS SELECT * FROM ").append(quote(TableSplitPlan.partTableName(tableName, 0), db));
        for (int p = 1; p < parts; p++) {
            sql.append(" JOIN ").append(quote(TableSplitPlan.partTableName(tableName, p), db))
               .append(" USING (").append(rowId).append(")");
        }

        log("All rows are loaded into the " + parts + " part tables.");
        log("Now joining the parts into the single table '" + tableName + "' (" + columnCount + " columns)...");
        log("SQL: " + sql);

        long start = System.currentTimeMillis();

        try {
            jdbcTemplate.execute(sql.toString());
            checkRowCountAndFinish(db, finalTable, tableName, parts, columnCount, start, true);
            return RepeatStatus.FINISHED;

        } catch (Exception tableEx) {
            String tableErrorMessage = rootMessage(tableEx);
            log("COULD NOT create '" + tableName + "' as a real table with " + columnCount + " columns.");
            log("Database message: " + tableErrorMessage);

            // Clean up any half-made table before trying the fallback
            try {
                jdbcTemplate.execute("DROP TABLE " + finalTable);
            } catch (Exception ignore) {
                // nothing to remove
            }

            // ---- Fallback: some databases (typically MySQL/InnoDB) cannot physically store
            // this many columns in one row, no matter how the data was loaded. A VIEW has no
            // such row-size limit, because it stores no data of its own - it just re-runs the
            // join every time it is queried. The part tables must stay, since the view reads
            // from them directly. ----
            log("Trying a VIEW instead, since a VIEW has no row-size limit (it stores no data itself)...");

            // Oracle does not support "DROP VIEW IF EXISTS" (it has no IF EXISTS clause at all),
            // so that statement throws a syntax error there. If we only caught-and-ignored it,
            // an old view with this name would silently survive on Oracle and the CREATE VIEW
            // below would then fail with "name already used by an existing object". So on Oracle
            // (or any database that rejects IF EXISTS) we retry with a plain DROP VIEW.
            try {
                jdbcTemplate.execute("DROP VIEW IF EXISTS " + finalTable);
            } catch (Exception ignoreIfExistsNotSupported) {
                try {
                    jdbcTemplate.execute("DROP VIEW " + finalTable);
                } catch (Exception ignoreNoViewExisted) {
                    // No view existed under this name - nothing to drop, which is the normal case
                }
            }

            String viewSql = "CREATE VIEW " + finalTable + " " + sql.toString().substring(sql.indexOf("AS SELECT"));

            try {
                jdbcTemplate.execute(viewSql);
                checkRowCountAndFinish(db, finalTable, tableName, parts, columnCount, start, false);

            } catch (Exception viewEx) {
                // Neither a table nor a view could be made - the data is still safe in the parts
                log("COULD NOT create '" + tableName + "' as a view either.");
                log("Database message: " + rootMessage(viewEx));
                log("Your data is safe in the part tables: "
                        + TableSplitPlan.partTableName(tableName, 0) + " ... "
                        + TableSplitPlan.partTableName(tableName, parts - 1)
                        + ". Join them yourself with the key column '" + TableSplitPlan.ROW_ID_COLUMN + "'.");
            }
        }

        return RepeatStatus.FINISHED;
    }

    // Checks the row count matches, then either drops the part tables (real table case)
    // or keeps them (view case, since the view needs them to answer queries).
    private void checkRowCountAndFinish(DbInfo db, String finalTable, String tableName, int parts,
                                        int columnCount, long start, boolean isRealTable) {

        Long partRows = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM " + quote(TableSplitPlan.partTableName(tableName, 0), db), Long.class);
        Long mergedRows = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM " + finalTable, Long.class);

        String kind = isRealTable ? "table" : "view";
        log("'" + tableName + "' (" + kind + ") is ready in "
                + String.format("%.2f", (System.currentTimeMillis() - start) / 1000.0)
                + " sec with " + mergedRows + " rows.");

        boolean rowsMatch = partRows != null && partRows.equals(mergedRows);

        if (!rowsMatch) {
            log("WARNING: '" + tableName + "' shows " + mergedRows + " rows but the first part has "
                    + partRows + " rows. The part tables are KEPT so you can check the data.");
            return;
        }

        if (!isRealTable) {
            log("'" + tableName + "' is a VIEW, not a real table (MySQL cannot fit " + columnCount
                    + " TEXT columns in one physical row). You can SELECT from '" + tableName
                    + "' normally. The " + parts + " part tables are KEPT, because the view reads from them.");
            return;
        }

        if (TableSplitPlan.DROP_PART_TABLES_AFTER_MERGE) {
            for (int p = 0; p < parts; p++) {
                String part = TableSplitPlan.partTableName(tableName, p);
                try {
                    jdbcTemplate.execute("DROP TABLE " + quote(part, db));
                } catch (Exception e) {
                    log("Could not delete the part table " + part + ": " + rootMessage(e));
                }
            }
            log("The " + parts + " temporary part tables were deleted. "
                    + "'" + tableName + "' now has all " + columnCount + " CSV columns plus the key column '"
                    + TableSplitPlan.ROW_ID_COLUMN + "'.");
        } else {
            log("The part tables are kept (DROP_PART_TABLES_AFTER_MERGE is false).");
        }
    }

    // Same quoting rules as TableInitializationTasklet, so the names always match
    private String quote(String identifier, DbInfo db) {
        if (db.isOracle() && !identifier.contains(" ")) {
            return identifier.toUpperCase();
        }
        return db.quoteString + identifier + db.quoteString;
    }

    // The deepest (real) error message
    private String rootMessage(Throwable t) {
        Throwable root = t;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        return root.getMessage();
    }

    private void log(String message) {
        System.out.println("[TableMergeTasklet] " + message);
    }

    private static class DbInfo {
        final String productName;
        final String quoteString;

        DbInfo(String productName, String quoteString) {
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
    }
}