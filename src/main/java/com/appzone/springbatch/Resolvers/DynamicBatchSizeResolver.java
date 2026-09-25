package com.appzone.springbatch.Resolvers;

import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.DatabaseMetaData;
import java.util.Collections;

public class DynamicBatchSizeResolver {

    // Maximum total VALUES (rows x columns actually inserted in ONE statement) per batch.
    // This is the key fix: a batch that is fine for a narrow table (few columns) can be far
    // too large for a wide table, because the SQL text sent to the database (with
    // rewriteBatchedStatements=true) grows with rows x columns, not rows alone.
    // 300,000 is a safe starting point; lower it (e.g. 150,000) if batches are still slow.
    private static final long TARGET_CELLS_PER_BATCH = 300_000;

    // Absolute floor and ceiling on the number of rows per batch, regardless of width
    private static final int MIN_BATCH_ROWS = 200;
    private static final int MAX_BATCH_ROWS = 10_000;

    /**
     * Resolves batch size dynamically using DB dialect checks.
     * MySQL uses max_allowed_packet, Oracle/others use a safe high-throughput buffer limit.
     *
     * @param columnsPerInsertStatement how many values ONE row actually carries in the INSERT
     *                                  that will really be executed. For a normal (non-split)
     *                                  table this is the CSV column count. For a wide CSV that
     *                                  was divided into part tables, this must be the column
     *                                  count of ONE part table (not the whole CSV), because
     *                                  that is what the writer actually sends to the database.
     */
    public static int resolveBatchSize(NamedParameterJdbcTemplate namedJdbc, String csvFilePath,
                                       int columnsPerInsertStatement) {
        try {
            DialectInfo dialect = resolveDialectInfo(namedJdbc);
            long avgRowSizeBytes = estimateAverageRowSize(csvFilePath);

            if (dialect.maxPacketBytes > 0 && avgRowSizeBytes > 0) {
                long safePacketLimitBytes = (long) (dialect.maxPacketBytes * 0.50);
                int packetBasedBatchSize = (int) (safePacketLimitBytes / avgRowSizeBytes);

                int finalBatchSize;

                if (dialect.rewritesBatchAsOneStatement) {
                    // Only true for MySQL/MariaDB with rewriteBatchedStatements=true: the driver
                    // concatenates the ENTIRE batch into one multi-value INSERT string, so that
                    // string's size grows with rows x columns and can blow past max_allowed_packet.
                    // Shrinking the row count as columns grow keeps that string safe.
                    int cellCountBasedCap = (int) Math.max(MIN_BATCH_ROWS,
                            Math.min(MAX_BATCH_ROWS, TARGET_CELLS_PER_BATCH / Math.max(1, columnsPerInsertStatement)));

                    finalBatchSize = Math.max(MIN_BATCH_ROWS,
                            Math.min(packetBasedBatchSize, cellCountBasedCap));

                    System.out.printf(
                            "Resolved Batch Size: %d rows (packet limit allowed up to %d rows, "
                                    + "%d columns/row cap allowed up to %d rows, DB packet size %d MB)%n",
                            finalBatchSize, packetBasedBatchSize, columnsPerInsertStatement,
                            cellCountBasedCap, dialect.maxPacketBytes / (1024 * 1024));
                } else {
                    // Oracle (OCI array binding) and SQL Server / PostgreSQL (RPC / protocol-level
                    // batching) send each batched row as its own bound parameter set, not as text
                    // appended into one SQL string. Column count does not multiply the wire size the
                    // same way, so shrinking the batch here only adds round trips without protecting
                    // anything - and extra round trips are exactly what slow these drivers down.
                    finalBatchSize = Math.max(MIN_BATCH_ROWS, Math.min(packetBasedBatchSize, MAX_BATCH_ROWS));

                    System.out.printf(
                            "Resolved Batch Size: %d rows (packet limit allowed up to %d rows, "
                                    + "no column-count shrink applied for this dialect, DB packet size %d MB)%n",
                            finalBatchSize, packetBasedBatchSize, dialect.maxPacketBytes / (1024 * 1024));
                }

                return finalBatchSize;
            }
        } catch (Exception e) {
            System.out.println("Could not resolve max_allowed_packet from DB. Switching to Column-Count Fallback strategy.");
        }

        return resolveFallbackByColumnCount(columnsPerInsertStatement);
    }

    private static DialectInfo resolveDialectInfo(NamedParameterJdbcTemplate namedJdbc) {
        return namedJdbc.getJdbcTemplate().execute((java.sql.Connection con) -> {
            DatabaseMetaData metaData = con.getMetaData();
            String dbProductName = metaData.getDatabaseProductName().toLowerCase();
            boolean isMySqlFamily = dbProductName.contains("mysql") || dbProductName.contains("mariadb");

            long maxPacketBytes;

            if (isMySqlFamily) {
                long mysqlPacket = 0;
                try {
                    Long result = namedJdbc.queryForObject(
                            "SELECT @@max_allowed_packet",
                            Collections.emptyMap(),
                            Long.class
                    );
                    if (result != null && result > 0) {
                        mysqlPacket = result;
                    }
                } catch (Exception e) {
                    // Fall back below if query fails
                }
                maxPacketBytes = mysqlPacket > 0 ? mysqlPacket : 64L * 1024 * 1024;
            } else {
                // Oracle, PostgreSQL, SQL Server default safe payload buffer (64 MB)
                maxPacketBytes = 64L * 1024 * 1024;
            }

            // isMySqlFamily doubles as "does this driver rewrite the batch into one SQL string?"
            // because that behaviour only exists behind MySQL's rewriteBatchedStatements=true.
            return new DialectInfo(maxPacketBytes, isMySqlFamily);
        });
    }

    private static final class DialectInfo {
        final long maxPacketBytes;
        final boolean rewritesBatchAsOneStatement;

        DialectInfo(long maxPacketBytes, boolean rewritesBatchAsOneStatement) {
            this.maxPacketBytes = maxPacketBytes;
            this.rewritesBatchAsOneStatement = rewritesBatchAsOneStatement;
        }
    }

    private static long estimateAverageRowSize(String filePath) {
        File file = new File(filePath);
        if (!file.exists() || file.length() == 0) {
            return 256;
        }

        int sampleLinesCount = 50;
        long totalBytes = 0;
        int linesRead = 0;

        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = reader.readLine()) != null && linesRead < sampleLinesCount) {
                totalBytes += line.getBytes().length;
                linesRead++;
            }
        } catch (IOException e) {
            return 256;
        }

        if (linesRead == 0) {
            return 256;
        }

        return Math.max(128, totalBytes / linesRead);
    }

    private static int resolveFallbackByColumnCount(int columnsPerInsertStatement) {
        if (columnsPerInsertStatement <= 10) {
            return 2000;
        } else if (columnsPerInsertStatement <= 30) {
            return 1000;
        } else if (columnsPerInsertStatement <= 50) {
            return 500;
        } else {
            return 250;
        }
    }
}