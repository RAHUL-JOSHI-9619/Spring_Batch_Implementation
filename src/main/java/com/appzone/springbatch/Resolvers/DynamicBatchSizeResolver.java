package com.appzone.springbatch.Resolvers;

import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.DatabaseMetaData;
import java.util.Collections;

public class DynamicBatchSizeResolver {

    /**
     * Resolves batch size dynamically using DB dialect checks.
     * MySQL uses max_allowed_packet, Oracle/others use a safe high-throughput buffer limit.
     */
    public static int resolveBatchSize(NamedParameterJdbcTemplate namedJdbc, String csvFilePath, int colCount) {
        try {
            long maxPacketBytes = resolveMaxPacketSizeByDialect(namedJdbc);
            long avgRowSizeBytes = estimateAverageRowSize(csvFilePath);

            if (maxPacketBytes > 0 && avgRowSizeBytes > 0) {
                long safePacketLimitBytes = (long) (maxPacketBytes * 0.50);
                int calculatedBatchSize = (int) (safePacketLimitBytes / avgRowSizeBytes);

                int finalBatchSize = Math.max(500, Math.min(calculatedBatchSize, 10000));

                System.out.printf("Resolved Batch Size using DB Limit (%d MB): %d%n",
                        maxPacketBytes / (1024 * 1024), finalBatchSize);
                return finalBatchSize;
            }
        } catch (Exception e) {
            System.out.println("Could not resolve max_allowed_packet from DB. Switching to Column-Count Fallback strategy.");
        }

        return resolveFallbackByColumnCount(colCount);
    }

    private static long resolveMaxPacketSizeByDialect(NamedParameterJdbcTemplate namedJdbc) {
        return namedJdbc.getJdbcTemplate().execute((java.sql.Connection con) -> {
            DatabaseMetaData metaData = con.getMetaData();
            String dbProductName = metaData.getDatabaseProductName().toLowerCase();

            // Run MySQL-specific variable check ONLY when connected to MySQL/MariaDB
            if (dbProductName.contains("mysql") || dbProductName.contains("mariadb")) {
                try {
                    Long mysqlPacket = namedJdbc.queryForObject(
                            "SELECT @@max_allowed_packet",
                            Collections.emptyMap(),
                            Long.class
                    );
                    if (mysqlPacket != null && mysqlPacket > 0) {
                        return mysqlPacket;
                    }
                } catch (Exception e) {
                    // Fall back if query fails
                }
            }

            // Oracle, PostgreSQL, SQL Server default safe payload buffer (64 MB)
            return 64L * 1024 * 1024;
        });
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

    private static int resolveFallbackByColumnCount(int colCount) {
        if (colCount <= 10) {
            return 2000;
        } else if (colCount <= 30) {
            return 1000;
        } else if (colCount <= 50) {
            return 500;
        } else {
            return 250;
        }
    }
}