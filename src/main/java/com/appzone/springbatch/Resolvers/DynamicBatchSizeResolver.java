package com.appzone.springbatch.Resolvers;

import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import java.io.File;
import java.util.Collections;


public class DynamicBatchSizeResolver {

    

    /**
     * Resolves batch size dynamically using MySQL max_allowed_packet.
     * Falls back to column-count based determination if it fails.
     */
    public static int resolveBatchSize(NamedParameterJdbcTemplate namedJdbc, String csvFilePath, int colCount) {
        try {
            // 1. Try querying MySQL max_allowed_packet using NamedParameterJdbcTemplate
            Long maxAllowedPacket = namedJdbc.queryForObject(
                    "SELECT @@max_allowed_packet",
                    Collections.emptyMap(),
                    Long.class
            );

            if (maxAllowedPacket != null && maxAllowedPacket > 0) {
                // Apply a safety margin (50% of total max_allowed_packet)
                long safePacketLimitBytes = (long) (maxAllowedPacket * 0.50);

                // Estimate average row size from the CSV file
                long avgRowSizeBytes = estimateAverageRowSize(csvFilePath);

                if (avgRowSizeBytes > 0) {
                    int calculatedBatchSize = (int) (safePacketLimitBytes / avgRowSizeBytes);

                    // Cap batch size between 500 and 10,000 for stability
                    int finalBatchSize = Math.max(500, Math.min(calculatedBatchSize, 10000));

                    System.out.printf("Resolved Batch Size using MySQL max_allowed_packet (%d MB): %d%n",
                            maxAllowedPacket / (1024 * 1024), finalBatchSize);
                    return finalBatchSize;
                }
            }
        } catch (Exception e) {
            System.out.println("Could not resolve max_allowed_packet from DB. Switching to Column-Count Fallback strategy.");
        }

        // 2. Fallback: Column-count strategy
        return resolveFallbackByColumnCount(colCount);
    }

    private static long estimateAverageRowSize(String filePath) {
        File file = new File(filePath);
        if (!file.exists() || file.length() == 0) {
            return 256; // Reasonable default estimate (256 bytes per row)
        }

        long sampleLength = Math.min(file.length(), 100_000); // Check up to first ~100KB
        return Math.max(128, sampleLength / 100);
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