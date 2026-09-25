package com.appzone.springbatch.service;

import org.springframework.batch.core.configuration.support.MapJobRegistry;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.TaskExecutorJobOperator;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.JdbcJobRepositoryFactoryBean;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.infrastructure.item.ItemWriter;
import org.springframework.batch.infrastructure.item.file.FlatFileItemReader;
import org.springframework.batch.infrastructure.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.infrastructure.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.infrastructure.item.file.separator.DefaultRecordSeparatorPolicy;
import org.springframework.batch.infrastructure.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;

import com.appzone.springbatch.DTO.CsvHeaderMetadata;
import com.appzone.springbatch.Resolvers.DynamicBatchSizeResolver;
import com.appzone.springbatch.Resolvers.TableSplitPlan;
import com.appzone.springbatch.processors.HeaderProcessor;
import com.appzone.springbatch.tasklets.TableInitializationTasklet;
import com.appzone.springbatch.tasklets.TableMergeTasklet;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

@Service
public class DynamicBatchService {

    // Used only when we cannot find out the database type/version.
    // This is the smallest (safest) value, so it works on every database.
    private static final int DEFAULT_MAX_COLUMN_NAME_LENGTH = 25;

    @Autowired
    private HeaderProcessor headerProcessor;

    public void runDynamicImport(String driverClassName, String dbUrl, String username, String password, String csvFilePath, String tableName) throws Exception {

        // 1. Create a pooled DataSource dynamically at runtime.
        //    DriverManagerDataSource used to open a brand-new physical connection
        //    (TCP handshake + auth, plus TLS negotiation if enabled) every time anything
        //    called getConnection() - at least once per chunk transaction. That per-chunk
        //    cost does not shrink or grow with how many rows are in the chunk, which is
        //    exactly the kind of overhead that stays flat no matter what batch size you pick.
        //    A small pool reuses already-established, already-authenticated connections instead.
        HikariDataSource dataSource = new HikariDataSource();
        dataSource.setDriverClassName(driverClassName);
        String effectiveUrl = applyBatchOptimizationFlags(dbUrl);
        dataSource.setJdbcUrl(effectiveUrl);
        dataSource.setUsername(username);
        dataSource.setPassword(password);
        dataSource.setMaximumPoolSize(5);
        dataSource.setMinimumIdle(1);
        dataSource.setPoolName("dynamic-batch-pool");

        System.out.println("[DynamicBatchService] JDBC URL in use (batch-tuning flags applied): " + effectiveUrl);

        // 2. Transaction Manager & JdbcTemplates
        PlatformTransactionManager txManager = new DataSourceTransactionManager(dataSource);
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        NamedParameterJdbcTemplate namedJdbcTemplate = new NamedParameterJdbcTemplate(dataSource);

        // Find the maximum column name length for this database and version,
        // then clean/shorten the CSV column names using that length.
        // (This now runs AFTER the DataSource is created, because we need to ask the database.)
        int maxColumnNameLength = resolveMaxColumnNameLength(dataSource, jdbcTemplate);
        CsvHeaderMetadata columnmetadata = headerProcessor.extractMetadata(csvFilePath, maxColumnNameLength);

        // Auto-create Spring Batch Metadata Tables
        try {
            ResourceDatabasePopulator populator = new ResourceDatabasePopulator();
            String lowerUrl = dbUrl.toLowerCase();

            if (lowerUrl.contains("sqlserver") || lowerUrl.contains("microsoft")) {
                populator.addScript(new ClassPathResource("org/springframework/batch/core/schema-sqlserver.sql"));
            } else if (lowerUrl.contains("oracle")) {
                populator.addScript(new ClassPathResource("org/springframework/batch/core/schema-oracle.sql"));
            } else if (lowerUrl.contains("postgresql")) {
                populator.addScript(new ClassPathResource("org/springframework/batch/core/schema-postgresql.sql"));
            } else {
                populator.addScript(new ClassPathResource("org/springframework/batch/core/schema-mysql.sql"));
            }
            populator.setContinueOnError(true);
            populator.execute(dataSource);
        } catch (Exception e) {
            // Batch schema tables might already exist
        }

        // 3. Initialize JobRepository & JobLauncher
        JdbcJobRepositoryFactoryBean factory = new JdbcJobRepositoryFactoryBean();
        factory.setDataSource(dataSource);
        factory.setTransactionManager(txManager);
        factory.afterPropertiesSet();
        JobRepository jobRepository = factory.getObject();

        // 4. Create Tasklet Step (Table Creation)
        TableInitializationTasklet tasklet = new TableInitializationTasklet(tableName, jdbcTemplate, columnmetadata.getSanitizedHeaders());
        Step createTableStep = new StepBuilder("createTableStep", jobRepository)
                .tasklet(tasklet, txManager)
                .build();

        // 5. Construct Job Flow
        var jobBuilder = new JobBuilder("dynamicCsvJob", jobRepository)
                .start(createTableStep)
                .next(createChunkStep(jobRepository, txManager, namedJdbcTemplate, csvFilePath, columnmetadata, tableName));

        // Very wide CSV: after the data is loaded into the part tables,
        // join the parts back into ONE table with the name the user gave.
        int totalColumns = columnmetadata.getSanitizedHeaders().size();
        if (TableSplitPlan.needsSplit(totalColumns)) {
            Step mergeTablesStep = new StepBuilder("mergeTablesStep", jobRepository)
                    .tasklet(new TableMergeTasklet(tableName, jdbcTemplate, totalColumns), txManager)
                    .build();
            jobBuilder = jobBuilder.next(mergeTablesStep);
        }

        Job importJob = jobBuilder.build();

        // 6. Set up JobRegistry and Register the Dynamic Job
        MapJobRegistry jobRegistry = new MapJobRegistry();
        jobRegistry.register(importJob);

        TaskExecutorJobOperator jobLauncher = new TaskExecutorJobOperator();
        jobLauncher.setJobRepository(jobRepository);
        jobLauncher.setJobRegistry(jobRegistry);
        jobLauncher.afterPropertiesSet();

        // 7. Launch the Job
        JobParameters jobParameters = new JobParametersBuilder()
                .addString("csvFilePath", csvFilePath)
                .addLong("time", System.currentTimeMillis())
                .toJobParameters();

        // Each dynamic run creates its own pool for its own credentials, so it must be closed
        // once the job is truly finished. This must NOT be done from a JobExecutionListener:
        // afterJob() fires before Spring Batch has finished persisting the final JobExecution
        // status to the job repository, which itself still needs a live connection - closing
        // the pool there causes that last bookkeeping write to fail and the whole job gets
        // reported as failed even though every step (including your data) already succeeded.
        // start() here blocks until the job is completely done (jobLauncher uses a
        // SyncTaskExecutor by default), so closing right after it returns is safe.
        try {
            jobLauncher.start(importJob, jobParameters);
        } finally {
            dataSource.close();
        }
    }

    // ------------------------------------------------------------------
    // Find the maximum column name length, based on the database and its version
    //
    //   Oracle 7 - 12.1                        -> 30  (bytes)
    //   Oracle 12.2+ (and COMPATIBLE >= 12.2)  -> 128 (bytes)
    //   MySQL (all mainstream versions)        -> 64  (characters)
    //   PostgreSQL 7.0 - 7.4                   -> 31  (characters)
    //   PostgreSQL 8.0+                        -> 63  (bytes)
    //
    // Extra (not in your table): MariaDB -> 64, SQL Server -> 128.
    // Our column names only contain a-z, 0-9 and "_", so 1 character = 1 byte.
    // ------------------------------------------------------------------
    private int resolveMaxColumnNameLength(DataSource dataSource, JdbcTemplate jdbcTemplate) {
        try (Connection con = dataSource.getConnection()) {
            DatabaseMetaData meta = con.getMetaData();

            String productName = meta.getDatabaseProductName();
            String lowerProduct = productName.toLowerCase();
            int major = meta.getDatabaseMajorVersion();
            int minor = meta.getDatabaseMinorVersion();

            System.out.println("[DynamicBatchService] Database detected: " + productName
                    + " (version " + major + "." + minor + ")");

            int maxLen;

            if (lowerProduct.contains("oracle")) {
                maxLen = resolveOracleLength(major, minor, jdbcTemplate);

            } else if (lowerProduct.contains("mysql") || lowerProduct.contains("mariadb")) {
                maxLen = 64;

            } else if (lowerProduct.contains("postgresql")) {
                // PostgreSQL 8.0 and above -> 63, older versions (7.x) -> 31
                maxLen = (major >= 8) ? 63 : 31;

            } else if (lowerProduct.contains("microsoft") || lowerProduct.contains("sql server")) {
                maxLen = 128;

            } else {
                System.out.println("[DynamicBatchService] Database type is not in the known list, using the safe default length.");
                maxLen = DEFAULT_MAX_COLUMN_NAME_LENGTH;
            }

            System.out.println("[DynamicBatchService] Maximum column name length selected: " + maxLen);
            return maxLen;

        } catch (Exception e) {
            System.err.println("[DynamicBatchService] Could not detect the database version ("
                    + e.getMessage() + "). Using the safe default length: " + DEFAULT_MAX_COLUMN_NAME_LENGTH);
            return DEFAULT_MAX_COLUMN_NAME_LENGTH;
        }
    }

    private int resolveOracleLength(int major, int minor, JdbcTemplate jdbcTemplate) {
        boolean versionAllows128 = major > 12 || (major == 12 && minor >= 2);

        // Oracle 12.1 and older -> 30 bytes
        if (!versionAllows128) {
            return 30;
        }

        // Oracle 12.2 or newer: 128 bytes is allowed ONLY when the COMPATIBLE setting is 12.2 or higher.
        // We read that setting from the database. (Reading it needs permission on V$PARAMETER.)
        try {
            String compatible = jdbcTemplate.queryForObject(
                    "SELECT value FROM v$parameter WHERE name = 'compatible'", String.class);

            System.out.println("[DynamicBatchService] Oracle COMPATIBLE setting: " + compatible);

            String[] parts = compatible.trim().split("\\.");
            int compatMajor = Integer.parseInt(parts[0]);
            int compatMinor = parts.length > 1 ? Integer.parseInt(parts[1]) : 0;

            boolean compatAllows128 = compatMajor > 12 || (compatMajor == 12 && compatMinor >= 2);
            return compatAllows128 ? 128 : 30;

        } catch (Exception e) {
            // Could not read COMPATIBLE (for example: no permission) -> choose the safe value
            System.err.println("[DynamicBatchService] Could not read the Oracle COMPATIBLE setting ("
                    + e.getMessage() + "). Using the safe length 30.");
            return 30;
        }
    }

    private String applyBatchOptimizationFlags(String url) {
        if (url == null) return url;

        String lowerUrl = url.toLowerCase();

        if (lowerUrl.contains("mysql")) {
            // rewriteBatchedStatements: turns many INSERTs into one multi-row INSERT (big speed up)
            if (!lowerUrl.contains("rewritebatchedstatements")) {
                url += (url.contains("?") ? "&" : "?") + "rewriteBatchedStatements=true";
            }
            // useServerPrepStmts=false: rewriteBatchedStatements works reliably with CLIENT-side
            // prepared statements. Leaving server-side prepared statements on can silently stop
            // the rewrite from happening, which makes batches slow without any error.
            if (!lowerUrl.contains("useserverprepstmts")) {
                url += "&useServerPrepStmts=false";
            }
            // cachePrepStmts + prepStmtCacheSize: reuse prepared statement metadata across
            // batches instead of re-parsing it every time.
            if (!lowerUrl.contains("cacheprepstmts")) {
                url += "&cachePrepStmts=true&prepStmtCacheSize=250&prepStmtCacheSqlLimit=2048";
            }
        } else if (lowerUrl.contains("postgresql") && !lowerUrl.contains("rewritebatchedinserts")) {
            url += (url.contains("?") ? "&" : "?") + "reWriteBatchedInserts=true";
        } else if ((lowerUrl.contains("sqlserver") || lowerUrl.contains("microsoft")) && !lowerUrl.contains("sendstringparametersasunicode")) {
            url += (url.contains(";") ? ";" : ";") + "sendStringParametersAsUnicode=true;";
        }

        return url;
    }

    private Step createChunkStep(JobRepository jobRepository, 
                                 PlatformTransactionManager txManager, 
                                 NamedParameterJdbcTemplate namedJdbc, 
                                 String csvFilePath,
                                 CsvHeaderMetadata columnmetadata, 
                                 String tableName) throws Exception {

        // IMPORTANT: use the column count of ONE actual INSERT statement, not the whole CSV width.
        // For a normal (non-split) table these are the same. For a wide CSV that was divided
        // into part tables, only the columns of ONE part table (+ the row-id key column) are
        // ever sent in a single INSERT, so the batch size must be based on that, not the full
        // 396-column CSV width. Otherwise the batch ends up far larger than it should be.
        int totalCsvColumns = columnmetadata.getColumnCount();
        int columnsPerInsertStatement = TableSplitPlan.needsSplit(totalCsvColumns)
                ? TableSplitPlan.partSize(totalCsvColumns) + 1   // +1 for the csv_row_id key column
                : totalCsvColumns;

        int chunkSize = DynamicBatchSizeResolver.resolveBatchSize(namedJdbc, csvFilePath, columnsPerInsertStatement);

        System.out.println("chunk size is : " + chunkSize + " (based on " + columnsPerInsertStatement
                + " columns per INSERT statement)");

        // Normal CSV -> one table (original writer).
        // Very wide CSV -> several smaller tables (split writer).
        int columnCount = columnmetadata.getSanitizedHeaders().size();
        ItemWriter<Map<String, Object>> writer = TableSplitPlan.needsSplit(columnCount)
                ? createSplitWriter(namedJdbc, tableName, columnCount)
                : createWriter(namedJdbc, tableName, columnCount);

        return new StepBuilder("chunkStep", jobRepository)
                .<Map<String, Object>, Map<String, Object>>chunk(chunkSize)
                .reader(createReader(csvFilePath))
                .writer(writer)
                .build();
    }

    private FlatFileItemReader<Map<String, Object>> createReader(String csvFilePath) {

        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
        tokenizer.setStrict(false);

        DefaultLineMapper<Map<String, Object>> lineMapper = new DefaultLineMapper<>();
        lineMapper.setLineTokenizer(tokenizer);
        lineMapper.setFieldSetMapper(fieldSet -> {
            Map<String, Object> record = new LinkedHashMap<>();
            String[] values = fieldSet.getValues();

            for (int i = 0; i < values.length; i++) {
                String val = values[i] != null ? values[i].trim() : "";
                record.put("col_" + i, val.isEmpty() ? null : val);
            }
            return record;
        });

        return new FlatFileItemReaderBuilder<Map<String, Object>>()
                .name("dynamicCsvReader")
                .resource(new FileSystemResource(csvFilePath))
                .linesToSkip(1)
                .recordSeparatorPolicy(new DefaultRecordSeparatorPolicy())
                .lineMapper(lineMapper)
                .build();
    }

    private ItemWriter<Map<String, Object>> createWriter(
            NamedParameterJdbcTemplate namedJdbc,
            String tableName,
            int columnCount) {

        java.util.concurrent.atomic.AtomicLong totalTimeMs = new java.util.concurrent.atomic.AtomicLong(0);

        // Built ONCE per job, not once per chunk: getQuotedTableName() opens a connection
        // and calls DatabaseMetaData, so doing this inside the writer lambda meant every
        // single chunk paid that round trip again, no matter how big or small the chunk was.
        String safeTableName = getQuotedTableName(namedJdbc, tableName);

        List<String> placeholders = new ArrayList<>();
        for (int i = 0; i < columnCount; i++) {
            placeholders.add(":param_" + i);
        }

        String sql = "INSERT INTO " + safeTableName + " VALUES ( "
                + String.join(", ", placeholders)
                + ")";

        return chunk -> {

            if (chunk.isEmpty()) return;

            List<MapSqlParameterSource> batchArgs = new ArrayList<>();

            for (int rowIndex = 0; rowIndex < chunk.getItems().size(); rowIndex++) {

                Map<String, Object> item = chunk.getItems().get(rowIndex);
                MapSqlParameterSource paramSource = new MapSqlParameterSource();

                int idx = 0;
                for (Object value : item.values()) {
                    paramSource.addValue("param_" + idx, value);
                    idx++;
                }

                batchArgs.add(paramSource);
            }

            long batchStart = System.currentTimeMillis();

            namedJdbc.batchUpdate(
                    sql,
                    batchArgs.toArray(new MapSqlParameterSource[0])
            );

            long batchEnd = System.currentTimeMillis();

            long batchDurationMs = batchEnd - batchStart;
            long cumulativeDurationMs = totalTimeMs.addAndGet(batchDurationMs);

            double batchSec = batchDurationMs / 1000.0;
            double totalSec = cumulativeDurationMs / 1000.0;

            System.out.printf(
                    "Inserted batch of %d rows in %.3f sec | Total cumulative time: %.3f sec%n",
                    batchArgs.size(), batchSec, totalSec
            );
        };
    }

    // ------------------------------------------------------------------
    // Writer for very wide CSV files: each row is cut into several pieces and
    // every piece is inserted into its own table (<name>_part1, <name>_part2, ...).
    // All pieces of one row get the same csv_row_id, so they can be joined later.
    // All pieces of a chunk are saved in the same transaction.
    // ------------------------------------------------------------------
    private ItemWriter<Map<String, Object>> createSplitWriter(
            NamedParameterJdbcTemplate namedJdbc,
            String tableName,
            int columnCount) {

        final int parts = TableSplitPlan.partCount(columnCount);

        java.util.concurrent.atomic.AtomicLong totalTimeMs = new java.util.concurrent.atomic.AtomicLong(0);
        java.util.concurrent.atomic.AtomicLong rowCounter = new java.util.concurrent.atomic.AtomicLong(0);

        // Build the INSERT statement of every part table once
        List<String> insertSqls = new ArrayList<>();
        for (int p = 0; p < parts; p++) {
            int start = TableSplitPlan.startIndex(columnCount, p);
            int end = TableSplitPlan.endIndex(columnCount, p);

            List<String> placeholders = new ArrayList<>();
            placeholders.add(":row_id");
            for (int i = 0; i < end - start; i++) {
                placeholders.add(":param_" + i);
            }

            String safePartTableName = getQuotedTableName(namedJdbc, TableSplitPlan.partTableName(tableName, p));
            insertSqls.add("INSERT INTO " + safePartTableName + " VALUES ( "
                    + String.join(", ", placeholders) + ")");
        }

        System.out.println("[DynamicBatchService] Wide CSV: " + columnCount + " columns will be written into "
                + parts + " tables.");

        return chunk -> {

            if (chunk.isEmpty()) return;

            List<? extends Map<String, Object>> items = chunk.getItems();

            // Give every row a number. The same number is used in every part table.
            long[] rowIds = new long[items.size()];
            for (int r = 0; r < items.size(); r++) {
                rowIds[r] = rowCounter.incrementAndGet();
            }

            long chunkStart = System.currentTimeMillis();

            for (int p = 0; p < parts; p++) {
                int start = TableSplitPlan.startIndex(columnCount, p);
                int end = TableSplitPlan.endIndex(columnCount, p);

                List<MapSqlParameterSource> batchArgs = new ArrayList<>();

                for (int r = 0; r < items.size(); r++) {
                    Map<String, Object> item = items.get(r);
                    MapSqlParameterSource paramSource = new MapSqlParameterSource();
                    paramSource.addValue("row_id", rowIds[r]);

                    for (int c = start; c < end; c++) {
                        // A missing value (short row) is saved as NULL
                        paramSource.addValue("param_" + (c - start), item.get("col_" + c));
                    }

                    batchArgs.add(paramSource);
                }

                namedJdbc.batchUpdate(
                        insertSqls.get(p),
                        batchArgs.toArray(new MapSqlParameterSource[0])
                );
            }

            long batchDurationMs = System.currentTimeMillis() - chunkStart;
            long cumulativeDurationMs = totalTimeMs.addAndGet(batchDurationMs);

            System.out.printf(
                    "Inserted batch of %d rows into %d tables in %.3f sec | Total cumulative time: %.3f sec%n",
                    items.size(), parts, batchDurationMs / 1000.0, cumulativeDurationMs / 1000.0
            );
        };
    }

    private String getQuotedTableName(NamedParameterJdbcTemplate namedJdbc, String tableName) {
        return namedJdbc.getJdbcTemplate().execute((java.sql.Connection con) -> {
            java.sql.DatabaseMetaData metaData = con.getMetaData();
            String dbProductName = metaData.getDatabaseProductName().toLowerCase();

            if (dbProductName.contains("microsoft") || dbProductName.contains("sql server")) {
                return "[" + tableName + "]";
            }

            String quoteString = metaData.getIdentifierQuoteString();
            if (quoteString == null || quoteString.trim().isEmpty()) {
                quoteString = "\"";
            }

            if (dbProductName.contains("oracle") && !tableName.contains(" ")) {
                return tableName.toUpperCase();
            }

            return quoteString + tableName + quoteString;
        });
    }
}