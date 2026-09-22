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
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;

import com.appzone.springbatch.DTO.CsvHeaderMetadata;
import com.appzone.springbatch.Resolvers.DynamicBatchSizeResolver;
import com.appzone.springbatch.processors.HeaderProcessor;
import com.appzone.springbatch.tasklets.TableInitializationTasklet;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Service
public class DynamicBatchService {

    @Autowired
    private HeaderProcessor headerProcessor;

    public void runDynamicImport(String driverClassName, String dbUrl, String username, String password, String csvFilePath, String tableName) throws Exception {

        CsvHeaderMetadata columnmetadata = headerProcessor.extractMetadata(csvFilePath);

        // 1. Create DataSource dynamically at runtime
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName(driverClassName);
        dataSource.setUrl(applyBatchOptimizationFlags(dbUrl));
        dataSource.setUsername(username);
        dataSource.setPassword(password);
        
       

        // 2. Transaction Manager & JdbcTemplates
        PlatformTransactionManager txManager = new DataSourceTransactionManager(dataSource);
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        NamedParameterJdbcTemplate namedJdbcTemplate = new NamedParameterJdbcTemplate(dataSource);

        // Auto-create Spring Batch Metadata Tables
        try {
            ResourceDatabasePopulator populator = new ResourceDatabasePopulator();
            if (dbUrl.contains("oracle")) {
                populator.addScript(new ClassPathResource("org/springframework/batch/core/schema-oracle.sql"));
            } else if (dbUrl.contains("postgresql")) {
                populator.addScript(new ClassPathResource("org/springframework/batch/core/schema-postgresql.sql"));
            } else {
                populator.addScript(new ClassPathResource("org/springframework/batch/core/schema-mysql.sql"));
            }
            populator.setContinueOnError(true);
            populator.execute(dataSource);
        } catch (Exception e) {
            // Tables might already exist, continue execution
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
        Job importJob = new JobBuilder("dynamicCsvJob", jobRepository)
                .start(createTableStep)
                .next(createChunkStep(jobRepository, txManager, namedJdbcTemplate, csvFilePath, columnmetadata, tableName))
                .build();

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

        jobLauncher.start(importJob, jobParameters);
    }

    private String applyBatchOptimizationFlags(String url) {
        if (url == null) return url;

        String lowerUrl = url.toLowerCase();

        if (lowerUrl.contains("mysql") && !lowerUrl.contains("rewritebatchedstatements")) {
            url += (url.contains("?") ? "&" : "?") + "rewriteBatchedStatements=true";
        } else if (lowerUrl.contains("postgresql") && !lowerUrl.contains("rewritebatchedinserts")) {
            url += (url.contains("?") ? "&" : "?") + "reWriteBatchedInserts=true";
        }

        return url;
    }
    
    
    private Step createChunkStep(JobRepository jobRepository, 
                                 PlatformTransactionManager txManager, 
                                 NamedParameterJdbcTemplate namedJdbc, 
                                 String csvFilePath,
                                 CsvHeaderMetadata columnmetadata, 
                                 String tableName) throws Exception {

        int chunkSize = DynamicBatchSizeResolver.resolveBatchSize(namedJdbc, csvFilePath, columnmetadata.getColumnCount());

        System.out.println("chunk size is : " + chunkSize);

        return new StepBuilder("chunkStep", jobRepository)
                .<Map<String, Object>, Map<String, Object>>chunk(chunkSize)
                .reader(createReader(csvFilePath))
                .writer(createWriter(namedJdbc, tableName))
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
            String tableName) {

        java.util.concurrent.atomic.AtomicLong totalTimeMs = new java.util.concurrent.atomic.AtomicLong(0);

        return chunk -> {

            if (chunk.isEmpty()) return;

            List<MapSqlParameterSource> batchArgs = new ArrayList<>();
            int colCount = chunk.getItems().get(0).size();

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

            List<String> placeholders = new ArrayList<>();
            for (int i = 0; i < colCount; i++) {
                placeholders.add(":param_" + i);
            }

            // Get dialect-aware quotes dynamically (e.g., `tableName` for MySQL, "tableName" for Oracle)
            String safeTableName = getQuotedTableName(namedJdbc, tableName);

            String sql = "INSERT INTO " + safeTableName + " VALUES ( "
                    + String.join(", ", placeholders)
                    + ")";

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

    /**
     * Resolves driver-specific table quoting dynamically using NamedParameterJdbcTemplate
     */
    private String getQuotedTableName(NamedParameterJdbcTemplate namedJdbc, String tableName) {
        return namedJdbc.getJdbcTemplate().execute((java.sql.Connection con) -> {
            java.sql.DatabaseMetaData metaData = con.getMetaData();
            String quoteString = metaData.getIdentifierQuoteString();

            if (quoteString == null || quoteString.trim().isEmpty()) {
                quoteString = "\"";
            }

            // Return uppercase table name for Oracle if no spaces exist (Oracle standard)
            String dbProductName = metaData.getDatabaseProductName().toLowerCase();
            if (dbProductName.contains("oracle") && !tableName.contains(" ")) {
                return tableName.toUpperCase();
            }

            return quoteString + tableName + quoteString;
        });
    }
}