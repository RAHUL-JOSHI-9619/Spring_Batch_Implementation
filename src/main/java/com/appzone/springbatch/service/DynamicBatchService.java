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

    public void runDynamicImport(String driverClassName,String dbUrl, String username, String password, String csvFilePath, String tableName) throws Exception {

        // 1. Create DataSource dynamically at runtime
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName(driverClassName);
        dataSource.setUrl(dbUrl);
        dataSource.setUsername(username);
        dataSource.setPassword(password);

        // 2. Transaction Manager & JdbcTemplates
        PlatformTransactionManager txManager = new DataSourceTransactionManager(dataSource);
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        NamedParameterJdbcTemplate namedJdbcTemplate = new NamedParameterJdbcTemplate(dataSource);
        
        
     // --- NEW FIX: Auto-create Spring Batch Metadata Tables if they don't exist ---
        try {
            ResourceDatabasePopulator populator = new ResourceDatabasePopulator();
            // For MySQL: use schema-mysql.sql. For PostgreSQL: use schema-postgresql.sql
            populator.addScript(new ClassPathResource("org/springframework/batch/core/schema-mysql.sql"));
            populator.setContinueOnError(true); // Ignores errors if tables already exist
            populator.execute(dataSource);
        } catch (Exception e) {
            // Tables might already exist, continue execution
        }

        // 3. Initialize JobRepository & JobLauncher (Spring Batch 5+ modern approach)
        JdbcJobRepositoryFactoryBean factory = new JdbcJobRepositoryFactoryBean();
        factory.setDataSource(dataSource);
        factory.setTransactionManager(txManager);
        factory.afterPropertiesSet();
        JobRepository jobRepository = factory.getObject();
        
        

        
        // 4. Create Tasklet Step (Table Creation)
        TableInitializationTasklet tasklet = new TableInitializationTasklet(tableName, jdbcTemplate, headerProcessor);
        Step createTableStep = new StepBuilder("createTableStep", jobRepository)
                .tasklet(tasklet, txManager)
                .build();

        // 5. Construct Job Flow
        Job importJob = new JobBuilder("dynamicCsvJob", jobRepository)
                .start(createTableStep)
                .next(createChunkStep(jobRepository, txManager, namedJdbcTemplate, csvFilePath, tableName))
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

    private Step createChunkStep(JobRepository jobRepository, 
                                 PlatformTransactionManager txManager, 
                                 NamedParameterJdbcTemplate namedJdbc, 
                                 String csvFilePath, 
                                 String tableName) throws Exception {

        // Note: Using updated chunk API syntax: .<In, Out>chunk(chunkSize, txManager)
        return new StepBuilder("chunkStep", jobRepository)
                .<Map<String, Object>, Map<String, Object>>chunk(getChunkSize(csvFilePath))
                .reader(createReader(csvFilePath))
                .writer(createWriter(namedJdbc, tableName))
                .build();
    }
    
    private int getChunkSize(String csvFilePath) throws Exception {
    	int columnCount=headerProcessor.processHeaders(csvFilePath).size();
    	if (columnCount <= 5) return 3000;
        if (columnCount <= 15) return 1500;
        if (columnCount <= 35) return 500;
        return 200;
    	
    }

    private FlatFileItemReader<Map<String, Object>> createReader(String csvFilePath) {
        return new FlatFileItemReaderBuilder<Map<String, Object>>()
                .name("dynamicCsvReader")
                .resource(new FileSystemResource(csvFilePath))
                .linesToSkip(1)
                .lineMapper((line, lineNumber) -> {
                    String[] values = line.split(",", -1);
                    Map<String, Object> record = new LinkedHashMap<>();

                    for (int i = 0; i < values.length; i++) {
                    	String val = values[i].trim();
                        // Convert empty strings to null for clean DB inserts
                        record.put("col_" + i, val.isEmpty() ? null : val);
                    }
                    return record;
                })
                .build();
    }

    private ItemWriter<Map<String, Object>> createWriter(NamedParameterJdbcTemplate namedJdbc, String tableName) {
        return chunk -> {
            if (chunk.isEmpty()) return;

            List<MapSqlParameterSource> batchArgs = new ArrayList<>();

            for (Map<String, Object> item : chunk.getItems()) {
                MapSqlParameterSource paramSource = new MapSqlParameterSource();
                int idx = 0;
                for (Object value : item.values()) {
                    paramSource.addValue("param_" + idx, value);
                    idx++;
                }
                batchArgs.add(paramSource);
            }

            int colCount = chunk.getItems().get(0).size();
            List<String> placeholders = new ArrayList<>();
            for (int i = 0; i < colCount; i++) {
                placeholders.add(":param_" + i);
            }

            String sql = "INSERT INTO `" + tableName + "` VALUES ( " + String.join(", ", placeholders) + ")";
            namedJdbc.batchUpdate(sql, batchArgs.toArray(new MapSqlParameterSource[0]));
        };
    }
}