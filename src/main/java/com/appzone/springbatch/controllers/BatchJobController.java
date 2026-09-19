package com.appzone.springbatch.controllers;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.appzone.springbatch.DTO.BatchRequest;
import com.appzone.springbatch.service.DynamicBatchService;

@RestController
@RequestMapping("/api/batch")
public class BatchJobController {

	@Autowired
    private DynamicBatchService dynamicBatchService;
	
	@PostMapping("/run-import")
    public ResponseEntity<String> triggerBatchJob(@RequestBody BatchRequest request) {
        try {
            dynamicBatchService.runDynamicImport(
                    request.getDriverClassName(),
                    request.getDbUrl(),
                    request.getUsername(),
                    request.getPassword(),
                    request.getCsvFilePath(), // Absolute path e.g., "C:/data/import.csv"
                    request.getTableName()
            );
            return ResponseEntity.ok("Batch job started successfully for table: " + request.getTableName());
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body("Error starting batch job: " + e.getMessage());
        }
    }
	
}
