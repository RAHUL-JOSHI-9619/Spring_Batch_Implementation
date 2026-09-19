package com.appzone.springbatch.DTO;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class BatchRequest {
	private String driverClassName;
    private String dbUrl;
    private String username;
    private String password;
    private String csvFilePath;
    private String tableName;
}
