package com.appzone.springbatch.controllers;




import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import com.appzone.springbatch.DTO.DatabaseConnectionRequest;
import com.appzone.springbatch.DTO.TableExistsRequest;

@RestController
@RequestMapping("/api/database")
@CrossOrigin
public class DatabaseConnectionController {

    @PostMapping("/test-connection")
    public ResponseEntity<?> testConnection(
            @RequestBody DatabaseConnectionRequest request) {

        try {

            if (request.getUrl() == null ||
                request.getUrl().isBlank()) {

                return ResponseEntity.badRequest()
                        .body("Database URL is required.");
            }

            if (request.getUsername() == null ||
                request.getUsername().isBlank()) {

                return ResponseEntity.badRequest()
                        .body("Username is required.");
            }

            if (request.getPassword() == null) {

                return ResponseEntity.badRequest()
                        .body("Password is required.");
            }


            // Actually attempt the database connection
            try (Connection connection =
                         DriverManager.getConnection(
                                 request.getUrl(),
                                 request.getUsername(),
                                 request.getPassword())) {

                if (connection.isValid(5)) {

                    return ResponseEntity.ok(
                            "Database connection successful."
                    );
                }

                return ResponseEntity.status(500)
                        .body("Database connection failed.");

            }

        } catch (Exception e) {

            return ResponseEntity.status(400)
                    .body("Database connection failed: "
                            + e.getMessage());
        }
    }

    // Used by the frontend BEFORE the batch job is started, purely to decide whether
    // to show the "a table with this name already exists" warning. It does not change
    // anything in the database - TableInitializationTasklet still does the actual
    // drop/create once the user has confirmed they want to proceed.
    @PostMapping("/check-table")
    public ResponseEntity<?> checkTableExists(@RequestBody TableExistsRequest request) {

        try {

            if (request.getUrl() == null || request.getUrl().isBlank()) {
                return ResponseEntity.badRequest().body("Database URL is required.");
            }

            if (request.getTableName() == null || request.getTableName().isBlank()) {
                return ResponseEntity.badRequest().body("Table name is required.");
            }

            try (Connection connection = DriverManager.getConnection(
                    request.getUrl(), request.getUsername(), request.getPassword())) {

                boolean exists = tableExists(connection, request.getTableName());

                Map<String, Object> body = new HashMap<>();
                body.put("exists", exists);
                body.put("tableName", request.getTableName());
                return ResponseEntity.ok(body);
            }

        } catch (Exception e) {

            return ResponseEntity.status(400)
                    .body("Could not check for existing table: " + e.getMessage());
        }
    }

    // Different databases store unquoted identifiers differently (Oracle -> uppercase,
    // Postgres -> lowercase, MySQL/SQL Server -> as typed), and a wide CSV from a previous
    // run may have left behind a VIEW instead of a TABLE (see TableMergeTasklet). So this
    // checks TABLE and VIEW, trying the name as typed, upper-cased and lower-cased.
    private boolean tableExists(Connection connection, String tableName) throws SQLException {
        DatabaseMetaData metaData = connection.getMetaData();
        String[] types = { "TABLE", "VIEW" };

        for (String candidate : new String[] { tableName, tableName.toUpperCase(), tableName.toLowerCase() }) {
            try (ResultSet rs = metaData.getTables(connection.getCatalog(), connection.getSchema(), candidate, types)) {
                if (rs.next()) {
                    return true;
                }
            }
        }
        return false;
    }
}