package com.appzone.springbatch.controllers;




import java.sql.Connection;
import java.sql.DriverManager;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import com.appzone.springbatch.DTO.DatabaseConnectionRequest;

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
}
