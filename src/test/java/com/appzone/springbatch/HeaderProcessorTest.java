package com.appzone.springbatch;

import com.appzone.springbatch.DTO.CsvHeaderMetadata;
import com.appzone.springbatch.processors.HeaderProcessor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.ai.chat.client.ChatClient;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class HeaderProcessorTest {

    @Mock
    private ChatClient chatClient;

    @InjectMocks
    private HeaderProcessor processor;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    private String createTestCsv(String filename, String... headers) throws IOException {
        Path csvPath = tempDir.resolve(filename);
        String headerLine = String.join(",", headers);
        Files.writeString(csvPath, headerLine + "\nval1,val2,val3");
        return csvPath.toAbsolutePath().toString();
    }

    @Test
    @DisplayName("Should handle standard valid headers")
    void testStandardHeaders() throws Exception {
        String csvPath = createTestCsv("standard.csv", "First Name", "Last Name", "Email Address");

        CsvHeaderMetadata metadata = processor.extractMetadata(csvPath);
        List<String> result = metadata.getSanitizedHeaders();

        assertThat(result).containsExactly("first_name", "last_name", "email_address");
    }

    @Test
    @DisplayName("Should clean special characters, whitespace, and leading/trailing underscores")
    void testSpecialCharactersAndWhitespace() throws Exception {
        String csvPath = createTestCsv("special.csv", "  User ID!! ", "### @$%!", "does_this__person____recieved_pension");

        CsvHeaderMetadata metadata = processor.extractMetadata(csvPath);
        List<String> result = metadata.getSanitizedHeaders();

        assertThat(result).containsExactly("user_id", "unnamed_column", "does_this_person_recieved_pension");
    }

    @Test
    @DisplayName("Should rename duplicate headers with sequence suffixes")
    void testDuplicateHeaders() throws Exception {
        String csvPath = createTestCsv("duplicates.csv", "Age", "Age", "Age");

        CsvHeaderMetadata metadata = processor.extractMetadata(csvPath);
        List<String> result = metadata.getSanitizedHeaders();

        assertThat(result).containsExactly("age", "age_1", "age_2");
    }

    @Test
    @DisplayName("Should handle empty and whitespace-only input strings")
    void testEmptyAndBlankHeaders() throws Exception {
        String csvPath = createTestCsv("empty_headers.csv", "", "   ", "!!!");

        CsvHeaderMetadata metadata = processor.extractMetadata(csvPath);
        List<String> result = metadata.getSanitizedHeaders();

        assertThat(result).containsExactly("unnamed_column", "unnamed_column_1", "unnamed_column_2");
    }

    @Test
    @DisplayName("Should handle mixed case, duplicates, and special characters simultaneously")
    void testComplexMixedInput() throws Exception {
        String csvPath = createTestCsv("complex.csv", "Name", "NAME", "  name  ", "Phone #", "Phone #");

        CsvHeaderMetadata metadata = processor.extractMetadata(csvPath);
        List<String> result = metadata.getSanitizedHeaders();

        assertThat(result).containsExactly("name", "name_1", "name_2", "phone", "phone_1");
    }
}