package com.appzone.springbatch;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import com.appzone.springbatch.processors.HeaderProcessor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import java.util.List;
import static org.assertj.core.api.Assertions.assertThat;

class HeaderProcessorTest {

    // Assuming processHeaders is package-private for testing
	
    private HeaderProcessor processor;

    

    @BeforeEach
    void setUp() {
        // Instantiate directly without needing Spring IoC Container
        processor = new HeaderProcessor();
    }
    
    @Test
    @DisplayName("Should handle standard valid headers")
    void testStandardHeaders() {
        String[] input = {"First Name", "Last Name", "Email Address"};
        
        List<String> result = processor.processHeaders(input);

        assertThat(result).containsExactly("first_name", "last_name", "email_address");
    }

    @Test
    @DisplayName("Should clean special characters, whitespace, and leading/trailing underscores")
    void testSpecialCharactersAndWhitespace() {
        String[] input = {"  User ID!! ", "### @$%!", "does_this__person____recieved_pension"};

        List<String> result = processor.processHeaders(input);

        assertThat(result).containsExactly("user_id", "unnamed_column", "does_this_person_recieved_pension");
    }

    @Test
    @DisplayName("Should rename duplicate headers with sequence suffixes")
    void testDuplicateHeaders() {
        String[] input = {"Age", "Age", "Age"};

        List<String> result = processor.processHeaders(input);

        // First occurrence gets base name, subsequent get _1, _2
        assertThat(result).containsExactly("age", "age_1", "age_2");
    }

    @Test
    @DisplayName("Should handle empty and whitespace-only input strings")
    void testEmptyAndBlankHeaders() {
        String[] input = {"", "   ", "!!!"};

        List<String> result = processor.processHeaders(input);

        // All clean up to empty, so they fall back to unnamed_column and handle deduplication
        assertThat(result).containsExactly("unnamed_column", "unnamed_column_1", "unnamed_column_2");
    }

    @Test
    @DisplayName("Should handle mixed case, duplicates, and special characters simultaneously")
    void testComplexMixedInput() {
        String[] input = {"Name", "NAME", "  name  ", "Phone #", "Phone #"};

        List<String> result = processor.processHeaders(input);

        assertThat(result).containsExactly("name", "name_1", "name_2", "phone", "phone_1");
    }
}
