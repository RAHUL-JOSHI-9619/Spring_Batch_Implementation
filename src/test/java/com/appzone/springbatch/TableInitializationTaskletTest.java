package com.appzone.springbatch;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.jdbc.core.JdbcTemplate;

import com.appzone.springbatch.processors.HeaderProcessor;
import com.appzone.springbatch.tasklets.TableInitializationTasklet;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class TableInitializationTaskletTest {

    @Mock
    private JdbcTemplate jdbcTemplate;

    @Mock
    private ChatClient chatClient;

    @Mock
    private ChatClient.ChatClientRequestSpec requestSpec;

    @Mock
    private ChatClient.CallResponseSpec callResponseSpec;

    // Mockito will inject the mocked chatClient into headerProcessor
    @InjectMocks
    private HeaderProcessor headerProcessor;

    private TableInitializationTasklet tasklet;

    @BeforeEach
    void setUp() {
        tasklet = new TableInitializationTasklet("test_table", jdbcTemplate, headerProcessor);
    }

    @Test
    @DisplayName("Should leave headers under 64 characters unchanged and skip AI call")
    void testProcessHeaders_Under64Chars_DoesNotCallAI() {
        String[] rawHeaders = {"First Name", "Last Name", "Email Address"};

        List<String> processed = headerProcessor.processHeaders(rawHeaders);

        assertEquals(3, processed.size());
        assertEquals("first_name", processed.get(0));
        assertEquals("last_name", processed.get(1));
        assertEquals("email_address", processed.get(2));

        // Verify ChatClient was NEVER called
        verifyNoInteractions(chatClient);
    }

    @Test
    @DisplayName("Should invoke AI ChatClient when header exceeds 64 characters")
    void testProcessHeaders_Exceeds64Chars_CallsAIAndShortens() {
        String longHeader = "Employee Annual Performance Evaluation And Feedback Metrics Score For Year Twenty Twenty Six";
        String[] rawHeaders = {longHeader};

        // Mock fluent ChatClient chain
        when(chatClient.prompt()).thenReturn(requestSpec);
        when(requestSpec.user(anyString())).thenReturn(requestSpec);
        when(requestSpec.call()).thenReturn(callResponseSpec);
        when(callResponseSpec.content()).thenReturn("emp_perf_eval_score_2026");

        List<String> processed = headerProcessor.processHeaders(rawHeaders);

        assertEquals(1, processed.size());
        assertEquals("emp_perf_eval_score_2026", processed.get(0));
        assertTrue(processed.get(0).length() <= 64);

        // Verify ChatClient WAS called once
        verify(chatClient, times(1)).prompt();
    }

    @Test
    @DisplayName("Should fallback to substring(0, 64) if AI call throws an exception")
    void testProcessHeaders_AIThrowsException_FallbackToSubstring() {
        String longHeader = "customer_account_transaction_history_detailed_summary_information_report_identifier";
        String[] rawHeaders = {longHeader};

        // Mock ChatClient throwing an exception (e.g. timeout or API failure)
        when(chatClient.prompt()).thenThrow(new RuntimeException("AI Service Unavailable"));

        List<String> processed = headerProcessor.processHeaders(rawHeaders);

        assertEquals(1, processed.size());
        
        String expectedFallback = longHeader.toLowerCase().substring(0, 64);
        assertEquals(expectedFallback, processed.get(0));
        assertEquals(64, processed.get(0).length());
    }
}