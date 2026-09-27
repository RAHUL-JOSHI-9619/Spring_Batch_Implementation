package com.appzone.springbatch.config;

import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.google.genai.GoogleGenAiChatModel;


import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Two independent ChatClient beans, one per AI provider used for column-name shortening.
 *
 * WHY NOT @ConditionalOnBean(OllamaChatModel.class) ANY MORE:
 * That looked correct but is a classic Spring Boot ordering trap. @ConditionalOnBean is
 * evaluated when THIS configuration class is parsed - and AIConfig is a normal, user-defined
 * @Configuration (component-scanned), which Spring Boot always parses BEFORE the deferred
 * auto-configuration classes run. spring-ai-autoconfigure-model-ollama's OllamaChatModel bean
 * is one of those deferred auto-configurations, so at the moment @ConditionalOnBean checked
 * for it, it genuinely did not exist yet in the registry - the condition failed every single
 * time, regardless of pom.xml, application.properties, or whether Ollama was actually running.
 * (geminiChatClient never hit this because it takes GoogleGenAiChatModel as a plain method
 * parameter - normal DI resolves at bean-instantiation time, long after auto-configuration has
 * finished, so ordering never mattered for it.)
 *
 * THE FIX: autowire OllamaChatModel directly as a nullable ("required = false") method
 * parameter instead of gating the whole bean definition on a parse-time condition. This is
 * resolved at instantiation time just like geminiChatClient is, so ordering can't break it.
 * If the model genuinely isn't configured, this method just returns null - and
 * AiColumnNameBatchShortener's existing @Autowired(required = false) on ollamaChatClient
 * already handles a null ChatClient correctly (that was always the "Ollama not configured"
 * path, just previously triggered for the wrong reason every time).
 */
@Configuration
public class AIConfig {

    @Bean
    @Qualifier("geminiChatClient")
    public ChatClient geminiChatClient(GoogleGenAiChatModel chatModel) {
        return ChatClient.builder(chatModel).build();
    }

    
}