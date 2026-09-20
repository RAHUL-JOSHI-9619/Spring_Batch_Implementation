package com.appzone.springbatch.DTO;

import java.util.List;

public class CsvHeaderMetadata {
    private final List<String> sanitizedHeaders;

    public CsvHeaderMetadata(List<String> sanitizedHeaders) {
        this.sanitizedHeaders = sanitizedHeaders;
    }

    public List<String> getSanitizedHeaders() {
        return sanitizedHeaders;
    }

    public int getColumnCount() {
        return sanitizedHeaders != null ? sanitizedHeaders.size() : 0;
    }
}