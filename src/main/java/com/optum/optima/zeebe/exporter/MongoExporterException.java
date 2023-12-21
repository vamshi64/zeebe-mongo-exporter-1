package com.optum.optima.zeebe.exporter;

public class MongoExporterException extends RuntimeException {

    public MongoExporterException(final String message) {
        super(message);
    }

    public MongoExporterException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
