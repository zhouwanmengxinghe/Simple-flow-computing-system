package com.streaming.exceptions;

public class StreamingException extends RuntimeException {
    private final ErrorCode errorCode;

    public StreamingException(ErrorCode errorCode) {
        super(errorCode.getMessage());
        this.errorCode = errorCode;
    }

    public StreamingException(ErrorCode errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    public StreamingException(ErrorCode errorCode, Throwable cause) {
        super(errorCode.getMessage(), cause);
        this.errorCode = errorCode;
    }

    public StreamingException(ErrorCode errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    public ErrorCode getErrorCode() {
        return errorCode;
    }

    public enum ErrorCode {
        OPERATOR_EXECUTION_ERROR("Error executing operator"),
        CONFIGURATION_ERROR("Error loading configuration"),
        KAFKA_CONNECTION_ERROR("Error connecting to Kafka"),
        DATA_PROCESSING_ERROR("Error processing data"),
        INVALID_PARAMETER("Invalid parameter provided"),
        IO_ERROR("Error performing I/O operation");

        private final String message;

        ErrorCode(String message) {
            this.message = message;
        }

        public String getMessage() {
            return message;
        }
    }
}