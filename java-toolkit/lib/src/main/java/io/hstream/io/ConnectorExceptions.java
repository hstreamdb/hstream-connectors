package io.hstream.io;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Builder;
import lombok.Getter;
import lombok.SneakyThrows;

public class ConnectorExceptions {
    static ObjectMapper mapper = new ObjectMapper();

    public abstract static class BaseException extends RuntimeException {
        abstract public String errorRecord();
        public boolean shouldRetry() {
            return true;
        }
    }

    public static class UnknownError extends BaseException {
        String reason;

        public UnknownError(String reason) {
            this.reason = reason;
        }

        @Override
        public String errorRecord() {
            return mapper.createObjectNode()
                    .put("type", "UNKNOWN_ERROR")
                    .put("reason", reason).toString();
        }
    }

    public static class SinkException extends BaseException {
        String reason;

        public SinkException(String reason) {
            this.reason = reason;
        }

        @Override
        public String errorRecord() {
            return mapper.createObjectNode()
                    .put("type", "Sink")
                    .put("reason", reason)
                    .toString();
        }

        @Override
        public String getMessage() {
            return errorRecord();
        }
    }

    public static class InvalidSinkRecordException extends SinkException {
        SinkRecord sinkRecord;

        public InvalidSinkRecordException(SinkRecord sinkRecord, String reason) {
            super(reason);
            this.sinkRecord = sinkRecord;
        }

        @Override
        public String errorRecord() {
            return mapper.createObjectNode()
                    .put("type", "InvalidSinkRecord")
                    .put("reason", reason)
                    .set("recordId", sinkRecord.toJsonNode())
                    .toString();
        }

        @Override
        public boolean shouldRetry() {
            return false;
        }
    }
}
