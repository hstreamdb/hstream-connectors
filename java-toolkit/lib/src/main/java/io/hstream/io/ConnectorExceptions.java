package io.hstream.io;

import com.fasterxml.jackson.databind.ObjectMapper;

public class ConnectorExceptions {
    static ObjectMapper mapper = new ObjectMapper();

    public abstract static class BaseException extends RuntimeException {
        abstract public String errorRecord();
        public boolean shouldRetry() {
            return true;
        }
    }

    public static BaseException fromThrowable(Throwable e) {
        if (e instanceof BaseException) {
            return (BaseException) e;
        } else {
            return new UnknownError(e.getMessage());
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

    public static class FailFastError extends BaseException {
        String reason;

        public FailFastError(String reason) {
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
            return reason;
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

    public static class InvalidBatchError extends SinkException {
        String beginRecordId;
        String tailRecordId;
        int count;

        public InvalidBatchError(SinkRecordBatch batch, String reason) {
            super(reason);
            var records = batch.getSinkRecords();
            count = records.size();
            if (count > 0) {
                beginRecordId = records.get(0).recordId;
                tailRecordId = records.get(count - 1).recordId;
            }
        }

        @Override public String errorRecord() {
            return mapper.createObjectNode() .put("type", "InvalidBatchSinkRecord")
                    .put("reason", reason)
                    .put("beginRecordId", beginRecordId)
                    .put("tailRecordId", tailRecordId)
                    .put("count", count)
                    .toString();
        }

        @Override
        public boolean shouldRetry() {
            return true;
        }
    }
}
