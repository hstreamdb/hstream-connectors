package sink.jdbc;

import io.hstream.HRecord;
import io.hstream.io.CheckResult;
import io.hstream.io.SinkTask;
import io.hstream.io.SinkTaskContext;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;
import sink.jdbc.JdbcRecord.RecordType;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.math3.util.Pair;

@Slf4j
abstract public class JdbcSinkTask implements SinkTask {
    HashMap<List<String>, PreparedStatement> preparedUpsertStmts = new HashMap<>();
    HashMap<List<String>, PreparedStatement> preparedDeleteStmts = new HashMap<>();
    Connection conn;

    @Override
    public void run(HRecord cfg, SinkTaskContext ctx) {
        init(cfg);
        ctx.handle((batch) -> {
            var jdbcRecords = batch.getSinkRecords().stream()
                    .map(Utils::jdbcRecordFromSinkRecord)
                    .collect(Collectors.toList());
            handleRecordsWithException(jdbcRecords);
        });
    }

    void clean() {
        preparedUpsertStmts.clear();
        preparedDeleteStmts.clear();
        conn = null;
    }

    @SneakyThrows
    void handleRecordsWithException(List<JdbcRecord> records) {
        try {
            handleRecords(records);
        } catch (Throwable e) {
            clean();
            throw e;
        }
    }

    @SneakyThrows
    void handleRecords(List<JdbcRecord> records) {
        var groups = splitRecords(records);
        for (var recordsPair : groups) {
            switch (recordsPair.getKey()) {
                case PLAIN_UPSERT:
                    upsertPainRecords(recordsPair.getValue());
                    return;
                case UPSERT:
                    upsert(recordsPair.getValue());
                    return;
                case DELETE:
                    delete(recordsPair.getValue());
                    return;
            }
        }
    }

    // split records for batching
    List<Pair<RecordType, List<JdbcRecord>>> splitRecords(List<JdbcRecord> records) {
        var result = new LinkedList<Pair<RecordType, List<JdbcRecord>>>();
        var cache = new LinkedList<JdbcRecord>();
        var currentType = RecordType.PLAIN_UPSERT;
        for (var record : records) {
            var recordType = record.getRecordType();
            if (currentType.equals(recordType)) {
                cache.add(record);
            } else {
                if (!cache.isEmpty()) {
                    result.add(new Pair<>(currentType, cache));
                    cache.clear();
                }
                currentType = recordType;
                cache.add(record);
            }
        }
        if (!cache.isEmpty()) {
            result.add(new Pair<>(currentType, cache));
        }
        return result;
    }

    void upsert(List<JdbcRecord> records) throws SQLException {
        var fields = new LinkedList<>(records.get(0).getRow().keySet());
        var keys = new ArrayList<>(records.get(0).getKeys().keySet());
        PreparedStatement stmt = getUpsertStmt(fields, keys);
        for(var record : records) {
            for(int i = 0; i < fields.size(); i++) {
                stmt.setObject(i + 1, record.getRow().get(fields.get(i)));
            }
            stmt.addBatch();
        }
        stmt.executeBatch();
    }

    void upsertPainRecords(List<JdbcRecord> records) throws SQLException {
        var fields = new ArrayList<>(records.get(0).getRow().keySet());
        var keys = getPrimaryKeys();
        PreparedStatement stmt = getUpsertStmt(fields, keys);
        for(var record : records) {
            for(int i = 0; i < fields.size(); i++) {
                stmt.setObject(i + 1, record.getRow().get(fields.get(i)));
            }
            stmt.addBatch();
        }
        stmt.executeBatch();
    }

    void delete(List<JdbcRecord> records) throws SQLException {
        var keyFields = new ArrayList<>(records.get(0).keys.keySet());
        PreparedStatement stmt = getDeleteStmt(keyFields);
        for(var record : records) {
            for(int i = 0; i < keyFields.size(); i++) {
                stmt.setObject(i + 1, record.keys.get(keyFields.get(i)));
            }
            stmt.addBatch();
        }
        stmt.executeBatch();
    }

    Connection getConn() {
        if (conn == null) {
            conn = getNewConn();
        }
        return conn;
    }

    @SneakyThrows
    PreparedStatement getUpsertStmt(List<String> fields, List<String> keys) {
        if (preparedUpsertStmts.containsKey(keys)) {
            return preparedUpsertStmts.get(keys);
        }
        var stmtSql = getUpsertSql(fields, keys);
        log.info("upsert stmtSQL:{}", stmtSql);
        var stmt = getConn().prepareStatement(stmtSql);
        preparedUpsertStmts.put(keys, stmt);
        return stmt;
    }

    @SneakyThrows
    public PreparedStatement getDeleteStmt(List<String> keys) {
        if (preparedDeleteStmts.containsKey(keys)) {
            preparedDeleteStmts.get(keys);
        }
        var stmtSql = getDeleteSql(keys);
        log.info("delete stmtSql:{}", stmtSql);
        var stmt = getConn().prepareStatement(stmtSql);
        preparedDeleteStmts.put(keys, stmt);
        return stmt;
    }

    @Override
    public CheckResult check(HRecord config) {
        try {
            init(config.getHRecord("connector"));
        } catch (Throwable e) {
            e.printStackTrace();
            log.info("check failed, {}", e.getMessage());
            return CheckResult.builder()
                    .result(false)
                    .type(CheckResult.CheckResultType.CONNECTION)
                    .message("get connector failed")
                    .build();
        }
        var keys = getPrimaryKeys();
        if (keys == null || keys.isEmpty()) {
            return CheckResult.builder()
                    .result(false)
                    .type(CheckResult.CheckResultType.KEYS)
                    .message("table/primary keys not found")
                    .build();
        }
        return CheckResult.ok();
    }


    abstract void init(HRecord cfg);
    abstract String getUpsertSql(List<String> fields, List<String> keys);
    abstract String getDeleteSql(List<String> keys);
    abstract Connection getNewConn();
    abstract List<String> getPrimaryKeys();

    @Override
    public void stop() {}
}
