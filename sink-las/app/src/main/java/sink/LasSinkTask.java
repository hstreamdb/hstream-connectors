package sink;

import com.bytedance.las.tunnel.ActionType;
import com.bytedance.las.tunnel.TableTunnel;
import com.bytedance.las.tunnel.TunnelConfig;
import com.bytedance.las.tunnel.authentication.AkSkAccount;
import com.bytedance.las.tunnel.data.PartitionSpec;
import com.bytedance.las.tunnel.session.StreamUploadSession;
import com.fasterxml.jackson.databind.JsonNode;
import io.hstream.HRecord;
import io.hstream.io.*;
import io.hstream.io.impl.SinkTaskContextImpl;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

import java.util.List;
import java.util.Map;

import static com.bytedance.las.tunnel.ActionType.*;
import static com.bytedance.las.tunnel.TunnelConfig.SERVICE_REGION;

@Slf4j
public class LasSinkTask implements SinkTask {
    HRecord cfg;
    StreamUploadSession session;
    Schema schema;
    SinkTaskContext ctx;

    // extra time field
    ExtraTimeField extraTimeField;

    @SneakyThrows
    @Override
    public void run(HRecord cfg, SinkTaskContext ctx) {
        this.cfg = cfg;
        this.ctx = ctx;
        this.extraTimeField = ExtraTimeField.fromConfig(cfg);
        init();
        // config
        ctx.handle(this::handleWithException);
    }

    @SneakyThrows
    void init() {
        var accessId = cfg.getString("accessId");
        var accessKey = cfg.getString("accessKey");
        var endpoint = cfg.getString("endpoint");
        var region = cfg.getString("region");
        var db = cfg.getString("database");
        var table = cfg.getString("table");
        var actionType = getActionType(cfg.getString("mode"));
        TunnelConfig tunnelConfig = new TunnelConfig.Builder()
                .config(SERVICE_REGION, region)
                .build();
        var account = new AkSkAccount(accessId, accessKey);
        TableTunnel tableTunnel = new TableTunnel(account, tunnelConfig);
        tableTunnel.setEndPoint(endpoint);
        log.info("creating upload session");
        if (cfg.contains("tableType") && cfg.getString("tableType").equalsIgnoreCase("normal")) {
            session = tableTunnel.createStreamUploadSession(db, table, actionType);
        } else {
            session = tableTunnel.createStreamUploadSession(db, table, PartitionSpec.SELF_ADAPTER, actionType);
        }
        log.info("created upload session");
        schema = session.getFullSchema();
        log.info("schema:{}", schema);
    }

    void clean() {
        session = null;
    }

    void handleWithException(SinkRecordBatch batch) {
        try {
            if (session == null) {
                init();
            }
            handle(batch);
        } catch (Throwable e) {
            clean();
            throw e;
        }
    }

    @SneakyThrows
    void handle(SinkRecordBatch batch) {
        var recordWriter = session.openRecordWriter(/*blockId*/0);
        for (var r : batch.getSinkRecords()) {
            var targetRecord = convertRecord(r);
            if (targetRecord == null) {
                // skip error record
                continue;
            }
            var valueRecord = targetRecord.get("value");
            if(valueRecord == null) {
                continue;
            }
            if (extraTimeField != null) {
                valueRecord.put(extraTimeField.getFieldName(), extraTimeField.getValue());
            }
            recordWriter.write(targetRecord);
        }
        recordWriter.close();
        session.commit(List.of(0L), List.of(recordWriter.getAttemptId()));
    }

    GenericData.Record convertRecord(SinkRecord sinkRecord) {
        try {
            var lasRecord = LasRecord.fromSinkRecord(sinkRecord);
            // lasRecord should be like: {"key": {"id": 0, ...}, "value": {"id": 0, "name": "...", ...} } ,
            // and this format would be compatible with debezium sources.
            var valueRecord = (Map<String,Object>)lasRecord.getRecord().get("value");
            if(valueRecord == null) {
                valueRecord= lasRecord.getRecord();
            }
            var r = new GenericData.Record(schema);
            for (var entry : valueRecord.entrySet()) {
                var value = entry.getValue();
                if (schema.getField(entry.getKey()).schema().getType().equals(Schema.Type.INT)) {
                    var intValue = (int) value;
                    r.put(entry.getKey(), intValue);
                } else {
                    r.put(entry.getKey(), value);
                }
            }
            return r;
        } catch (Exception e) {
            log.warn("convertRecord failed:" + e.getMessage());
            if (!ctx.getSinkSkipStrategy().trySkip(sinkRecord, e.getMessage())) {
                throw new ConnectorExceptions.FailFastError(e.getMessage());
            }
            return null;
        }
    }

    static ActionType getActionType(String mode) {
        switch (mode.toUpperCase()) {
//            case "INSERT":
//                return INSERT;
            case "OVERWRITE_INSERT":
                return OVERWRITE_INSERT;
            default:
                return UPSERT;
        }
    }

    @Override
    public CheckResult check(HRecord config) {
        try {
            ExtraTimeField.fromConfig(config);
            return CheckResult.ok();
        } catch (Exception e) {
            return CheckResult.builder()
                    .result(false)
                    .type(CheckResult.CheckResultType.CONFIG)
                    .message("invalid extra datetime config: " + e.getMessage())
                    .build();
        }
    }

    @Override
    public JsonNode spec() {
        return Utils.getSpec(this, "/spec.json");
    }

    @SneakyThrows
    @Override
    public void stop() {
        session.close();
    }

    public static void main(String[] args) {
        new TaskRunner().run(args, new LasSinkTask(), new SinkTaskContextImpl());
    }
}