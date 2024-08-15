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
            if (extraTimeField != null) {
                targetRecord.put(extraTimeField.getFieldName(), extraTimeField.getValue());
            }
            recordWriter.write(targetRecord);
        }
        recordWriter.close();
        session.commit(List.of(0L), List.of(recordWriter.getAttemptId()));
    }

    void putValue(GenericData.Record record, Schema schema, String field, Object value) {
       Schema valueSchema = schema.getField(field).schema();
       switch (valueSchema.getType()) {
           // for number type, it must be a double type.
           case INT:
               int intValue = ((Double) value).intValue();
               record.put(field, intValue);
               break;
           case LONG:
               long longValue = ((Double) value).longValue();
               record.put(field, longValue);
               break;
           case FLOAT:
               float floatValue = ((Double) value).floatValue();
               record.put(field, floatValue);
               break;
           case UNION:
               // by default, a field in LAS table is nullable, ant its schema is a union like [type, null].
               List<Schema> unionTypes = valueSchema.getTypes();
               if(unionTypes.stream().anyMatch(it -> it.getType().equals(Schema.Type.INT))) {
                   int unionIntValue = ((Double) value).intValue();
                   record.put(field, unionIntValue);
                   break;
               } else if (unionTypes.stream().anyMatch(it -> it.getType().equals(Schema.Type.LONG))) {
                   long unionLongValue = ((Double) value).longValue();
                   record.put(field, unionLongValue);
                   break;
               } else if (unionTypes.stream().anyMatch(it -> it.getType().equals(Schema.Type.FLOAT))) {
                   float unionFloatValue = ((Double) value).floatValue();
                   record.put(field, unionFloatValue);
                   break;
               }
           default:
               record.put(field, value);

       }
    }
    GenericData.Record convertRecord(SinkRecord sinkRecord) {
        try {
            var lasRecord = LasRecord.fromSinkRecord(sinkRecord);
            // lasRecord should be like: {"key": {"id": 0, ...}, "value": {"id": 0, "name": "...", ...} } ,
            // and this format would be compatible with debezium sources.
            Map<String, Object> valueRecord = (Map<String,Object>)lasRecord.getRecord().get("value");
            if(valueRecord == null) {
                valueRecord= lasRecord.getRecord();
            }
            var r = new GenericData.Record(schema);
            for (var entry : valueRecord.entrySet()) {
                var value = entry.getValue();
                putValue(r, schema, entry.getKey(), value);
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