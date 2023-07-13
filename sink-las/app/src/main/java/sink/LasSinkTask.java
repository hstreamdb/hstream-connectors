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
import java.util.stream.Collectors;

import static com.bytedance.las.tunnel.ActionType.*;
import static com.bytedance.las.tunnel.TunnelConfig.SERVICE_REGION;

@Slf4j
public class LasSinkTask implements SinkTask {
    HRecord cfg;
    StreamUploadSession session;
    Schema schema;

    @SneakyThrows
    @Override
    public void run(HRecord cfg, SinkTaskContext ctx) {
        this.cfg = cfg;
        init();
        // config
        ctx.handle((batch) -> handleWithException(batch.getSinkRecords().stream()
                .map(LasRecord::fromSinkRecord)
                .collect(Collectors.toList())));
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
        session = tableTunnel.createStreamUploadSession(db, table, PartitionSpec.SELF_ADAPTER, actionType);
        log.info("created upload session");
        schema = session.getFullSchema();
        log.info("schema:{}", schema);
    }

    void clean() {
        session = null;
    }

    void handleWithException(List<LasRecord> records) {
        try {
            if (session == null) {
                init();
            }
            handle(records);
        } catch (Throwable e) {
            clean();
            throw e;
        }
    }

    @SneakyThrows
    void handle(List<LasRecord> records) {
        var recordWriter = session.openRecordWriter(/*blockId*/0);
        for (var r : records) {
            recordWriter.write(convertRecord(r));
        }
        recordWriter.close();
        session.commit(List.of(0L), List.of(recordWriter.getAttemptId()));
    }

    GenericData.Record convertRecord(LasRecord lasRecord) {
        var r = new GenericData.Record(schema);
        for (var entry : lasRecord.getRecord().entrySet()) {
            var value = entry.getValue();
            if (schema.getField(entry.getKey()).schema().getType().equals(Schema.Type.INT)) {
                var intValue = (int) value;
                r.put(entry.getKey(), intValue);
            } else {
                r.put(entry.getKey(), value);
            }
        }
        return r;
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