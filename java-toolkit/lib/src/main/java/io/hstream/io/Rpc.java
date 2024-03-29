package io.hstream.io;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.hstream.io.internal.Channel;

import java.util.concurrent.CompletableFuture;

public class Rpc {
    static ObjectMapper mapper = new ObjectMapper();
    Channel channel;

    public Rpc(Channel channel) {
        this.channel = channel;
    }

    public CompletableFuture<Void> kvSet(String key, String val) {
        var kvMsg = mapper.createObjectNode()
                .put("key", key)
                .put("value", val);
        return channel.call(ConnectorRequestName.KvSet.name(), kvMsg).thenApply(c -> null);
    }

    public CompletableFuture<String> kvGet(String key) {
        var kvMsg = mapper.createObjectNode().put("key", key);
        return channel.call(ConnectorRequestName.KvGet.name(), kvMsg)
                .thenApply(n -> n.isNull() ? null : n.asText());
    }

    public CompletableFuture<Void> report(ReportMessage reportMessage) {
        var body = mapper.valueToTree(reportMessage);
        return channel.call(ConnectorRequestName.Report.name(), body).thenApply(c -> null);
    }
}
