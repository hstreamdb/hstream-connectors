package sink.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.hstream.HRecord;
import io.hstream.io.*;
import lombok.Builder;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;
import org.elasticsearch.client.RestClient;

import javax.annotation.Nullable;
import javax.net.ssl.SSLContext;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.util.List;

@Slf4j
public class EsClient {
    final ConnectionConfig connectionConfig;
    ElasticsearchClient esClient;

    public EsClient(HRecord cfg) {
        connectionConfig = new ConnectionConfig(cfg);
    }

    @SneakyThrows
    ElasticsearchClient getConnection() {
        synchronized (connectionConfig) {
            if (esClient == null) {
                esClient = createNewConnection();
            }
            return esClient;
        }
    }

    @SneakyThrows
    ElasticsearchClient createNewConnection() {
        RestClient restClient;
        if (connectionConfig.getScheme().equalsIgnoreCase("https")) {
            Path caCertificatePath = Paths.get(connectionConfig.getCaPath());
            CertificateFactory factory =
                    CertificateFactory.getInstance("X.509");
            Certificate trustedCa;
            try (InputStream is = Files.newInputStream(caCertificatePath)) {
                trustedCa = factory.generateCertificate(is);
            }
            KeyStore trustStore = KeyStore.getInstance("pkcs12");
            trustStore.load(null, null);
            trustStore.setCertificateEntry("ca", trustedCa);
            SSLContextBuilder sslContextBuilder = SSLContexts.custom()
                    .loadTrustMaterial(trustStore, null);
            final SSLContext sslContext = sslContextBuilder.build();
            restClient = RestClient.builder(connectionConfig.toHttpHosts().toArray(new HttpHost[0]))
                    .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setSSLContext(sslContext))
                    .build();
        } else {
            restClient = RestClient.builder(connectionConfig.toHttpHosts().toArray(new HttpHost[0])).build();
        }

        ElasticsearchTransport transport = new RestClientTransport(
                restClient, new JacksonJsonpMapper());
        return new ElasticsearchClient(transport);
    }

    void reset() {
        synchronized (connectionConfig) {
            esClient = null;
        }
    }

    @SneakyThrows
    void writeRecords(String index, SinkRecordBatch batch, SinkSkipStrategy skipStrategy) {
        var client = getConnection();
        BulkRequest.Builder br = new BulkRequest.Builder();
        for (var record : batch.getSinkRecords()) {
            var mappedRecord = mapRecord(record, skipStrategy);
            if (mappedRecord == null) {
                continue;
            }
            br.operations(op -> op.index(idx -> idx.index(index)
                    .id(mappedRecord.getId())
                    .document(mappedRecord.getSource())));
        }
        BulkResponse result;
        try {
            result = client.bulk(br.build());
        } catch (Exception e) {
            log.warn("bulk records failed:{}", e.getMessage());
            reset();
            throw e;
        }
        if (result != null && result.errors()) {
            log.warn("bulk response error");
            for (var error : result.items()) {
                log.warn("bulk error: {}", error);
            }
            var reason = result.items().get(0).toString();
            throw new ConnectorExceptions.InvalidBatchError(batch, reason);
        }
    }

    @Getter
    @Builder
    static class MappedRecord {
        String id;
        JsonNode source;
    }

    @Nullable
    MappedRecord mapRecord(SinkRecord record, SinkSkipStrategy skipStrategy) {
        try {
            var data = Utils.mapper.readValue(record.getRecord(), ObjectNode.class);
            String id = record.getRecordId();
            if (data.get("_id") != null) {
                var idNode = data.remove("_id");
                if (idNode.isTextual()) {
                    id = idNode.asText();
                } else {
                    throw new RuntimeException("_id field must be string");
                }
            }
            return MappedRecord.builder().id(id).source(data).build();
        } catch (Exception e) {
            if (!skipStrategy.trySkip(record, e.getMessage())) {
                throw new ConnectorExceptions.FailFastError("unskippable record");
            }
            return null;
        }
    }

    @SneakyThrows
    List<Hit<JsonNode>> readRecords(String index) {
        var client = getConnection();
        var resp = client.search(g -> g.index(index), JsonNode.class);
        return resp.hits().hits();
    }
}
