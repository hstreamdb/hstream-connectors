package sink.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import co.elastic.clients.util.BinaryData;
import co.elastic.clients.util.ContentType;
import com.fasterxml.jackson.databind.JsonNode;
import io.hstream.HRecord;
import io.hstream.io.SinkRecord;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;
import org.elasticsearch.client.RestClient;

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
    ConnectionConfig connectionConfig;
    ElasticsearchClient esClient;

    public EsClient(HRecord cfg) {
        connectionConfig = new ConnectionConfig(cfg);
    }

    @SneakyThrows
    synchronized ElasticsearchClient getConnection() {
        if (esClient == null) {
            esClient = createNewConnection();
        }
        return esClient;
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
        esClient = null;
    }

    @SneakyThrows
    void writeRecords(String index, List<SinkRecord> records) {
        var client = getConnection();
        BulkRequest.Builder br = new BulkRequest.Builder();
        for (var record : records) {
            BinaryData data = BinaryData.of(record.record, ContentType.APPLICATION_JSON);
            br.operations(op -> op.index(idx -> idx.index(index)
                    .id(record.getRecordId())
                    .document(data)));
        }
        try {
            client.bulk(br.build());
        } catch (Exception e) {
            log.warn("bulk records failed:{}", e.getMessage());
            reset();
            throw e;
        }
    }

    @SneakyThrows
    List<Hit<JsonNode>> readRecords(String index) {
        var client = getConnection();
        var resp = client.search(g -> g.index(index), JsonNode.class);
        return resp.hits().hits();
    }
}
