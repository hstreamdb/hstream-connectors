package sink.elasticsearch;

import io.hstream.HRecord;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.http.HttpHost;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Getter
public class ConnectionConfig {
    String schema;
    List<Map.Entry<String, Integer>> hosts;
    String caPath;

    public ConnectionConfig(HRecord cfg) {
        var url = cfg.getString("url");
        initUrl(url);
        if (schema.equalsIgnoreCase("https")) {
            if (!cfg.contains("ca")) {
                throw new RuntimeException("ca should not be null in https schema");
            }
            initCa(cfg.getString("ca"));
        }
    }

    @SneakyThrows
    public List<HttpHost> toHttpHosts() {
        return hosts.stream()
                .map(host -> new HttpHost(host.getKey(), host.getValue(), schema))
                .collect(Collectors.toList());
    }

    void initUrl(String url) {
        String uriStr = url.strip();
        var schemaHosts = uriStr.split("://");
        if (schemaHosts.length != 2) {
            throw new IllegalArgumentException(
                    "incorrect serviceUrl: " + uriStr + " (correct example: http://host1:9200)");
        }
        schema = schemaHosts[0];
        try {
            hosts = parseHosts(schemaHosts[1]);
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "incorrect serviceUrl: " + uriStr + " (correct example: http://host1:9200)");
        }
    }

    @SneakyThrows
    public void initCa(String ca) {
        caPath = Files.createTempFile("es_ca_", ".crt").toString();
        Files.writeString(Path.of(caPath), ca);
    }

    static List<Map.Entry<String, Integer>> parseHosts(String hosts) {
        return Arrays.stream(hosts.split(","))
                .map(ConnectionConfig::parseHost)
                .collect(Collectors.toList());
    }

    static Map.Entry<String, Integer> parseHost(String host) {
        var address_port = host.split(":");
        if (address_port.length == 1) {
            return Map.entry(host, 9200);
        }
        if (address_port.length == 2) {
            return Map.entry(address_port[0], Integer.valueOf(address_port[1]));
        }
        throw new RuntimeException("invalid host:" + host);
    }
}
