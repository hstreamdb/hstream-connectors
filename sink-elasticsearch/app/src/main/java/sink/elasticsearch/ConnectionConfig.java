package sink.elasticsearch;

import io.hstream.HRecord;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.SystemDefaultCredentialsProvider;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@Getter
public class ConnectionConfig {
    String scheme;
    List<Map.Entry<String, Integer>> hosts;
    String caPath;
    String auth = "none";
    CredentialsProvider credentialsProvider;

    public ConnectionConfig(HRecord cfg) {
        scheme = cfg.getString("scheme");
        hosts = parseHosts(cfg.getString("hosts"));
        if (scheme.equalsIgnoreCase("https")) {
            if (cfg.contains("ca")) {
                var ca = cfg.getString("ca");
                if (!ca.isBlank()) {
                    initCa(ca);
                }
            }
        }
        if (cfg.contains("auth")) {
            auth = cfg.getString("auth");
        }
        if (auth.equals("basic")) {
            credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY,
                    new UsernamePasswordCredentials(cfg.getString("username"), cfg.getString("password")));
        }
    }

    @SneakyThrows
    public List<HttpHost> toHttpHosts() {
        return hosts.stream()
                .map(host -> new HttpHost(host.getKey(), host.getValue(), scheme))
                .collect(Collectors.toList());
    }

    @SneakyThrows
    public CredentialsProvider getCredentialsProvider() {
        return credentialsProvider;
    }

    @SneakyThrows
    public void initCa(String ca) {
        caPath = Files.createTempFile("es_ca_", ".crt").toString();
        Files.writeString(Path.of(caPath), ca);
    }

    static List<Map.Entry<String, Integer>> parseHosts(String hosts) {
        try {
            return Arrays.stream(hosts.split(","))
                    .map(ConnectionConfig::parseHost)
                    .collect(Collectors.toList());
        } catch (Exception e) {
            log.info(e.getMessage());
            throw new IllegalArgumentException(
                    "incorrect hosts: " + hosts + " (correct example: host1:9200,host2:9200)");
        }
    }

    static Map.Entry<String, Integer> parseHost(String host) {
        var address_port = host.split(":");
        if (address_port.length == 1) {
            return Map.entry(host, 9200);
        }
        if (address_port.length == 2) {
            return Map.entry(address_port[0], Integer.valueOf(address_port[1]));
        }
        throw new IllegalArgumentException("invalid host:" + host);
    }
}
