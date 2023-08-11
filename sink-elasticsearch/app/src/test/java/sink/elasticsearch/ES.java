package sink.elasticsearch;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

@Slf4j
public class ES {
    GenericContainer<?> es;

    @SneakyThrows
    public ES(boolean enableTls) {
        es = new GenericContainer<>(DockerImageName.parse("docker.elastic.co/elasticsearch/elasticsearch:7.17.11"))
                .withEnv("discovery.type", "single-node")
                .withEnv("ES_JAVA_OPTS", "-Xms512m -Xmx512m")
                .withExposedPorts(9200)
                .withLogConsumer(log -> System.out.print(log.getUtf8String()));
        if (enableTls) {
            var certsPath = getClass().getResource("/certs").getPath();
            log.info("certsPath:{}", certsPath);
            es.addFileSystemBind(certsPath, "/usr/share/elasticsearch/config/certs", BindMode.READ_ONLY);
            // tls config
            es.withEnv("xpack.security.http.ssl.enabled", "true");
            es.withEnv("xpack.security.http.ssl.key", "certs/server-pk8.key");
            es.withEnv("xpack.security.http.ssl.certificate", "certs/server.crt");
            es.withEnv("xpack.security.http.ssl.certificate_authorities", "certs/ca.crt");
        }
        log.info("bind:{}", es.getBinds());
        es.start();
    }

    int getPort() {
        return es.getFirstMappedPort();
    }

    void close() {
        es.stop();
        es.close();
    }
}
