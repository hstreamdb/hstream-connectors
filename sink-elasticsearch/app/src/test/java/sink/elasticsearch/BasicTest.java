package sink.elasticsearch;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.hstream.HRecord;
import io.hstream.HRecordBuilder;
import io.hstream.io.CheckResult;
import io.hstream.io.SinkRecord;
import io.hstream.io.test.FakeKvStore;
import io.hstream.io.test.FakeSinkTaskContext;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.*;
import org.testcontainers.shaded.org.apache.commons.io.IOUtils;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
public class BasicTest {
    FakeSinkTaskContext ctx;
    ES es;
    EsClient esClient;
    HRecord cfg;
    String index = "index01";
    static ObjectMapper mapper = new ObjectMapper();
    Integer defaultPort = 1234;

    @SneakyThrows
    @BeforeEach
    void setup(TestInfo info) {
        String password = info.getTags().contains("enableBasicAuth") ? "testPassword" : null;

        var enableTls = info.getTags().contains("enableTls");
        es = new ES(enableTls, password);
        var port = es.getPort();
        if (info.getTags().contains("useDefaultPort")) {
            port = defaultPort;
        }
        String hosts = "localhost:" + port;
        String scheme = enableTls ? "https" : "http";

        var configBuilder = HRecord.newBuilder()
                .put("stream", "stream01")
                .put("hosts", hosts)
                .put("scheme", scheme)
                .put("index", index);
        if (enableTls) {
            var ca = IOUtils.toString(getClass().getResourceAsStream("/certs/ca.crt"), StandardCharsets.UTF_8);
            configBuilder = configBuilder.put("ca", ca);
        }
        if (password != null) {
            if (info.getTags().contains("useWrongPassword")) {
                password = "WrongPassword";
            }
            configBuilder = configBuilder
                    .put("auth", "basic")
                    .put("username", "elastic")
                    .put("password", password);
        }
        cfg = configBuilder.build();

        // ctx
        ctx = new FakeSinkTaskContext();
        ctx.init(cfg, new FakeKvStore());

        // client
        esClient = new EsClient(cfg);
    }

    @AfterEach
    void tearDown() {
        es.close();
    }

    @SneakyThrows
    @Test
    void testFullSync() {
        ctx.appendRecords(randSinkRecords(10));
        var task = new ElasticsearchSinkTask();
        new Thread(() -> task.run(cfg, ctx)).start();
        Thread.sleep(5000);
        var rs = esClient.readRecords(index);
        log.info("rs:{}", rs);
        Assertions.assertEquals(10, rs.size());
    }

    @SneakyThrows
    @Test
    void testWithId() {
        ctx.appendRecords(randSinkRecords(10, true));
        var task = new ElasticsearchSinkTask();
        new Thread(() -> task.run(cfg, ctx)).start();
        Thread.sleep(5000);
        var rs = esClient.readRecords(index);
        log.info("rs:{}", rs);
        Assertions.assertEquals(10, rs.size());
    }

    @Tag("enableTls")
    @SneakyThrows
    @Test
    void testTls() {
        ctx.appendRecords(randSinkRecords(10));
        var task = new ElasticsearchSinkTask();
        new Thread(() -> task.run(cfg, ctx)).start();
        Thread.sleep(5000);
        var rs = esClient.readRecords(index);
        log.info("rs:{}", rs);
        Assertions.assertEquals(10, rs.size());
    }

    @Tag("enableBasicAuth")
    @SneakyThrows
    @Test
    void testBasicAuth() {
        testFullSync();
    }

    @Tag("useDefaultPort")
    @Test
    void testConnectionCheckWithWrongPort() {
        var task = new ElasticsearchSinkTask();
        var result = task.check(cfg);
        System.out.println("check result:" + result);
        Assertions.assertFalse(result.getResult());
        Assertions.assertEquals(CheckResult.CheckResultType.CONNECTION, result.getType());
    }

    @Tag("enableBasicAuth")
    @Tag("useWrongPassword")
    @Test
    void testConnectionCheckWithWrongPassword() {
        var task = new ElasticsearchSinkTask();
        var result = task.check(cfg);
        System.out.println("check result:" + result);
        Assertions.assertFalse(result.getResult());
        Assertions.assertEquals(CheckResult.CheckResultType.CONNECTION, result.getType());
    }

    @Test
    void testConnectionCheck() {
        var task = new ElasticsearchSinkTask();
        var result = task.check(cfg);
        Assertions.assertTrue(result.getResult());
    }

    List<SinkRecord> randSinkRecords(int count) {
        return randSinkRecords(count, false);
    }

    List<SinkRecord> randSinkRecords(int count, boolean includeId) {
        assert count > 0;
        return IntStream.range(0, count)
                .mapToObj(i -> randSinkRecord(includeId))
                .collect(Collectors.toList());

    }

    SinkRecord randSinkRecord() {
        return randSinkRecord(false);
    }

    SinkRecord randSinkRecord(boolean includeId) {
        var recordObject = mapper.createObjectNode()
                .put("k1", UUID.randomUUID().toString());
        if (includeId) {
            recordObject.put("_id", UUID.randomUUID().toString());
        }
        return SinkRecord.builder()
                .recordId(UUID.randomUUID().toString())
                .record(recordObject.toString().getBytes(StandardCharsets.UTF_8))
                .build();
    }
}
