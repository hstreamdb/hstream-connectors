package io.hstream;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.*;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper;

@Slf4j
public class SourceGeneratorTest {
  HStreamHelper helper;
  static ObjectMapper mapper = new ObjectMapper();

  @BeforeEach
  void setup(TestInfo testInfo) {
    helper = new HStreamHelper(testInfo);
  }

  @SneakyThrows
  @AfterEach
  void tearDown() {
    helper.close();
  }

  @SneakyThrows
  @Test
  void testJsonSchema() {
    var schema = "{\n" +
            "  \"type\": \"object\",\n" +
            "  \"properties\": {\n" +
            "    \"k1\": {\n" +
            "      \"type\": \"integer\",\n" +
            "      \"minimum\": 0,\n" +
            "      \"maximum\": 100\n" +
            "    },\n" +
            "    \"v1\": {\n" +
            "      \"type\": \"integer\"\n" +
            "    },\n" +
            "    \"v2\": {\n" +
            "      \"type\": \"string\"\n" +
            "    },\n" +
            "    \"p1\": {\n" +
            "      \"type\": \"string\"\n" +
            "    },\n" +
            "    \"p2\": {\n" +
            "      \"type\": \"string\"\n" +
            "    }\n" +
            "  },\n" +
            "  \"required\": [\"k1\", \"v1\", \"v2\", \"p1\", \"p2\"]\n" +
            "}\n";
    var cfg =
            mapper
                    .createObjectNode()
                    .put("stream", "stream01")
                    .put("type", "json")
                    .put("batchSize", 1)
                    .put("period", 1)
                    .put("schema", schema);
    var connector =
            helper.client.createConnector(
                    CreateConnectorRequest.newBuilder()
                            .name("src01")
                            .type(ConnectorType.valueOf("SOURCE"))
                            .target("generator")
                            .config(cfg.toString())
                            .build());
    log.info("created connector:{}", connector);
    Thread.sleep(5000);
    var result = Utils.readStream(helper.client, "stream01", 10, 15);
    log.info("result:{}", result);
  }
}
