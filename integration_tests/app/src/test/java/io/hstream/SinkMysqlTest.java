package io.hstream;

import io.hstream.external.Mysql;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.*;

import static io.hstream.Utils.pullImage;

@Slf4j
public class SinkMysqlTest {
  HStreamHelper helper;
  Mysql mysql;

  @BeforeEach
  @Timeout(300)
  void setup(TestInfo testInfo) throws Exception {
    pullImage("hstreamdb/sink-mysql:latest");
    helper = new HStreamHelper(testInfo);
    mysql = new Mysql();
    log.info("set up environment");
  }

  @Test
  void testFullSync() throws Exception {
    Utils.testSinkFullSync(helper, mysql);
  }

  @Test
  void testPlainHRecord() throws Exception {
    Utils.testSinkFullSync(helper, mysql, Utils.IORecordType.PLAIN);
  }

  @Test
  void testRawJsonRecord() throws Exception {
    Utils.testSinkFullSync(helper, mysql, Utils.IORecordType.RAW_JSON);
  }

  @Test
  void testBsonRecord() throws Exception {
    Utils.testSinkFullSync(helper, mysql, Utils.IORecordType.BSON);
  }

  @Test
  void testOffsets() throws Exception {
    var connector = Utils.testSinkFullSync(helper, mysql, Utils.IORecordType.KEY_VALUE, false);
    Thread.sleep(5000);
    var offsets = helper.client.getConnector(connector.getName()).getOffsets();
    log.info("offsets:{}", offsets);
    Assertions.assertFalse(offsets.isEmpty());
  }

  @Test
  void testMultiShards() throws Exception {
    var connector = Utils.testSinkFullSync(helper, mysql, Utils.IORecordType.KEY_VALUE, false, 3);
    Thread.sleep(5000);
    var offsets = helper.client.getConnector(connector.getName()).getOffsets();
    log.info("offsets:{}", offsets);
    Assertions.assertFalse(offsets.isEmpty());
  }

  @Test
  void testSpec() throws Exception {
    var spec = helper.client.getConnectorSpec("SINK", "mysql");
    log.info("spec:{}", spec);
    Utils.mapper.readTree(spec);
  }

  @AfterEach
  void tearDown() throws Exception {
    mysql.close();
    helper.close();
  }
}
