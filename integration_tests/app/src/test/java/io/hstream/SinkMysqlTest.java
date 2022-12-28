package io.hstream;

import io.hstream.external.Mysql;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

@Slf4j
public class SinkMysqlTest {
  HStreamHelper helper;
  Mysql mysql;

  @BeforeEach
  void setup(TestInfo testInfo) throws Exception {
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

  @AfterEach
  void tearDown() throws Exception {
    mysql.close();
    helper.close();
  }
}
