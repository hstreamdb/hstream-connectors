package io.hstream;

import io.hstream.external.Postgresql;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

@Slf4j
public class SinkPostgresqlTest {
  HStreamHelper helper;
  Postgresql pg;

  @BeforeEach
  void setup(TestInfo testInfo) throws Exception {
    helper = new HStreamHelper(testInfo);
    pg = new Postgresql();
    log.info("set up environment");
  }

  @Test
  void testFullSync() throws Exception {
    Utils.testSinkFullSync(helper, pg);
  }

  @AfterEach
  void tearDown() throws Exception {
    pg.close();
    helper.close();
  }
}
