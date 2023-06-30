package io.hstream;

import io.hstream.external.Mongodb;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.*;

@Slf4j
public class SinkMongodbTest {
  HStreamHelper helper;
  Mongodb mongodb;

  @BeforeEach
  void setup(TestInfo testInfo) throws Exception {
    helper = new HStreamHelper(testInfo);
    mongodb = new Mongodb();
    log.info("set up environment");
  }

  @Disabled
  @Test
  void testFullSync() {
    Utils.testSinkFullSync(helper, mongodb);
  }

  @AfterEach
  void tearDown() throws Exception {
    mongodb.close();
    helper.close();
  }
}
