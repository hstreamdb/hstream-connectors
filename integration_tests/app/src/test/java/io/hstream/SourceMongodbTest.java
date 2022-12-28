package io.hstream;

import io.hstream.external.Mongodb;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

@Slf4j
public class SourceMongodbTest {
  HStreamHelper helper;
  Mongodb mongodb;

  @BeforeEach
  void setup(TestInfo testInfo) throws Exception {
    helper = new HStreamHelper(testInfo);
    mongodb = new Mongodb();
    log.info("set up environment");
  }

  @Test
  void testFullSync() {
    Utils.testSourceFullSync(helper, mongodb);
  }

  @AfterEach
  void tearDown() throws Exception {
    mongodb.close();
    helper.close();
  }
}
