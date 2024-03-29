/*
 * This Java source file was generated by the Gradle 'init' task.
 */
package io.hstream;

import io.hstream.external.Postgresql;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

class SourcePostgresqlTest {
  Postgresql pg;
  HStreamHelper helper;

  @BeforeEach
  void setup(TestInfo testInfo) throws Exception {
    // setup HStreamDB
    helper = new HStreamHelper(testInfo);
    // setup postgresql
    pg = new Postgresql();
  }

  @AfterEach
  void tearDown() throws Exception {
    pg.close();
    helper.close();
  }

  @Test
  void testFullSync() throws Exception {
    Utils.testSourceFullSync(helper, pg);
  }
}
