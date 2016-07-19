package com.example.FileToJdbcApp;

import com.datatorrent.api.LocalMode;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import javax.validation.ConstraintViolationException;
import java.io.IOException;

/**
 * Test the DAG declaration in local mode.<br>
 * The assumption to run this test case is that test_jdbc_table
 * and meta-table are created already.
 */
public class ApplicationTest {

  @Test
  @Ignore
  public void testCsvParserApp() throws IOException, Exception {
    try {
      LocalMode lma = LocalMode.newInstance();
      Configuration conf = new Configuration(false);
      conf.addResource(this.getClass().getResourceAsStream("/META-INF/properties.xml"));
      lma.prepareDAG(new FileToJdbcCsvParser(), conf);
      LocalMode.Controller lc = lma.getController();
      lc.run(50000); // runs for 10 seconds and quits
    } catch (ConstraintViolationException e) {
      Assert.fail("constraint violations: " + e.getConstraintViolations());
    }
  }

  @Test
  @Ignore
  public void testCustomParserApp() throws IOException, Exception {
    try {
      LocalMode lma = LocalMode.newInstance();
      Configuration conf = new Configuration(false);
      conf.addResource(this.getClass().getResourceAsStream("/META-INF/properties.xml"));
      lma.prepareDAG(new FileToJdbcCustomParser(), conf);
      LocalMode.Controller lc = lma.getController();
      lc.run(50000); // runs for 10 seconds and quits
    } catch (ConstraintViolationException e) {
      Assert.fail("constraint violations: " + e.getConstraintViolations());
    }
  }
}
