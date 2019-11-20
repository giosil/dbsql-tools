package org.dew.test;

import org.dew.dbsql.CommandSQL;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class TestDBSQL extends TestCase {
  
  public TestDBSQL(String testName) {
    super(testName);
  }
  
  public static Test suite() {
    return new TestSuite(TestDBSQL.class);
  }
  
  public void testApp() {
    CommandSQL.printHelp();
  }
  
}
