/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.jdbc.minikdc;

import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.jdbc.SparkConnection;
import org.apache.spark.sql.service.internal.ServiceConf;
import org.junit.*;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class TestJdbcWithMiniKdcCookie {
  private static MiniSparkThriftServer miniHS2 = null;
  private static MiniHiveKdc miniHiveKdc = null;
  private Connection hs2Conn;
  File dataFile;
  protected static Map<String, String> confMap = new HashMap<String, String>();
  private static String HIVE_NON_EXISTENT_USER = "hive_no_exist";

  @BeforeClass
  public static void beforeTest() throws Exception {
    confMap.put(ServiceConf.THRIFTSERVER_TRANSPORT_MODE().key(), MiniSparkThriftServer.SS2_HTTP_MODE);
    System.err.println("Testing using HS2 mode : "
        + confMap.get(ServiceConf.THRIFTSERVER_TRANSPORT_MODE().key()));
    confMap.put(ServiceConf.THRIFTSERVER_THRIFT_HTTP_COOKIE_AUTH_ENABLED().key(), "true");
    // set a small time unit as cookie max age so that the server sends a 401
    confMap.put(ServiceConf.THRIFTSERVER_THRIFT_HTTP_COOKIE_MAX_AGE().key(), "1");
    miniHiveKdc = MiniHiveKdc.getMiniHiveKdc();
    miniHS2 = MiniHiveKdc.getMiniHS2WithKerb(miniHiveKdc, confMap);
    miniHS2.start();
  }

  @Before
  public void setUp() throws Exception {
  }

  @After
  public void tearDown() throws Exception {
    if (hs2Conn != null) {
      try {
        hs2Conn.close();
      } catch (Exception e) {
        // Ignore shutdown errors since there are negative tests
      }
    }
  }

  @AfterClass
  public static void afterTest() throws Exception {
    miniHS2.stop();
  }

  @Test
  public void testCookie() throws Exception {
    String tableName = "test_cookie";
    dataFile = new File(confMap.get("test.data.files"), "kv1.txt");
    Connection hs2Conn = getConnection(MiniHiveKdc.HIVE_TEST_USER_1);

    Statement stmt = hs2Conn.createStatement();

    // create table
    stmt.execute("create table " + tableName + "(key int, value string) ");
    stmt.execute("load data local inpath '" + dataFile + "' into table " + tableName);

    // run a query in a loop so that we hit a 401 occasionally
    for (int i = 0; i < 10; i++) {
      stmt.execute("select * from " + tableName );
    }
    stmt.execute("drop table " + tableName);
    stmt.close();
  }

  @Test
  public void testCookieNegative() throws Exception {
    try {
      // Trying to connect with a non-existent user should still fail with
      // login failure.
      getConnection(HIVE_NON_EXISTENT_USER);
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage().contains("Login failure"));
    }
  }

  private Connection getConnection(String userName) throws Exception {
    miniHiveKdc.loginUser(userName);
    return new SparkConnection(miniHS2.getJdbcURL(), new Properties());
  }
}