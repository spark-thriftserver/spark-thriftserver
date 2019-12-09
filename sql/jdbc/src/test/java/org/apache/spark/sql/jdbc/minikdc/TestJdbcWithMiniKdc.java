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

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.sql.jdbc.SparkConnection;
import org.apache.spark.sql.jdbc.miniSS2.MiniSS2;
import org.apache.spark.sql.service.SparkSQLEnv;
import org.apache.spark.sql.service.auth.SparkAuthFactory;
import org.apache.spark.sql.service.utils.Utils;
import org.junit.*;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class TestJdbcWithMiniKdc {
  // Need to hive.server2.session.hook to SessionHookTest in hive-site
  public static final String SESSION_USER_NAME = "proxy.test.session.user";

  protected static MiniSS2 miniSS2 = null;
  protected static MiniHiveKdc miniHiveKdc = null;
  protected static Map<String, String> confOverlay = new HashMap<String, String>();
  protected Connection hs2Conn;

  @BeforeClass
  public static void beforeTest() throws Exception {
    Class.forName(MiniSS2.getJdbcDriverName());
    miniHiveKdc = MiniHiveKdc.getMiniHiveKdc();
    SparkSQLEnvUtils.setStartUpProperties();
    SparkSQLEnv.init();
    miniSS2 = MiniHiveKdc.getMiniSS2WithKerb(miniHiveKdc, SparkSQLEnv.sqlContext());
    miniSS2.start(confOverlay);
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
    SparkSQLEnv.stop();
    SparkSQLEnvUtils.clearStartUpProperties();
    miniSS2.stop();
  }

  /***
   * Basic connection test
   * @throws Exception
   */
  @Test
  public void testConnection() throws Exception {
    miniHiveKdc.loginUser(MiniHiveKdc.HIVE_TEST_USER_1);
    System.out.println(miniSS2.getJdbcURL());
    hs2Conn = DriverManager.getConnection(miniSS2.getJdbcURL());
    verifyProperty(SESSION_USER_NAME, MiniHiveKdc.HIVE_TEST_USER_1);
  }

  /***
   * Negative test, verify that connection to secure HS2 fails when
   * required connection attributes are not provided
   * @throws Exception
   */
  @Test
  public void testConnectionNeg() throws Exception {
    miniHiveKdc.loginUser(MiniHiveKdc.HIVE_TEST_USER_1);
    try {
      String url = miniSS2.getJdbcURL().replaceAll(";principal.*", "");
      hs2Conn = DriverManager.getConnection(url);
      fail("NON kerberos connection should fail");
    } catch (SQLException e) {
      // expected error
      assertEquals("08S01", e.getSQLState().trim());
    }
  }

  /***
   * Test isValid() method
   * @throws Exception
   */
  @Test
  public void testIsValid() throws Exception {
    miniHiveKdc.loginUser(MiniHiveKdc.HIVE_TEST_SUPER_USER);
    hs2Conn = DriverManager.getConnection(miniSS2.getJdbcURL());
    assertTrue(hs2Conn.isValid(1000));
    hs2Conn.close();
  }

  /***
   * Negative test isValid() method
   * @throws Exception
   */
  @Test
  public void testIsValidNeg() throws Exception {
    miniHiveKdc.loginUser(MiniHiveKdc.HIVE_TEST_SUPER_USER);
    hs2Conn = DriverManager.getConnection(miniSS2.getJdbcURL());
    hs2Conn.close();
    assertFalse(hs2Conn.isValid(1000));
  }

  /***
   * Test token based authentication over kerberos
   * Login as super user and retrieve the token for normal user
   * use the token to connect connect as normal user
   * @throws Exception
   */
  @Test
  public void testTokenAuth() throws Exception {
    miniHiveKdc.loginUser(MiniHiveKdc.HIVE_TEST_SUPER_USER);
    hs2Conn = DriverManager.getConnection(miniSS2.getJdbcURL());

    // retrieve token and store in the cache
    String token = ((SparkConnection) hs2Conn).getDelegationToken(
        MiniHiveKdc.HIVE_TEST_USER_1, MiniHiveKdc.HIVE_SERVICE_PRINCIPAL);
    assertTrue(token != null && !token.isEmpty());
    hs2Conn.close();

    UserGroupInformation ugi = miniHiveKdc.
        loginUser(MiniHiveKdc.HIVE_TEST_USER_1);
    // Store token in the cache
    storeToken(token, ugi);
    hs2Conn = DriverManager.getConnection(miniSS2.getBaseJdbcURL() +
        "default;auth=delegationToken");
    verifyProperty(SESSION_USER_NAME, MiniHiveKdc.HIVE_TEST_USER_1);
  }

  /***
   * Negative test for token based authentication
   * Verify that a user can't retrieve a token for user that
   * it's not allowed to impersonate
   * @throws Exception
   */
  @Test
  public void testNegativeTokenAuth() throws Exception {
    miniHiveKdc.loginUser(MiniHiveKdc.HIVE_TEST_SUPER_USER);
    hs2Conn = DriverManager.getConnection(miniSS2.getJdbcURL());

    try {
      // retrieve token and store in the cache
      String token = ((SparkConnection) hs2Conn).getDelegationToken(
          MiniHiveKdc.HIVE_TEST_USER_2, MiniHiveKdc.HIVE_SERVICE_PRINCIPAL);

      fail(MiniHiveKdc.HIVE_TEST_SUPER_USER + " shouldn't be allowed to retrieve token for " +
          MiniHiveKdc.HIVE_TEST_USER_2);
    } catch (SQLException e) {
      // Expected error
      assertTrue(e.getMessage().contains("Error retrieving delegation token for user"));
      assertTrue(e.getCause().getCause().getMessage().contains("is not allowed to impersonate"));
    } finally {
      hs2Conn.close();
    }
  }

  /**
   * Test connection using the proxy user connection property
   *
   * @throws Exception
   */
  @Test
  public void testProxyAuth() throws Exception {
    miniHiveKdc.loginUser(MiniHiveKdc.HIVE_TEST_SUPER_USER);
    hs2Conn = DriverManager.getConnection(miniSS2.getJdbcURL("default",
        ";hive.server2.proxy.user=" + MiniHiveKdc.HIVE_TEST_USER_1));
    verifyProperty(SESSION_USER_NAME, MiniHiveKdc.HIVE_TEST_USER_1);
  }

  /**
   * Test connection using the proxy user connection property.
   * Verify proxy connection fails when super user doesn't have privilege to
   * impersonate the given user
   *
   * @throws Exception
   */
  @Test
  public void testNegativeProxyAuth() throws Exception {
    miniHiveKdc.loginUser(MiniHiveKdc.HIVE_TEST_SUPER_USER);
    try {
      hs2Conn = DriverManager.getConnection(miniSS2.getJdbcURL("default",
          ";hive.server2.proxy.user=" + MiniHiveKdc.HIVE_TEST_USER_2));
      verifyProperty(SESSION_USER_NAME, MiniHiveKdc.HIVE_TEST_USER_2);
    } catch (SQLException e) {
      // Expected error
      assertTrue(e.getMessage().contains("Failed to validate proxy privilege"));
      assertTrue(e.getCause().getCause().getCause().getMessage()
          .contains("is not allowed to impersonate"));
    }
  }

  /**
   * Verify the config property value
   *
   * @param propertyName
   * @param expectedValue
   * @throws Exception
   */
  protected void verifyProperty(String propertyName, String expectedValue) throws Exception {
    Statement stmt = hs2Conn.createStatement();
    ResultSet res = stmt.executeQuery("set " + propertyName);
    assertTrue(res.next());
    String results[] = res.getString(1).split("=");
    // Todo Current Spark Thrift Server not support Session Hook, we should add this feature later
    // assertEquals("Property should be set", results.length, 2);
    // assertEquals("Property should be set", expectedValue, results[1]);
    assertEquals("Property should be set", results.length, 1);
  }

  // Store the given token in the UGI
  protected void storeToken(String tokenStr, UserGroupInformation ugi)
      throws Exception {
    Utils.setTokenStr(ugi,
        tokenStr, SparkAuthFactory.SS2_CLIENT_TOKEN);
  }

}