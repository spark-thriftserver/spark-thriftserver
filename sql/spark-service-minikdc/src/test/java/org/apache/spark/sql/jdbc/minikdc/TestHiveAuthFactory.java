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
import org.apache.spark.sql.service.auth.SparkAuthFactory;
import org.apache.spark.sql.service.internal.ServiceConf;
import org.junit.Assert;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;


public class TestHiveAuthFactory {
  private static SQLConf sqlConf = null;
  private static MiniHiveKdc miniHiveKdc = null;

  @BeforeClass
  public static void setUp() throws Exception {
    sqlConf = new SQLConf();
    miniHiveKdc = MiniHiveKdc.getMiniHiveKdc();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    sqlConf = null;
  }

  /**
   * Verify that delegation token manager is started with no exception for MemoryTokenStore
   * @throws Exception
   */
  @Test
  public void testStartTokenManagerForMemoryTokenStore() throws Exception {
    sqlConf.setConf(ServiceConf.THRIFTSERVER_AUTHENTICATION(),
        SparkAuthFactory.AuthTypes.KERBEROS.getAuthName());

    miniHiveKdc.loginUser(miniHiveKdc.getHiveServicePrincipal());
    String principalName = miniHiveKdc.getHiveServicePrincipal();
    System.out.println("Principal: " + principalName);
    sqlConf.setConf(ServiceConf.THRIFTSERVER_KERBEROS_PRINCIPAL(), principalName);

    String keyTabFile = miniHiveKdc.getKeyTabFile(miniHiveKdc.getHiveServicePrincipal());
    System.out.println("keyTabFile: " + keyTabFile);
    Assert.assertNotNull(keyTabFile);
    sqlConf.setConf(ServiceConf.THRIFTSERVER_KERBEROS_KEYTAB(), keyTabFile);

    SparkAuthFactory authFactory = new SparkAuthFactory(sqlConf);
    Assert.assertNotNull(authFactory);
    Assert.assertEquals(
        "org.apache.spark.sql.service.auth.thrift." +
            "HadoopThriftAuthBridge$Server$TUGIAssumingTransportFactory",
        authFactory.getAuthTransFactory().getClass().getName());
  }

  /**
   * Verify that delegation token manager is started with no exception for MemoryTokenStore
   * @throws Exception
   */
  @Test
  public void testStartTokenManagerForDBTokenStore() throws Exception {
    sqlConf.setConf(ServiceConf.THRIFTSERVER_AUTHENTICATION(),
        SparkAuthFactory.AuthTypes.KERBEROS.getAuthName());
    String principalName = miniHiveKdc.getFullHiveServicePrincipal();
    System.out.println("Principal: " + principalName);

    sqlConf.setConf(ServiceConf.THRIFTSERVER_KERBEROS_PRINCIPAL(), principalName);
    String keyTabFile = miniHiveKdc.getKeyTabFile(miniHiveKdc.getHiveServicePrincipal());
    System.out.println("keyTabFile: " + keyTabFile);
    Assert.assertNotNull(keyTabFile);
    sqlConf.setConf(ServiceConf.THRIFTSERVER_KERBEROS_KEYTAB(), keyTabFile);

    sqlConf.setConf(ServiceConf.THRIFTSERVER_CLUSTER_DELEGATION_TOKEN_STORE_CLS(),
        "org.apache.sparkSession.sql.service.auth.thrift.MemoryTokenStore");

    SparkAuthFactory authFactory = new SparkAuthFactory(sqlConf);
    Assert.assertNotNull(authFactory);
    Assert.assertEquals("org.apache.spark.sql.service.auth.thrift." +
            "HadoopThriftAuthBridge$Server$TUGIAssumingTransportFactory",
        authFactory.getAuthTransFactory().getClass().getName());
  }
}
