/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.jdbc.minikdc;

import com.google.common.io.Files;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.sql.service.utils.Utils;
import org.junit.*;

import java.io.File;

import static org.junit.Assert.*;

public class TestMiniHiveKdc {

  private static File baseDir;
  private MiniHiveKdc miniHiveKdc;

  @BeforeClass
  public static void beforeTest() throws Exception {
    baseDir = Files.createTempDir();
    baseDir.deleteOnExit();
  }

  @Before
  public void setUp() throws Exception {
    miniHiveKdc = MiniHiveKdc.getMiniHiveKdc();
  }

  @After
  public void tearDown() throws Exception {
    miniHiveKdc.shutDown();
  }

  @Test
  public void testLogin() throws Exception {
    String servicePrinc = miniHiveKdc.getHiveServicePrincipal();
    assertNotNull(servicePrinc);
    miniHiveKdc.loginUser(servicePrinc);
    assertTrue(UserGroupInformation.isLoginKeytabBased());
    UserGroupInformation ugi = Utils.getUGI();
    assertEquals(MiniHiveKdc.HIVE_SERVICE_PRINCIPAL, ugi.getShortUserName());
  }

  @AfterClass
  public static void afterTest() throws Exception {

  }

}