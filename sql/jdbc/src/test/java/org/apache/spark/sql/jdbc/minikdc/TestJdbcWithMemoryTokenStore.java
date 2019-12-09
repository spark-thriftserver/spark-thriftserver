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

import org.apache.spark.sql.jdbc.miniSS2.MiniSS2;
import org.apache.spark.sql.service.SparkSQLEnv;
import org.apache.spark.sql.service.internal.ServiceConf;
import org.junit.BeforeClass;

public class TestJdbcWithMemoryTokenStore extends TestJdbcWithMiniKdc{

  @BeforeClass
  public static void beforeTest() throws Exception {
    SparkSQLEnvUtils.setStartUpProperties();
    SparkSQLEnv.init();
    Class.forName(MiniSS2.getJdbcDriverName());
    confOverlay.put(ServiceConf.THRIFTSERVER_CLUSTER_DELEGATION_TOKEN_STORE_CLS().key(), "org.apache.spark.sql.service.auth.thrift.MemoryTokenStore");
    miniHiveKdc = MiniHiveKdc.getMiniHiveKdc();
    miniSS2 = MiniHiveKdc.getMiniHS2WithKerbWithRemoteHMS(miniHiveKdc, SparkSQLEnv.sqlContext());
    miniSS2.start(confOverlay);
  }
}