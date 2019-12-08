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

import org.apache.spark.DebugFilesystem;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.catalyst.optimizer.ConvertToLocalRelation;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.internal.StaticSQLConf;
import org.apache.spark.sql.jdbc.SparkDriver;
import org.apache.spark.sql.service.SparkThriftServer2;
import org.apache.spark.sql.service.internal.ServiceConf;
import org.apache.spark.sql.test.TestSparkSession;

import java.util.Iterator;
import java.util.Map;

public class MiniSparkThriftServer {
  public static String SS2_HTTP_MODE = "http";
  public static String SS2_BINARY_MODE = "binary";

  TestSparkSession spark = null;
  private SparkThriftServer2 sparkServer = null;

  MiniSparkThriftServer(SparkConf sparkConf) {
    spark = new TestSparkSession(sparkConf);
  }


  public static String getJdbcDriverName() {
    return SparkDriver.class.getName();
  }

  public void start() {
    sparkServer = SparkThriftServer2.startWithContext(spark.newSession().sqlContext());
  }

  public void stop() {
    sparkServer.stop();
    spark.stop();
  }

  public String getJdbcURL() {
    int serverPort = (int) sparkServer.getSqlConf().getConf(ServiceConf.THRIFTSERVER_THRIFT_PORT());
    String principal = sparkServer.getSqlConf().getConf(ServiceConf.THRIFTSERVER_KERBEROS_PRINCIPAL());
    return "jdbc:spark://localhost:" + serverPort + "/default;principal=" + principal;
  }

  public String getJdbcURL(String db, String conf) {
    int serverPort = (int) sparkServer.getSqlConf().getConf(ServiceConf.THRIFTSERVER_THRIFT_PORT());
    String principal = sparkServer.getSqlConf().getConf(ServiceConf.THRIFTSERVER_KERBEROS_PRINCIPAL());
    return "jdbc:spark://localhost:" + serverPort + "/" + db + ";principal=" + principal + conf;
  }

  public String getBaseJdbcURL() {
    return getJdbcURL();
  }

  public static class Builder {
    private SparkConf sparkConf = getSparkConf();

    private SparkConf getSparkConf() {
      SparkConf conf = new SparkConf()
          .set("spark.hadoop.fs.file.impl", DebugFilesystem.class.getName())
          .set("spark.unsafe.exceptionOnMemoryLeak", "true")
          .set(SQLConf.CODEGEN_FALLBACK().key(), "false")
          // Disable ConvertToLocalRelation for better test coverage. Test cases built on
          // LocalRelation will exercise the optimization rules better by disabling it as
          // this rule may potentially block testing of other optimization rules such as
          // ConstantPropagation etc.
          .set(SQLConf.OPTIMIZER_EXCLUDED_RULES().key(), ConvertToLocalRelation.ruleName());
      conf.set(StaticSQLConf.WAREHOUSE_PATH(),
          conf.get(StaticSQLConf.WAREHOUSE_PATH()) + "/" + this.getClass().getCanonicalName());
      return conf;
    }

    public Builder withConf(Map<String, String> conf) {
      Iterator<Map.Entry<String, String>> iterator = conf.entrySet().iterator();
      while (iterator.hasNext()) {
        Map.Entry<String, String> kv = iterator.next();
        sparkConf.set(kv.getKey(), kv.getValue());
      }
      return this;
    }

    public Builder withMiniKdc(String principal, String keyTab) {
      sparkConf.set(ServiceConf.THRIFTSERVER_KERBEROS_PRINCIPAL(), principal);
      sparkConf.set(ServiceConf.THRIFTSERVER_KERBEROS_KEYTAB(), keyTab);
      return this;
    }

    public Builder withAuthenticationType(String type) {
      sparkConf.set(ServiceConf.THRIFTSERVER_AUTHENTICATION(), type);
      return this;
    }

    public MiniSparkThriftServer build() {
      return new MiniSparkThriftServer(sparkConf);
    }

  }

}
