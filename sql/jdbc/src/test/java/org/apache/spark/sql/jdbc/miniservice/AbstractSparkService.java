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

package org.apache.spark.sql.jdbc.miniservice;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.internal.StaticSQLConf;

/***
 * Base class for Spark service
 * AbstractSparkService.
 *
 */
public abstract class AbstractSparkService {
  private SQLContext sqlContext = null;
  private String hostname;
  private int binaryPort;
  private int httpPort;
  private boolean startedSparkService = false;
  private List<String> addedProperties = new ArrayList<String>();

  public AbstractSparkService(SQLContext sqlContext, String hostname,
                              int binaryPort, int httpPort) {
    this.sqlContext = sqlContext;
    this.hostname = hostname;
    this.binaryPort = binaryPort;
    this.httpPort = httpPort;
  }

  /**
   * Get SQLContext
   */
  public SQLContext getSqlContext() {
    return sqlContext;
  }

  /**
   * Get Spark conf
   *
   * @return
   */
  public SQLConf getSqlConf() {
    return sqlContext.conf();
  }

  /**
   * Get config property
   *
   * @param propertyKey
   * @return
   */
  public String getConfProperty(String propertyKey) {
    return sqlContext.conf().getConfString(propertyKey);
  }

  /**
   * Set config property
   *
   * @param propertyKey
   * @param propertyValue
   */
  public void setConfProperty(String propertyKey, String propertyValue) {
    System.setProperty(propertyKey, propertyValue);
    sqlContext.setConf(propertyKey, propertyValue);
    addedProperties.add(propertyKey);
  }

  /**
   * Create system properties set by this server instance. This ensures that
   * the changes made by current test are not impacting subsequent tests.
   */
  public void clearProperties() {
    for (String propKey : addedProperties) {
      System.clearProperty(propKey);
    }
  }

  /**
   * Retrieve warehouse directory
   *
   * @return
   */
  public Path getWareHouseDir() {
    return new Path(sqlContext.conf().getConf(StaticSQLConf.WAREHOUSE_PATH()));
  }

  public void setWareHouseDir(String wareHouseURI) {
    verifyNotStarted();
    System.setProperty("hive.metastore.warehouse.dir", wareHouseURI);
    sqlContext.conf().setConf(StaticSQLConf.WAREHOUSE_PATH(), wareHouseURI);
  }

  /**
   * Set service host
   *
   * @param hostName
   */
  public void setHost(String hostName) {
    this.hostname = hostName;
  }

  // get service host
  public String getHost() {
    return hostname;
  }

  /**
   * Set binary service port #
   *
   * @param portNum
   */
  public void setBinaryPort(int portNum) {
    this.binaryPort = portNum;
  }

  /**
   * Set http service port #
   *
   * @param portNum
   */
  public void setHttpPort(int portNum) {
    this.httpPort = portNum;
  }

  // Get binary service port #
  public int getBinaryPort() {
    return binaryPort;
  }

  // Get http service port #
  public int getHttpPort() {
    return httpPort;
  }

  public boolean isStarted() {
    return startedSparkService;
  }

  public void setStarted(boolean sparkServiceStatus) {
    this.startedSparkService = sparkServiceStatus;
  }

  protected void verifyStarted() {
    if (!isStarted()) {
      throw new IllegalStateException("SparkServer2 is not running");
    }
  }

  protected void verifyNotStarted() {
    if (isStarted()) {
      throw new IllegalStateException("SparkServer2 already running");
    }
  }

}
