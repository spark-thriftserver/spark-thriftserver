/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.service.cli.thrift;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportFactory;

import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.service.auth.SparkAuthFactory;
import org.apache.spark.sql.service.auth.SparkAuthUtils;
import org.apache.spark.sql.service.cli.CLIService;
import org.apache.spark.sql.service.internal.ServiceConf;
import org.apache.spark.sql.service.server.ThreadFactoryWithName;

public class ThriftBinaryCLIService extends ThriftCLIService {

  public ThriftBinaryCLIService(CLIService cliService, SQLContext sqlContext) {
    super(cliService, sqlContext, ThriftBinaryCLIService.class.getSimpleName());
  }

  @Override
  public void run() {
    try {
      // Server thread pool
      String threadPoolName = "SparkThriftServer-Handler-Pool";
      ExecutorService executorService = new ThreadPoolExecutor(minWorkerThreads, maxWorkerThreads,
          workerKeepAliveTime, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(),
          new ThreadFactoryWithName(threadPoolName));

      // Thrift configs
      sparkAuthFactory = new SparkAuthFactory(sqlConf);
      TTransportFactory transportFactory = sparkAuthFactory.getAuthTransFactory();
      TProcessorFactory processorFactory = sparkAuthFactory.getAuthProcFactory(this);
      TServerSocket serverSocket = null;
      List<String> sslVersionBlacklist = new ArrayList<String>();
      for (String sslVersion : sqlConf.getConf(ServiceConf.THRIFTSERVER_SSL_PROTOCOL_BLACKLIST())
          .split(",")) {
        sslVersionBlacklist.add(sslVersion);
      }
      if (!((boolean) sqlConf.getConf(ServiceConf.THRIFTSERVER_USE_SSL()))) {
        serverSocket = SparkAuthUtils.getServerSocket(sparkHost, portNum);
      } else {
        String keyStorePath = sqlConf.getConf(ServiceConf.THRIFTSERVER_SSL_KEYSTORE_PATH()).trim();
        if (keyStorePath.isEmpty()) {
          throw new IllegalArgumentException(ServiceConf.THRIFTSERVER_SSL_KEYSTORE_PATH().key()
              + " Not configured for SSL connection");
        }
        org.apache.hadoop.conf.Configuration hadoopConf =
            sqlContext.sparkContext().hadoopConfiguration();
        char[] pass = hadoopConf.getPassword(
            ServiceConf.THRIFTSERVER_SSL_KEYSTORE_PASSWORD().key()
                .substring("spark.hadoop.".length()));
        String keyStorePassword = new String(pass);
        serverSocket = SparkAuthUtils.getServerSSLSocket(sparkHost, portNum, keyStorePath,
            keyStorePassword, sslVersionBlacklist);
      }

      // Server args
      int maxMessageSize =
          (int) sqlConf.getConf(ServiceConf.THRIFTSERVER_MAX_MESSAGE_SIZE());
      int requestTimeout =
          new Long((long) sqlConf.getConf(ServiceConf.THRIFTSERVER_THRIFT_LOGIN_TIMEOUT()))
              .intValue();
      int beBackoffSlotLength =
          new Long(((long) sqlConf.getConf(
              ServiceConf.THRIFTSERVER_THRIFT_LOGIN_BEBACKOFF_SLOT_LENGTH()))).intValue();
      TThreadPoolServer.Args sargs = new TThreadPoolServer.Args(serverSocket)
          .processorFactory(processorFactory).transportFactory(transportFactory)
          .protocolFactory(new TBinaryProtocol.Factory())
          .inputProtocolFactory(new TBinaryProtocol.Factory(true, true,
              maxMessageSize, maxMessageSize))
          .requestTimeout(requestTimeout).requestTimeoutUnit(TimeUnit.SECONDS)
          .beBackoffSlotLength(beBackoffSlotLength).beBackoffSlotLengthUnit(TimeUnit.MILLISECONDS)
          .executorService(executorService);

      // TCP Server
      server = new TThreadPoolServer(sargs);
      server.setServerEventHandler(serverEventHandler);
      String msg = "Starting " + ThriftBinaryCLIService.class.getSimpleName() + " on port "
          + portNum + " with " + minWorkerThreads + "..." + maxWorkerThreads + " worker threads";
      LOG.info(msg);
      server.serve();
    } catch (Throwable t) {
      LOG.error(
          "Error starting SparkThriftServer: could not start "
              + ThriftBinaryCLIService.class.getSimpleName(), t);
      System.exit(-1);
    }
  }

}
