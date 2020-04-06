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

package org.apache.spark.sql.thriftserver.cli.thrift;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportFactory;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.thriftserver.auth.SparkAuthFactory;
import org.apache.spark.sql.thriftserver.auth.SparkAuthUtils;
import org.apache.spark.sql.thriftserver.cli.CLIService;
import org.apache.spark.sql.thriftserver.internal.ServiceConf;
import org.apache.spark.sql.thriftserver.server.ThreadFactoryWithName;

public class ThriftBinaryCLIService extends ThriftCLIService {

  public ThriftBinaryCLIService(CLIService cliService, SparkSession spark) {
    super(cliService, spark, ThriftBinaryCLIService.class.getSimpleName());
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
      sparkAuthFactory = new SparkAuthFactory(spark);
      TTransportFactory transportFactory = sparkAuthFactory.getAuthTransFactory();
      TProcessorFactory processorFactory = sparkAuthFactory.getAuthProcFactory(this);
      TServerSocket serverSocket = null;
      List<String> sslVersionBlacklist = new ArrayList<String>();
      Collections.addAll(sslVersionBlacklist, ServiceConf.sslProtocolBlacklist(conf).split(","));
      if (!ServiceConf.useSSL(conf)) {
        serverSocket = SparkAuthUtils.getServerSocket(sparkHost, portNum);
      } else {
        String keyStorePath = ServiceConf.sslKeystorePath(conf).trim();
        if (keyStorePath.isEmpty()) {
          throw new IllegalArgumentException(ServiceConf.THRIFTSERVER_SSL_KEYSTORE_PATH().key()
              + " Not configured for SSL connection");
        }
        Configuration hadoopConf = spark.sparkContext().hadoopConfiguration();
        char[] pass = hadoopConf.getPassword(
            ServiceConf.THRIFTSERVER_SSL_KEYSTORE_PASSWORD().key()
                .substring("spark.hadoop.".length()));
        String keyStorePassword = new String(pass);
        serverSocket = SparkAuthUtils.getServerSSLSocket(sparkHost, portNum, keyStorePath,
            keyStorePassword, sslVersionBlacklist);
      }

      // Server args
      int maxMessageSize = ServiceConf.maxMessageSize(conf);
      int requestTimeout = ServiceConf.thriftLoginTimeout(conf);
      int beBackoffSlotLength = ServiceConf.thriftLoginBackoffSlotLength(conf);
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
