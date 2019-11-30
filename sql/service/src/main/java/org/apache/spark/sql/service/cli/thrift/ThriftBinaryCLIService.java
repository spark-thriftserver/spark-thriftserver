/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.spark.sql.service.auth.SparkAuthFactory;
import org.apache.spark.sql.service.auth.SparkAuthUtils;
import org.apache.spark.sql.service.cli.CLIService;
import org.apache.spark.sql.service.internal.ServiceConf;
import org.apache.spark.sql.service.server.ThreadFactoryWithGarbageCleanup;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportFactory;


public class ThriftBinaryCLIService extends ThriftCLIService {

  public ThriftBinaryCLIService(CLIService cliService) {
    super(cliService, ThriftBinaryCLIService.class.getSimpleName());
  }

  @Override
  public void run() {
    try {
      // Server thread pool
      String threadPoolName = "SparkServer2-Handler-Pool";
      ExecutorService executorService = new ThreadPoolExecutor(minWorkerThreads, maxWorkerThreads,
          workerKeepAliveTime, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(),
          new ThreadFactoryWithGarbageCleanup(threadPoolName));

      // Thrift configs
      sparkAuthFactory = new SparkAuthFactory(sqlConf, hiveConf);
      TTransportFactory transportFactory = sparkAuthFactory.getAuthTransFactory();
      TProcessorFactory processorFactory = sparkAuthFactory.getAuthProcFactory(this);
      TServerSocket serverSocket = null;
      List<String> sslVersionBlacklist = new ArrayList<String>();
      for (String sslVersion : sqlConf.getConfString(ServiceConf.THRIFTSERVER_SSL_PROTOCOL_BLACKLIST().key()).split(",")) {
        sslVersionBlacklist.add(sslVersion);
      }
      if (!Boolean.valueOf(sqlConf.getConfString(ServiceConf.THRIFTSERVER_USE_SSL().key()))) {
        serverSocket = SparkAuthUtils.getServerSocket(sparkHost, portNum);
      } else {
        String keyStorePath = sqlConf.getConfString(ServiceConf.THRIFTSERVER_SSL_KEYSTORE_PATH().key()).trim();
        if (keyStorePath.isEmpty()) {
          throw new IllegalArgumentException(ServiceConf.THRIFTSERVER_SSL_KEYSTORE_PATH().key()
              + " Not configured for SSL connection");
        }
        String keyStorePassword = ShimLoader.getHadoopShims().getPassword(hiveConf,
            HiveConf.ConfVars.HIVE_SERVER2_SSL_KEYSTORE_PASSWORD.varname);
        serverSocket = SparkAuthUtils.getServerSSLSocket(sparkHost, portNum, keyStorePath,
            keyStorePassword, sslVersionBlacklist);
      }

      // Server args
      int maxMessageSize = Integer.valueOf(sqlConf.getConfString(ServiceConf.THRIFTSERVER_MAX_MESSAGE_SIZE().key()));
      int requestTimeout = Integer.valueOf(sqlConf.getConfString(ServiceConf.THRIFTSERVER_THRIFT_LOGIN_TIMEOUT().key()));
      int beBackoffSlotLength = Integer.valueOf(sqlConf.getConfString(ServiceConf.THRIFTSERVER_THRIFT_LOGIN_BEBACKOFF_SLOT_LENGTH().key()));
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
          "Error starting SparkServer2: could not start "
              + ThriftBinaryCLIService.class.getSimpleName(), t);
      System.exit(-1);
    }
  }

}
