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

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Shell;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.service.SparkSQLEnv;
import org.apache.spark.sql.service.auth.SparkAuthFactory;
import org.apache.spark.sql.service.auth.shims.ShimLoader;
import org.apache.spark.sql.service.cli.CLIService;
import org.apache.spark.sql.service.internal.ServiceConf;
import org.apache.spark.sql.service.rpc.thrift.TCLIService;
import org.apache.spark.sql.service.rpc.thrift.TCLIService.Iface;
import org.apache.spark.sql.service.server.ThreadFactoryWithGarbageCleanup;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.TServlet;
import org.eclipse.jetty.server.AbstractConnectionFactory;
import org.eclipse.jetty.server.ConnectionFactory;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.ExecutorThreadPool;
import org.eclipse.jetty.util.thread.ScheduledExecutorScheduler;

import java.util.Arrays;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


public class ThriftHttpCLIService extends ThriftCLIService {

  public ThriftHttpCLIService(CLIService cliService, SQLContext sqlContext) {
    super(cliService, sqlContext, ThriftHttpCLIService.class.getSimpleName());
  }

  /**
   * Configure Jetty to serve http requests. Example of a client connection URL:
   * http://localhost:10000/servlets/thrifths2/ A gateway may cause actual target URL to differ,
   * e.g. http://gateway:port/hive2/servlets/thrifths2/
   */
  @Override
  public void run() {
    try {
      // Server thread pool
      // Start with minWorkerThreads, expand till maxWorkerThreads and reject subsequent requests
      String threadPoolName = "SparkServer2-HttpHandler-Pool";
      ThreadPoolExecutor executorService =
          new ThreadPoolExecutor(minWorkerThreads, maxWorkerThreads,
              workerKeepAliveTime, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(),
          new ThreadFactoryWithGarbageCleanup(threadPoolName));
      ExecutorThreadPool threadPool = new ExecutorThreadPool(executorService);

      // HTTP Server
      httpServer = new org.eclipse.jetty.server.Server(threadPool);

      // Connector configs

      ConnectionFactory[] connectionFactories;
      boolean useSsl = Boolean.valueOf(sqlConf.getConfString(ServiceConf.THRIFTSERVER_USE_SSL().key()));
      String schemeName = useSsl ? "https" : "http";
      // Change connector if SSL is used
      if (useSsl) {
        String keyStorePath = sqlConf.getConfString(ServiceConf.THRIFTSERVER_SSL_KEYSTORE_PATH().key());
        org.apache.hadoop.conf.Configuration hadoopConf = sqlContext.sparkContext().hadoopConfiguration();
        String keyStorePassword = ShimLoader.getHadoopShims().getPassword(hadoopConf,
            ServiceConf.THRIFTSERVER_SSL_KEYSTORE_PASSWORD().key().substring("spark.hadoop.".length()));
        if (keyStorePath.isEmpty()) {
          throw new IllegalArgumentException(ServiceConf.THRIFTSERVER_SSL_KEYSTORE_PATH().key()
              + " Not configured for SSL connection");
        }
        SslContextFactory sslContextFactory = new SslContextFactory.Server();
        String[] excludedProtocols = sqlConf.getConfString(ServiceConf.THRIFTSERVER_SSL_PROTOCOL_BLACKLIST().key()).split(",");
        LOG.info("HTTP Server SSL: adding excluded protocols: " +
             Arrays.toString(excludedProtocols));
        sslContextFactory.addExcludeProtocols(excludedProtocols);
        LOG.info("HTTP Server SSL: SslContextFactory.getExcludeProtocols = " +
          Arrays.toString(sslContextFactory.getExcludeProtocols()));
        sslContextFactory.setKeyStorePath(keyStorePath);
        sslContextFactory.setKeyStorePassword(keyStorePassword);
        connectionFactories = AbstractConnectionFactory.getFactories(
            sslContextFactory, new HttpConnectionFactory());
      } else {
        connectionFactories = new ConnectionFactory[] { new HttpConnectionFactory() };
      }
      ServerConnector connector = new ServerConnector(
          httpServer,
          null,
          // Call this full constructor to set this, which forces daemon threads:
          new ScheduledExecutorScheduler("SparkServer2-HttpHandler-JettyScheduler", true),
          null,
          -1,
          -1,
          connectionFactories);

      connector.setPort(portNum);
      // Linux:yes, Windows:no
      connector.setReuseAddress(!Shell.WINDOWS);
      int maxIdleTime = Integer.valueOf(sqlConf.getConfString(ServiceConf.THRIFTSERVER_THRIFT_HTTP_MAX_IDLE_TIME().key()));
      connector.setIdleTimeout(maxIdleTime);

      httpServer.addConnector(connector);

      // Thrift configs
      sparkAuthFactory = new SparkAuthFactory(sqlContext);
      TProcessor processor = new TCLIService.Processor<Iface>(this);
      TProtocolFactory protocolFactory = new TBinaryProtocol.Factory();
      // Set during the init phase of SparkServer2 if auth mode is kerberos
      // UGI for the hive/_HOST (kerberos) principal
      UserGroupInformation serviceUGI = cliService.getServiceUGI();
      // UGI for the http/_HOST (SPNego) principal
      UserGroupInformation httpUGI = cliService.getHttpUGI();
      String authType = sqlConf.getConfString(ServiceConf.THRIFTSERVER_AUTHENTICATION().key());
      TServlet thriftHttpServlet = new ThriftHttpServlet(processor, protocolFactory, authType,
          serviceUGI, httpUGI, sparkAuthFactory, sqlConf);

      // Context handler
      final ServletContextHandler context = new ServletContextHandler(
          ServletContextHandler.SESSIONS);
      context.setContextPath("/");
      String httpPath = getHttpPath(sqlConf.getConfString(ServiceConf.THRIFTSERVER_HTTP_PATH().key()));
      httpServer.setHandler(context);
      context.addServlet(new ServletHolder(thriftHttpServlet), httpPath);

      // TODO: check defaults: maxTimeout, keepalive, maxBodySize, bodyRecieveDuration, etc.
      // Finally, start the server
      httpServer.start();
      String msg = "Started " + ThriftHttpCLIService.class.getSimpleName() + " in " + schemeName
          + " mode on port " + portNum + " path=" + httpPath + " with " + minWorkerThreads + "..."
          + maxWorkerThreads + " worker threads";
      LOG.info(msg);
      httpServer.join();
    } catch (Throwable t) {
      LOG.error(
          "Error starting SparkServer2: could not start "
              + ThriftHttpCLIService.class.getSimpleName(), t);
      System.exit(-1);
    }
  }

  /**
   * The config parameter can be like "path", "/path", "/path/", "path/*", "/path1/path2/*"
   * and so on.
   * httpPath should end up as "/*", "/path/*" or "/path1/../pathN/*"
   * @param httpPath
   * @return
   */
  private String getHttpPath(String httpPath) {
    if(httpPath == null || httpPath.equals("")) {
      httpPath = "/*";
    }
    else {
      if(!httpPath.startsWith("/")) {
        httpPath = "/" + httpPath;
      }
      if(httpPath.endsWith("/")) {
        httpPath = httpPath + "*";
      }
      if(!httpPath.endsWith("/*")) {
        httpPath = httpPath + "/*";
      }
    }
    return httpPath;
  }
}
