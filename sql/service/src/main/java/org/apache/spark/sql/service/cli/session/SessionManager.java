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

package org.apache.spark.sql.service.cli.session;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.service.CompositeService;
import org.apache.spark.sql.service.SparkThriftServer;
import org.apache.spark.sql.service.cli.ServiceSQLException;
import org.apache.spark.sql.service.cli.SessionHandle;
import org.apache.spark.sql.service.cli.operation.OperationManager;
import org.apache.spark.sql.service.internal.ServiceConf;
import org.apache.spark.sql.service.rpc.thrift.TProtocolVersion;
import org.apache.spark.sql.service.server.ThreadFactoryWithName;

/**
 * SessionManager.
 *
 */
public class SessionManager extends CompositeService {

  private static final Logger LOG = LoggerFactory.getLogger(SessionManager.class);
  public static final String SPARKRCFILE = ".sparkrc";
  private SQLConf sqlConf;
  private SQLContext sqlContext;
  private final Map<SessionHandle, ServiceSession> handleToSession =
      new ConcurrentHashMap<SessionHandle, ServiceSession>();
  private final OperationManager operationManager = new OperationManager();
  private ThreadPoolExecutor backgroundOperationPool;
  private boolean isOperationLogEnabled;
  private File operationLogRootDir;

  private long checkInterval;
  private long sessionTimeout;
  private boolean checkOperation;

  private volatile boolean shutdown;

  public SessionManager(SQLContext sqlContext) {
    super(SessionManager.class.getSimpleName());
    this.sqlContext = sqlContext;
  }

  @Override
  public synchronized void init(SQLConf sqlConf) {
    this.sqlConf = sqlConf;
    //Create operation log root directory, if operation logging is enabled
    if (((boolean) sqlConf.getConf(ServiceConf.THRIFTSERVER_LOGGING_OPERATION_ENABLE()))) {
      initOperationLogRootDir();
    }
    createBackgroundOperationPool();
    addService(operationManager);
    super.init(sqlConf);
  }

  private void createBackgroundOperationPool() {
    int poolSize = (int) sqlConf.getConf(ServiceConf.THRIFTSERVER_ASYNC_EXEC_THREADS());
    LOG.info("SparkThriftServer: Background operation thread pool size: " + poolSize);
    int poolQueueSize =
        (int) sqlConf.getConf(ServiceConf.THRIFTSERVER_ASYNC_EXEC_WAIT_QUEUE_SIZE());
    LOG.info("SparkThriftServer: Background operation thread wait queue size: " + poolQueueSize);
    long keepAliveTime =
       (long) sqlConf.getConf(ServiceConf.THRIFTSERVER_ASYNC_EXEC_KEEPALIVE_TIME());
    LOG.info("SparkThriftServer: Background operation thread keepalive time: "
        + keepAliveTime + " seconds");

    // Create a thread pool with #poolSize threads
    // Threads terminate when they are idle for more than the keepAliveTime
    // A bounded blocking queue is used to queue incoming operations, if #operations > poolSize
    String threadPoolName = "SparkThriftServer-Background-Pool";
    backgroundOperationPool = new ThreadPoolExecutor(poolSize, poolSize,
        keepAliveTime, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(poolQueueSize),
        new ThreadFactoryWithName(threadPoolName));
    backgroundOperationPool.allowCoreThreadTimeOut(true);

    checkInterval = (long) sqlConf.getConf(ServiceConf.THRIFTSERVER_SESSION_CHECK_INTERVAL());
    sessionTimeout = (long) sqlConf.getConf(ServiceConf.THRIFTSERVER_IDLE_SESSION_TIMEOUT());
    checkOperation =
        (boolean) sqlConf.getConf(ServiceConf.THRIFTSERVER_IDLE_SESSION_CHECK_OPERATION());
  }

  private void initOperationLogRootDir() {
    operationLogRootDir = new File(
        sqlConf.getConf(ServiceConf.THRIFTSERVER_LOGGING_OPERATION_LOG_LOCATION()));
    isOperationLogEnabled = true;

    if (operationLogRootDir.exists() && !operationLogRootDir.isDirectory()) {
      LOG.warn("The operation log root directory exists, but it is not a directory: " +
          operationLogRootDir.getAbsolutePath());
      isOperationLogEnabled = false;
    }

    if (!operationLogRootDir.exists()) {
      if (!operationLogRootDir.mkdirs()) {
        LOG.warn("Unable to create operation log root directory: " +
            operationLogRootDir.getAbsolutePath());
        isOperationLogEnabled = false;
      }
    }

    if (isOperationLogEnabled) {
      LOG.info("Operation log root directory is created: " + operationLogRootDir.getAbsolutePath());
      try {
        FileUtils.forceDeleteOnExit(operationLogRootDir);
      } catch (IOException e) {
        LOG.warn("Failed to schedule cleanup HS2 operation logging root dir: " +
            operationLogRootDir.getAbsolutePath(), e);
      }
    }
  }

  @Override
  public synchronized void start() {
    super.start();
    if (checkInterval > 0) {
      startTimeoutChecker();
    }
  }

  private void startTimeoutChecker() {
    final long interval = Math.max(checkInterval, 3000L);  // minimum 3 seconds
    Runnable timeoutChecker = new Runnable() {
      @Override
      public void run() {
        for (sleepInterval(interval); !shutdown; sleepInterval(interval)) {
          long current = System.currentTimeMillis();
          for (ServiceSession session : new ArrayList<ServiceSession>(handleToSession.values())) {
            if (sessionTimeout > 0 && session.getLastAccessTime() + sessionTimeout <= current
                && (!checkOperation || session.getNoOperationTime() > sessionTimeout)) {
              SessionHandle handle = session.getSessionHandle();
              LOG.warn("Session " + handle + " is Timed-out (last access : " +
                  new Date(session.getLastAccessTime()) + ") and will be closed");
              try {
                closeSession(handle);
              } catch (ServiceSQLException e) {
                LOG.warn("Exception is thrown closing session " + handle, e);
              }
            } else {
              session.closeExpiredOperations();
            }
          }
        }
      }

      private void sleepInterval(long interval) {
        try {
          Thread.sleep(interval);
        } catch (InterruptedException e) {
          // ignore
        }
      }
    };
    backgroundOperationPool.execute(timeoutChecker);
  }

  @Override
  public synchronized void stop() {
    super.stop();
    shutdown = true;
    if (backgroundOperationPool != null) {
      backgroundOperationPool.shutdown();
      long timeout = (long) sqlConf.getConf(ServiceConf.THRIFTSERVER_ASYNC_EXEC_SHUTDOWN_TIMEOUT());
      try {
        backgroundOperationPool.awaitTermination(timeout, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        LOG.warn("THRIFTSERVER_ASYNC_EXEC_SHUTDOWN_TIMEOUT = " + timeout +
            " seconds has been exceeded. RUNNING background operations will be shut down", e);
      }
      backgroundOperationPool = null;
    }
    cleanupLoggingRootDir();
  }

  private void cleanupLoggingRootDir() {
    if (isOperationLogEnabled) {
      try {
        FileUtils.forceDelete(operationLogRootDir);
      } catch (Exception e) {
        LOG.warn("Failed to cleanup root dir of HS2 logging: " + operationLogRootDir
            .getAbsolutePath(), e);
      }
    }
  }

  public SessionHandle openSession(TProtocolVersion protocol, String username, String password,
      String ipAddress, Map<String, String> sessionConf) throws ServiceSQLException {
    return openSession(protocol, username, password, ipAddress, sessionConf, false, null);
  }

  /**
   * Opens a new session and creates a session handle.
   * The username passed to this method is the effective username.
   * If withImpersonation is true (==doAs true) we wrap all the calls in ServiceSession
   * within a UGI.doAs, where UGI corresponds to the effective user.
   *
   * Please see {@code ThriftCLIService.getUserName()} for
   * more details.
   *
   * @param protocol
   * @param username
   * @param password
   * @param ipAddress
   * @param sessionConf
   * @param withImpersonation
   * @param delegationToken
   * @return
   * @throws ServiceSQLException
   */
  public SessionHandle openSession(TProtocolVersion protocol, String username, String password,
      String ipAddress, Map<String, String> sessionConf, boolean withImpersonation,
      String delegationToken) throws ServiceSQLException {
    ServiceSession session;
    SQLContext ctx = null;
    if(sqlContext.conf().hiveThriftServerSingleSession()) {
      ctx = sqlContext;
    } else {
      ctx = sqlContext.newSession();
    }
    // If doAs is set to true for SparkThriftServer,
    // we will create a proxy object for the session impl.
    // Within the proxy object, we wrap the method call in a UserGroupInformation#doAs
    if (withImpersonation) {
      ServiceSessionImplwithUGI sessionWithUGI = new ServiceSessionImplwithUGI(protocol, username,
          password, ctx, ipAddress, delegationToken);
      session = ServiceSessionProxy.getProxy(sessionWithUGI, sessionWithUGI.getSessionUgi());
      sessionWithUGI.setProxySession(session);
    } else {
      session = new ServiceSessionImpl(protocol, username, password, ctx, ipAddress);
    }
    session.setSessionManager(this);
    session.setOperationManager(operationManager);
    try {
      session.open(sessionConf);
    } catch (Exception e) {
      try {
        session.close();
      } catch (Throwable t) {
        LOG.warn("Error closing session", t);
      }
      session = null;
      throw new ServiceSQLException("Failed to open new session: " + e, e);
    }
    if (isOperationLogEnabled) {
      session.setOperationLogSessionDir(operationLogRootDir);
    }
    SparkThriftServer.listener().onSessionCreated(session.getIpAddress(),
        session.getSessionHandle().getSessionId().toString(),
        session.getUserName());
    if (sessionConf != null && sessionConf.containsKey("use:database")) {
      session.getSQLContext().sql("use " + sessionConf.get("use:database"));
    }
    handleToSession.put(session.getSessionHandle(), session);
    return session.getSessionHandle();
  }

  public void closeSession(SessionHandle sessionHandle) throws ServiceSQLException {
    SparkThriftServer.listener().onSessionClosed(sessionHandle.getSessionId().toString());
    ServiceSession session = handleToSession.remove(sessionHandle);
    if (session == null) {
      throw new ServiceSQLException("Session does not exist!");
    }
    session.close();
    operationManager.sessionToActivePool().remove(sessionHandle);
  }

  public ServiceSession getSession(SessionHandle sessionHandle) throws ServiceSQLException {
    ServiceSession session = handleToSession.get(sessionHandle);
    if (session == null) {
      throw new ServiceSQLException("Invalid SessionHandle: " + sessionHandle);
    }
    return session;
  }

  public OperationManager getOperationManager() {
    return operationManager;
  }

  private static ThreadLocal<String> threadLocalIpAddress = new ThreadLocal<String>() {
    @Override
    protected synchronized String initialValue() {
      return null;
    }
  };

  public static void setIpAddress(String ipAddress) {
    threadLocalIpAddress.set(ipAddress);
  }

  public static void clearIpAddress() {
    threadLocalIpAddress.remove();
  }

  public static String getIpAddress() {
    return threadLocalIpAddress.get();
  }

  private static ThreadLocal<String> threadLocalUserName = new ThreadLocal<String>(){
    @Override
    protected synchronized String initialValue() {
      return null;
    }
  };

  public static void setUserName(String userName) {
    threadLocalUserName.set(userName);
  }

  public static void clearUserName() {
    threadLocalUserName.remove();
  }

  public static String getUserName() {
    return threadLocalUserName.get();
  }

  private static ThreadLocal<String> threadLocalProxyUserName = new ThreadLocal<String>(){
    @Override
    protected synchronized String initialValue() {
      return null;
    }
  };

  public static void setProxyUserName(String userName) {
    LOG.debug("setting proxy user name based on query param to: " + userName);
    threadLocalProxyUserName.set(userName);
  }

  public static String getProxyUserName() {
    return threadLocalProxyUserName.get();
  }

  public static void clearProxyUserName() {
    threadLocalProxyUserName.remove();
  }

  public Future<?> submitBackgroundOperation(Runnable r) {
    return backgroundOperationPool.submit(r);
  }

  public int getOpenSessionCount() {
    return handleToSession.size();
  }
}

