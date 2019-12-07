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

package org.apache.spark.sql.service.cli.session

import java.io.{File, IOException}
import java.util
import java.util.Date
import java.util.concurrent._

import scala.collection.JavaConverters._

import org.apache.commons.io.FileUtils

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.service.{CompositeService, SparkThriftServer2}
import org.apache.spark.sql.service.cli.{ServiceSQLException, SessionHandle}
import org.apache.spark.sql.service.cli.operation.OperationManager
import org.apache.spark.sql.service.internal.ServiceConf
import org.apache.spark.sql.service.rpc.thrift.TProtocolVersion
import org.apache.spark.sql.service.server.{SparkServer2, ThreadFactoryWithName}

class SessionManager(sparkServer2: SparkServer2, sqlContext: SQLContext)
  extends CompositeService(classOf[SessionManager].getSimpleName)
    with Logging {

  private var sqlConf: SQLConf = null
  private val handleToSession = new ConcurrentHashMap[SessionHandle, ServiceSession]
  private val operationManager = new OperationManager
  private var backgroundOperationPool: ThreadPoolExecutor = null
  private var isOperationLogEnabled: Boolean = false
  private var operationLogRootDir: File = null

  private var checkInterval: Long = 0L
  private var sessionTimeout: Long = 0L
  private var checkOperation: Boolean = false

  private var shutdown: Boolean = false
  // The SparkServer2 instance running this service

  override def init(sqlConf: SQLConf): Unit = {
    this.sqlConf = sqlConf
    // Create operation log root directory, if operation logging is enabled
    if (sqlConf.getConf(ServiceConf.THRIFTSERVER_LOGGING_OPERATION_ENABLE).asInstanceOf[Boolean]) {
      initOperationLogRootDir()
    }
    createBackgroundOperationPool()
    addService(operationManager)
    super.init(sqlConf)
  }

  private def createBackgroundOperationPool(): Unit = {
    val poolSize = sqlConf.getConf(ServiceConf.THRIFTSERVER_ASYNC_EXEC_THREADS)
    logInfo("SparkServer2: Background operation thread pool size: " + poolSize)
    val poolQueueSize: Int = sqlConf.getConf(ServiceConf.THRIFTSERVER_ASYNC_EXEC_WAIT_QUEUE_SIZE)
    logInfo("SparkServer2: Background operation thread wait queue size: " + poolQueueSize)
    val keepAliveTime: Long = sqlConf.getConf(ServiceConf.THRIFTSERVER_ASYNC_EXEC_KEEPALIVE_TIME)
    logInfo("SparkServer2: Background operation thread keepalive time: " +
      keepAliveTime + " seconds")
    // Create a thread pool with #poolSize threads
    // Threads terminate when they are idle for more than the keepAliveTime
    // A bounded blocking queue is used to queue incoming operations, if #operations > poolSize
    val threadPoolName = "SparkServer2-Background-Pool"
    backgroundOperationPool = new ThreadPoolExecutor(poolSize, poolSize,
      keepAliveTime, TimeUnit.SECONDS,
      new LinkedBlockingQueue[Runnable](poolQueueSize),
      new ThreadFactoryWithName(threadPoolName))
    backgroundOperationPool.allowCoreThreadTimeOut(true)
    checkInterval = sqlConf.getConf(ServiceConf.THRIFTSERVER_SESSION_CHECK_INTERVAL)
    sessionTimeout = sqlConf.getConf(ServiceConf.THRIFTSERVER_IDLE_SESSION_TIMEOUT)
    checkOperation = sqlConf.getConf(ServiceConf.THRIFTSERVER_IDLE_SESSION_CHECK_OPERATION)
  }

  private def initOperationLogRootDir(): Unit = {
    operationLogRootDir =
      new File(sqlConf.getConf(ServiceConf.THRIFTSERVER_LOGGING_OPERATION_LOG_LOCATION))
    isOperationLogEnabled = true
    if (operationLogRootDir.exists && !operationLogRootDir.isDirectory) {
      logWarning("The operation log root directory exists, but it is not a directory: " +
        operationLogRootDir.getAbsolutePath)
      isOperationLogEnabled = false
    }
    if (!operationLogRootDir.exists) {
      if (!operationLogRootDir.mkdirs) {
        logWarning("Unable to create operation log root directory: " +
          operationLogRootDir.getAbsolutePath)
        isOperationLogEnabled = false
      }
    }
    if (isOperationLogEnabled) {
      logInfo("Operation log root directory is created: " + operationLogRootDir.getAbsolutePath)
      try
        FileUtils.forceDeleteOnExit(operationLogRootDir)
      catch {
        case e: IOException =>
          logWarning("Failed to schedule cleanup HS2 operation logging root dir: " +
            operationLogRootDir.getAbsolutePath, e)
      }
    }
  }

  override def start(): Unit = {
    super.start()
    if (checkInterval > 0) {
      startTimeoutChecker()
    }
  }

  private def startTimeoutChecker(): Unit = {
    val interval = Math.max(checkInterval, 3000L)
    // minimum 3 seconds
    val timeoutChecker = new Runnable() {
      override def run(): Unit = {
        sleepInterval(interval)
        while ( {
          !shutdown
        }) {
          val current = System.currentTimeMillis
          handleToSession.values().asScala.foreach(session => {
            if (sessionTimeout > 0 && session.getLastAccessTime + sessionTimeout <= current &&
              (!checkOperation || session.getNoOperationTime > sessionTimeout)) {
              val handle = session.getSessionHandle
              logWarning("Session " + handle + " is Timed-out (last access : " +
                new Date(session.getLastAccessTime) + ") and will be closed")
              try {
                closeSession(handle)
              } catch {
                case e: ServiceSQLException =>
                  logWarning("Exception is thrown closing session " + handle, e)
              }
            } else {
              session.closeExpiredOperations()
            }
          })
          sleepInterval(interval)
        }
      }

      private def sleepInterval(interval: Long): Unit = {
        try
          Thread.sleep(interval)
        catch {
          case e: InterruptedException =>

          // ignore
        }
      }
    }
    backgroundOperationPool.execute(timeoutChecker)
  }

  override def stop(): Unit = {
    super.stop()
    shutdown = true
    if (backgroundOperationPool != null) {
      backgroundOperationPool.shutdown()
      val timeout: Long = sqlConf.getConf(ServiceConf.THRIFTSERVER_ASYNC_EXEC_SHUTDOWN_TIMEOUT)
      try
        backgroundOperationPool.awaitTermination(timeout, TimeUnit.SECONDS)
      catch {
        case e: InterruptedException =>
          logWarning("THRIFTSERVER_ASYNC_EXEC_SHUTDOWN_TIMEOUT = " + timeout +
            " seconds has been exceeded. RUNNING background operations will be shut down", e)
      }
      backgroundOperationPool = null
    }
    cleanupLoggingRootDir()
  }

  private def cleanupLoggingRootDir(): Unit = {
    if (isOperationLogEnabled) {
      try {
        FileUtils.forceDelete(operationLogRootDir)
      } catch {
        case e: Exception =>
          logWarning("Failed to cleanup root dir of HS2 logging: " +
            operationLogRootDir.getAbsolutePath, e)
      }
    }
  }

  @throws[ServiceSQLException]
  def openSession(protocol: TProtocolVersion, username: String,
                  password: String, ipAddress: String,
                  sessionConf: util.Map[String, String]): SessionHandle = {
    openSession(protocol, username, password, ipAddress,
      sessionConf, false, null)
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
  @throws[ServiceSQLException]
  def openSession(protocol: TProtocolVersion, username: String, password: String,
                  ipAddress: String, sessionConf: util.Map[String, String],
                  withImpersonation: Boolean, delegationToken: String): SessionHandle = {
    val ctx = if (sqlContext.conf.hiveThriftServerSingleSession) {
      sqlContext
    } else {
      sqlContext.newSession()
    }
    if (sessionConf != null && sessionConf.containsKey("use:database")) {
      ctx.sql(s"use ${sessionConf.get("use:database")}")
    }

    var session: ServiceSession = null
    // If doAs is set to true for SparkServer2, we will create a proxy object for the session impl.
    // Within the proxy object, we wrap the method call in a UserGroupInformation#doAs
    if (withImpersonation) {
      val sessionWithUGI = new ServiceSessionImplwithUGI(protocol, username, password,
        ctx, ipAddress, delegationToken)
      session = ServiceSessionProxy.getProxy(sessionWithUGI, sessionWithUGI.getSessionUgi)
      sessionWithUGI.setProxySession(session)
    } else session = new ServiceSessionImpl(protocol, username, password, ctx, ipAddress)
    session.setSessionManager(this)
    session.setOperationManager(operationManager)
    try {
      session.open(sessionConf)
    } catch {
      case e: Exception =>
        try {
          session.close()
        } catch {
          case t: Throwable =>
            logWarning("Error closing session", t)
        }
        session = null
        throw new ServiceSQLException("Failed to open new session: " + e, e)
    }
    if (isOperationLogEnabled) {
      session.setOperationLogSessionDir(operationLogRootDir)
    }
    handleToSession.put(session.getSessionHandle, session)
    SparkThriftServer2.listener.onSessionCreated(
      session.getIpAddress, session.getSessionHandle.getSessionId.toString, session.getUsername)
    session.getSessionHandle
  }

  @throws[ServiceSQLException]
  def closeSession(sessionHandle: SessionHandle): Unit = {
    SparkThriftServer2.listener.onSessionClosed(sessionHandle.getSessionId.toString)
    val session = handleToSession.remove(sessionHandle)
    if (session == null) {
      throw new ServiceSQLException("Session does not exist!")
    }
    session.close()
  }

  @throws[ServiceSQLException]
  def getSession(sessionHandle: SessionHandle): ServiceSession = {
    val session = handleToSession.get(sessionHandle)
    if (session == null) throw new ServiceSQLException("Invalid SessionHandle: " + sessionHandle)
    session
  }

  def getOperationManager: OperationManager = operationManager

  def submitBackgroundOperation(r: Runnable): Future[_] = backgroundOperationPool.submit(r)

  def getOpenSessionCount: Int = handleToSession.size

}

object SessionManager extends Logging {
  val SPARKRCFILE: String = ".sparkrc"

  private val threadLocalIpAddress = new ThreadLocal[String]() {
    override protected def initialValue: String = null
  }

  def setIpAddress(ipAddress: String): Unit = {
    threadLocalIpAddress.set(ipAddress)
  }

  def clearIpAddress(): Unit = {
    threadLocalIpAddress.remove()
  }

  def getIpAddress: String = threadLocalIpAddress.get

  private val threadLocalUserName = new ThreadLocal[String]() {
    override protected def initialValue: String = null
  }

  def setUserName(userName: String): Unit = {
    threadLocalUserName.set(userName)
  }

  def clearUserName(): Unit = {
    threadLocalUserName.remove()
  }

  def getUserName: String = threadLocalUserName.get

  private val threadLocalProxyUserName = new ThreadLocal[String]() {
    override protected def initialValue: String = null
  }

  def setProxyUserName(userName: String): Unit = {
    logDebug("setting proxy user name based on query param to: " + userName)
    threadLocalProxyUserName.set(userName)
  }

  def getProxyUserName: String = threadLocalProxyUserName.get

  def clearProxyUserName(): Unit = {
    threadLocalProxyUserName.remove()
  }

}
