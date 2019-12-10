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

package org.apache.spark.sql.service

import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkContext
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.UI.UI_ENABLED
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd, SparkListenerJobStart}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.service.cli.CLIService
import org.apache.spark.sql.service.cli.thrift.{ThriftBinaryCLIService, ThriftCLIService, ThriftHttpCLIService}
import org.apache.spark.sql.service.internal.ServiceConf
import org.apache.spark.sql.service.server.ServerStartUpUtil
import org.apache.spark.sql.service.ui.ThriftServerTab
import org.apache.spark.util.{ShutdownHookManager, Utils}

/**
 * The main entry point for the Spark SQL port of SparkThriftServer.
 * Starts up a `SparkSQLContext` and a `SparkThriftServer` thrift server.
 */
object SparkThriftServer extends Logging {
  var uiTab: Option[ThriftServerTab] = None
  var listener: SparkThriftServerListener = _

  /**
   * :: DeveloperApi ::
   * Starts a new thrift server with the given context.
   */
  @DeveloperApi
  def startWithContext(sqlContext: SQLContext): SparkThriftServer = {
    SparkSQLEnv.setSQLContext(sqlContext)
    val server = new SparkThriftServer(sqlContext)

    server.init(sqlContext.conf)
    server.start()
    listener = new SparkThriftServerListener(server, sqlContext.conf)
    sqlContext.sparkContext.addSparkListener(listener)
    uiTab = if (sqlContext.sparkContext.getConf.get(UI_ENABLED)) {
      Some(new ThriftServerTab(sqlContext.sparkContext))
    } else {
      None
    }
    server
  }

  def isHTTPTransportMode(sqlConf: SQLConf): Boolean = {
    var transportMode = System.getenv("THRIFTSERVER_TRANSPORT_MODE")
    if (transportMode == null) {
      transportMode = sqlConf.getConf(ServiceConf.THRIFTSERVER_TRANSPORT_MODE)
    }
    if (transportMode != null && transportMode.equalsIgnoreCase("http")) {
      true
    } else {
      false
    }
  }

  def main(args: Array[String]): Unit = {
    // If the arguments contains "-h" or "--help", print out the usage and exit.
    if (args.contains("-h") || args.contains("--help")) {
      ServerStartUpUtil.process(args)
      // The following code should not be reachable. It is added to ensure the main function exits.
      return
    }

    Utils.initDaemon(log)
    val optionsProcessor = new ServerStartUpUtil.ServerOptionsProcessor("SparkThriftServer")
    optionsProcessor.parse(args)

    logInfo("Starting SparkContext")
    SparkSQLEnv.init()


    ShutdownHookManager.addShutdownHook { () =>
      SparkSQLEnv.stop()
      uiTab.foreach(_.detach())
    }

    try {
      val server = new SparkThriftServer(SparkSQLEnv.sqlContext)
      server.init(SparkSQLEnv.sqlContext.conf)
      server.start()
      logInfo("SparkThriftServer started")
      listener = new SparkThriftServerListener(server, SparkSQLEnv.sqlContext.conf)
      SparkSQLEnv.sparkContext.addSparkListener(listener)
      uiTab = if (SparkSQLEnv.sparkContext.getConf.get(UI_ENABLED)) {
        Some(new ThriftServerTab(SparkSQLEnv.sparkContext))
      } else {
        None
      }
      // If application was killed before SparkThriftServer start successfully then SparkSubmit
      // process can not exit, so check whether if SparkContext was stopped.
      if (SparkSQLEnv.sparkContext.stopped.get()) {
        logError("SparkContext has stopped even if SparkThriftServer has started, so exit")
        System.exit(-1)
      }
    } catch {
      case e: Exception =>
        logError("Error starting SparkThriftServer", e)
        System.exit(-1)
    }
  }

  private[service] class SessionInfo(
      val sessionId: String,
      val startTimestamp: Long,
      val ip: String,
      val userName: String) {
    var finishTimestamp: Long = 0L
    var totalExecution: Int = 0
    def totalTime: Long = {
      if (finishTimestamp == 0L) {
        System.currentTimeMillis - startTimestamp
      } else {
        finishTimestamp - startTimestamp
      }
    }
  }

  private[service] object ExecutionState extends Enumeration {
    val STARTED, COMPILED, CANCELED, FAILED, FINISHED, CLOSED = Value
    type ExecutionState = Value
  }

  private[service] class ExecutionInfo(
      val statement: String,
      val sessionId: String,
      val startTimestamp: Long,
      val userName: String) {
    var finishTimestamp: Long = 0L
    var closeTimestamp: Long = 0L
    var executePlan: String = ""
    var detail: String = ""
    var state: ExecutionState.Value = ExecutionState.STARTED
    val jobId: ArrayBuffer[String] = ArrayBuffer[String]()
    var groupId: String = ""
    def totalTime(endTime: Long): Long = {
      if (endTime == 0L) {
        System.currentTimeMillis - startTimestamp
      } else {
        endTime - startTimestamp
      }
    }
  }


  /**
   * An inner sparkListener called in sc.stop to clean up the SparkThriftServer
   */
  class SparkThriftServerListener(
      val server: SparkThriftServer,
      val conf: SQLConf) extends SparkListener {

    override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
      server.stop()
    }
    private val sessionList = new mutable.LinkedHashMap[String, SessionInfo]
    private val executionList = new mutable.LinkedHashMap[String, ExecutionInfo]
    private val retainedStatements = conf.getConf(SQLConf.THRIFTSERVER_UI_STATEMENT_LIMIT)
    private val retainedSessions = conf.getConf(SQLConf.THRIFTSERVER_UI_SESSION_LIMIT)

    def getOnlineSessionNum: Int = synchronized {
      sessionList.count(_._2.finishTimestamp == 0)
    }

    def isExecutionActive(execInfo: ExecutionInfo): Boolean = {
      !(execInfo.state == ExecutionState.FAILED ||
        execInfo.state == ExecutionState.CANCELED ||
        execInfo.state == ExecutionState.CLOSED)
    }

    /**
     * When an error or a cancellation occurs, we set the finishTimestamp of the statement.
     * Therefore, when we count the number of running statements, we need to exclude errors and
     * cancellations and count all statements that have not been closed so far.
     */
    def getTotalRunning: Int = synchronized {
      executionList.count {
        case (_, v) => isExecutionActive(v)
      }
    }

    def getSessionList: Seq[SessionInfo] = synchronized { sessionList.values.toSeq }

    def getSession(sessionId: String): Option[SessionInfo] = synchronized {
      sessionList.get(sessionId)
    }

    def getExecutionList: Seq[ExecutionInfo] = synchronized { executionList.values.toSeq }

    override def onJobStart(jobStart: SparkListenerJobStart): Unit = synchronized {
      for {
        props <- Option(jobStart.properties)
        groupId <- Option(props.getProperty(SparkContext.SPARK_JOB_GROUP_ID))
        (_, info) <- executionList if info.groupId == groupId
      } {
        info.jobId += jobStart.jobId.toString
        info.groupId = groupId
      }
    }

    def onSessionCreated(ip: String, sessionId: String, userName: String = "UNKNOWN"): Unit = {
      synchronized {
        val info = new SessionInfo(sessionId, System.currentTimeMillis, ip, userName)
        sessionList.put(sessionId, info)
        trimSessionIfNecessary()
      }
    }

    def onSessionClosed(sessionId: String): Unit = synchronized {
      sessionList(sessionId).finishTimestamp = System.currentTimeMillis
      trimSessionIfNecessary()
    }

    def onStatementStart(
        id: String,
        sessionId: String,
        statement: String,
        groupId: String,
        userName: String = "UNKNOWN"): Unit = synchronized {
      val info = new ExecutionInfo(statement, sessionId, System.currentTimeMillis, userName)
      info.state = ExecutionState.STARTED
      executionList.put(id, info)
      trimExecutionIfNecessary()
      sessionList(sessionId).totalExecution += 1
      executionList(id).groupId = groupId
    }

    def onStatementParsed(id: String, executionPlan: String): Unit = synchronized {
      executionList(id).executePlan = executionPlan
      executionList(id).state = ExecutionState.COMPILED
    }

    def onStatementCanceled(id: String): Unit = synchronized {
      executionList(id).finishTimestamp = System.currentTimeMillis
      executionList(id).state = ExecutionState.CANCELED
      trimExecutionIfNecessary()
    }

    def onStatementError(id: String, errorMsg: String, errorTrace: String): Unit = synchronized {
      executionList(id).finishTimestamp = System.currentTimeMillis
      executionList(id).detail = errorMsg
      executionList(id).state = ExecutionState.FAILED
      trimExecutionIfNecessary()
    }

    def onStatementFinish(id: String): Unit = synchronized {
      executionList(id).finishTimestamp = System.currentTimeMillis
      executionList(id).state = ExecutionState.FINISHED
      trimExecutionIfNecessary()
    }

    def onOperationClosed(id: String): Unit = synchronized {
      executionList(id).closeTimestamp = System.currentTimeMillis
      executionList(id).state = ExecutionState.CLOSED
    }

    private def trimExecutionIfNecessary() = {
      if (executionList.size > retainedStatements) {
        val toRemove = math.max(retainedStatements / 10, 1)
        executionList.filter(_._2.finishTimestamp != 0).take(toRemove).foreach { s =>
          executionList.remove(s._1)
        }
      }
    }

    private def trimSessionIfNecessary() = {
      if (sessionList.size > retainedSessions) {
        val toRemove = math.max(retainedSessions / 10, 1)
        sessionList.filter(_._2.finishTimestamp != 0).take(toRemove).foreach { s =>
          sessionList.remove(s._1)
        }
      }

    }
  }
}

private[spark] class SparkThriftServer(sqlContext: SQLContext)
  extends CompositeService(classOf[SparkThriftServer].getCanonicalName) with Logging {

  import SparkThriftServer._

  // state is tracked internally so that the server only attempts to shut down if it successfully
  // started, and then once only.
  private val started = new AtomicBoolean(false)

  private var cliService: CLIService = _
  private var thriftCLIService: ThriftCLIService = _

  override def init(sqlConf: SQLConf): Unit = {
    cliService = new CLIService(this, sqlContext)
    addService(cliService)
    if (isHTTPTransportMode(sqlConf)) {
      thriftCLIService = new ThriftHttpCLIService(cliService, sqlContext)
    } else {
      thriftCLIService = new ThriftBinaryCLIService(cliService, sqlContext)
    }
    addService(thriftCLIService)
    super.init(sqlConf)
  }

  override def start(): Unit = {
    super.start()
    started.set(true)
  }

  override def stop(): Unit = {
    if (started.getAndSet(false)) {
      logInfo("Shutting down SparkThriftServer")
      super.stop()
    }
  }
}
