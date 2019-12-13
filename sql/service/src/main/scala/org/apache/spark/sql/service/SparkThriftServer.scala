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

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.commons.cli.{CommandLine, GnuParser, HelpFormatter, Option => CLIOption,
  OptionBuilder, Options, ParseException}

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

  /**
   * ServerOptionsProcessor.
   * Process arguments given to SparkThriftServer (-sparkconf property=value)
   * Set properties in System properties
   * Create an appropriate response object,
   * which has executor to execute the appropriate command based on the parsed options.
   */
  private[this] class ServerOptionsProcessor(val serverName: String) {
    // -sparkconf x=y
    final private val options = new Options
    private var commandLine: CommandLine = null
    final private val debugMessage = new StringBuilder

    OptionBuilder.withValueSeparator
    OptionBuilder.hasArgs(2)
    OptionBuilder.withArgName("property=value")
    OptionBuilder.withLongOpt("sparkconf")
    OptionBuilder.withDescription("Use value for given property")
    options.addOption(OptionBuilder.create)
    options.addOption(new CLIOption("H", "help", false, "Print help information"))

    def parse(argv: Array[String]): ServerOptionsProcessorResponse = {
      try {
        commandLine = new GnuParser().parse(options, argv)
        // Process --sparkconf
        // Get sparkconf param values and set the System property values
        val confProps = commandLine.getOptionProperties("sparkconf")
        confProps.stringPropertyNames().asScala.foreach(propKey => {
          // save logging message for log4j output latter after log4j initialize properly
          debugMessage.append("Setting " + propKey + "=" + confProps.getProperty(propKey) + ";\n")
          System.setProperty(propKey, confProps.getProperty(propKey))
        })
        // Process --help
        if (commandLine.hasOption('H')) {
          return new ServerOptionsProcessorResponse(new HelpOptionExecutor(serverName, options))
        }
      } catch {
        case e: ParseException =>
          // Error out & exit - we were not able to parse the args successfully
          logError("Error starting SparkThriftServer with given arguments: ")
          logError(e.getMessage)
          System.exit(-1)
      }
      // Default executor, when no option is specified
      new ServerOptionsProcessorResponse(new StartOptionExecutor)
    }

    def getDebugMessage: StringBuilder = debugMessage
  }

  /**
   * The response sent back from {@link ServerOptionsProcessor#parse(String[])}
   */
  private[this] class ServerOptionsProcessorResponse(
      val serverOptionsExecutor: ServerOptionsExecutor) {
    def getServerOptionsExecutor: ServerOptionsExecutor = serverOptionsExecutor
  }

  /**
   * The executor interface for running the appropriate
   * SparkThriftServer command based on parsed options
   */
  private[this] trait ServerOptionsExecutor {
    def execute(): Unit
  }

  /**
   * HelpOptionExecutor: executes the --help option by printing out the usage
   */
  private[this] class HelpOptionExecutor(val serverName: String, val options: Options)
    extends ServerOptionsExecutor {
    override def execute(): Unit = {
      new HelpFormatter().printHelp(serverName, options)
      System.exit(0)
    }
  }

  /**
   * StartOptionExecutor: starts SparkThriftServer.
   * This is the default executor, when no option is specified.
   */
  class StartOptionExecutor extends ServerOptionsExecutor {
    override def execute(): Unit = {
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
  }

  def main(args: Array[String]): Unit = {
    Utils.initDaemon(log)
    try {
      val oproc = new ServerOptionsProcessor("sparkserver")
      val oprocResponse = oproc.parse(args)
      // Log debug message from "oproc" after log4j initialize properly
      logDebug(oproc.getDebugMessage.toString)
      // Call the executor which will execute the appropriate command based on the parsed options
      oprocResponse.getServerOptionsExecutor.execute()
    } catch {
      case e: Exception =>
        logError("Error initializing log: " + e.getMessage, e)
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
