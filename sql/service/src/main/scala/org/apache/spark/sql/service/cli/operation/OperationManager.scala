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

package org.apache.spark.sql.service.cli.operation

import java.sql.SQLException
import java.util
import java.util.{List => JList, Map => JMap}
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.api.{FieldSchema, Schema}
import org.apache.hadoop.hive.ql.session.OperationLog
import org.apache.log4j.Logger

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.service.AbstractService
import org.apache.spark.sql.service.cli._
import org.apache.spark.sql.service.cli.session.HiveSession
import org.apache.spark.sql.types.StructType

class OperationManager
  extends AbstractService(classOf[OperationManager].getSimpleName)
    with Logging {

  private[this] lazy val logSchema: StructType = new StructType().add("operation_log", "string")
  private[this] val handleToOperation = new ConcurrentHashMap[OperationHandle, Operation]
  val sessionToContexts = new ConcurrentHashMap[SessionHandle, SQLContext]()
  val sessionToActivePool = new ConcurrentHashMap[SessionHandle, String]()

  override def init(hiveConf: HiveConf): Unit = synchronized {
    if (hiveConf.getBoolVar(HiveConf.ConfVars.HIVE_SERVER2_LOGGING_OPERATION_ENABLED)) {
      initOperationLogCapture(
        hiveConf.getVar(HiveConf.ConfVars.HIVE_SERVER2_LOGGING_OPERATION_LEVEL))
    } else {
      logDebug("Operation level logging is turned off")
    }
    super.init(hiveConf)
  }

  override def start(): Unit = {
    super.start()
    // TODO
  }

  override def stop(): Unit = {
    super.stop()
  }

  private def initOperationLogCapture(loggingMode: String): Unit = {
    // Register another Appender (with the same layout) that talks to us.
    val ap = new LogDivertAppender(this, OperationLog.getLoggingLevel(loggingMode))
    Logger.getRootLogger.addAppender(ap)
  }


  def newExecuteStatementOperation(parentSession: HiveSession,
                                   statement: String,
                                   confOverlay: JMap[String, String],
                                   async: Boolean,
                                   queryTimeOut: Long): SparkExecuteStatementOperation = {
    synchronized {
      val sqlContext = sessionToContexts.get(parentSession.getSessionHandle)
      require(sqlContext != null, s"Session handle: ${parentSession.getSessionHandle} " +
        s"has not been initialized or had already closed.")
      val conf = sqlContext.sessionState.conf
      val runInBackground = async && conf.getConf(HiveUtils.HIVE_THRIFT_SERVER_ASYNC)
      val operation = new SparkExecuteStatementOperation(parentSession, statement, confOverlay,
        runInBackground)(sqlContext, sessionToActivePool)
      handleToOperation.put(operation.getHandle, operation)
      logDebug(s"Created Operation for $statement with session=$parentSession, " +
        s"runInBackground=$runInBackground")
      operation
    }
  }

  def newGetTypeInfoOperation(session: HiveSession): SparkGetTypeInfoOperation =
    synchronized {
      val sqlContext = sessionToContexts.get(session.getSessionHandle)
      require(sqlContext != null, s"Session handle: ${session.getSessionHandle} has not been" +
        " initialized or had already closed.")
      val operation = new SparkGetTypeInfoOperation(sqlContext, session)
      handleToOperation.put(operation.getHandle, operation)
      logDebug(s"Created GetTypeInfoOperation with session=$session.")
      operation
    }

  def newGetCatalogsOperation(session: HiveSession): SparkGetCatalogsOperation =
    synchronized {
      val sqlContext = sessionToContexts.get(session.getSessionHandle)
      require(sqlContext != null, s"Session handle: ${session.getSessionHandle} has not been" +
        " initialized or had already closed.")
      val operation = new SparkGetCatalogsOperation(sqlContext, session)
      handleToOperation.put(operation.getHandle, operation)
      logDebug(s"Created GetCatalogsOperation with session=$session.")
      operation
    }

  def newGetSchemasOperation(session: HiveSession,
                             catalogName: String,
                             schemaName: String): SparkGetSchemasOperation = synchronized {
    val sqlContext = sessionToContexts.get(session.getSessionHandle)
    require(sqlContext != null, s"Session handle: ${session.getSessionHandle} has not been" +
      " initialized or had already closed.")
    val operation = new SparkGetSchemasOperation(sqlContext, session, catalogName, schemaName)
    handleToOperation.put(operation.getHandle, operation)
    logDebug(s"Created GetSchemasOperation with session=$session.")
    operation
  }

  def newGetTablesOperation(session: HiveSession,
                            catalogName: String,
                            schemaName: String,
                            tableName: String,
                            tableTypes: JList[String]): SparkMetadataOperation = synchronized {
    val sqlContext = sessionToContexts.get(session.getSessionHandle)
    require(sqlContext != null, s"Session handle: ${session.getSessionHandle} has not been" +
      " initialized or had already closed.")
    val operation = new SparkGetTablesOperation(sqlContext, session,
      catalogName, schemaName, tableName, tableTypes)
    handleToOperation.put(operation.getHandle, operation)
    logDebug(s"Created GetTablesOperation with session=$session.")
    operation
  }

  def newGetColumnsOperation(session: HiveSession,
                             catalogName: String,
                             schemaName: String,
                             tableName: String,
                             columnName: String): SparkGetColumnsOperation = synchronized {
    val sqlContext = sessionToContexts.get(session.getSessionHandle)
    require(sqlContext != null, s"Session handle: ${session.getSessionHandle} has not been" +
      " initialized or had already closed.")
    val operation = new SparkGetColumnsOperation(sqlContext, session,
      catalogName, schemaName, tableName, columnName)
    handleToOperation.put(operation.getHandle, operation)
    logDebug(s"Created GetColumnsOperation with session=$session.")
    operation
  }

  def newGetTableTypesOperation(session: HiveSession): SparkGetTableTypesOperation =
    synchronized {
      val sqlContext = sessionToContexts.get(session.getSessionHandle)
      require(sqlContext != null, s"Session handle: ${session.getSessionHandle} has not been" +
        " initialized or had already closed.")
      val operation = new SparkGetTableTypesOperation(sqlContext, session)
      handleToOperation.put(operation.getHandle, operation)
      logDebug(s"Created GetTableTypesOperation with session=$session.")
      operation
    }

  def newGetFunctionsOperation(session: HiveSession,
                               catalogName: String,
                               schemaName: String,
                               functionName: String): SparkGetFunctionsOperation = synchronized {
    val sqlContext = sessionToContexts.get(session.getSessionHandle)
    require(sqlContext != null, s"Session handle: ${session.getSessionHandle} has not been" +
      " initialized or had already closed.")
    val operation = new SparkGetFunctionsOperation(sqlContext, session,
      catalogName, schemaName, functionName)
    handleToOperation.put(operation.getHandle, operation)
    logDebug(s"Created GetFunctionsOperation with session=$session.")
    operation
  }

  def newGetPrimaryKeysOperation(session: HiveSession,
                                 catalogName: String,
                                 schemaName: String,
                                 tableName: String): Operation = {
    throw new HiveSQLException("GetPrimaryKeysOperation is not supported yet")
  }

  def newGetCrossReferenceOperation(parentSession: HiveSession,
                                    primaryCatalog: String,
                                    primarySchema: String,
                                    primaryTable: String,
                                    foreignCatalog: String,
                                    foreignSchema: String,
                                    foreignTable: String): Operation = {
    throw new HiveSQLException("GetCrossReferenceOperation is not supported yet")
  }

  def setConfMap(conf: SQLConf, confMap: java.util.Map[String, String]): Unit = {
    val iterator = confMap.entrySet().iterator()
    while (iterator.hasNext) {
      val kv = iterator.next()
      conf.setConfString(kv.getKey, kv.getValue)
    }
  }

  @throws[HiveSQLException]
  def getOperation(operationHandle: OperationHandle): Operation = {
    val operation: Operation = getOperationInternal(operationHandle)
    if (operation == null) {
      throw new HiveSQLException("Invalid OperationHandle: " + operationHandle)
    }
    operation
  }

  private def getOperationInternal(operationHandle: OperationHandle): Operation = {
    handleToOperation.get(operationHandle)
  }

  private def removeTimedOutOperation(operationHandle: OperationHandle): Operation = {
    val operation: Operation = handleToOperation.get(operationHandle)
    if (operation != null && operation.isTimedOut(System.currentTimeMillis)) {
      handleToOperation.remove(operationHandle)
      return operation
    }
    null
  }

  private def addOperation(operation: Operation): Unit = {
    handleToOperation.put(operation.getHandle, operation)
  }

  private def removeOperation(opHandle: OperationHandle): Operation = {
    handleToOperation.remove(opHandle)
  }

  @throws[HiveSQLException]
  def getOperationStatus(opHandle: OperationHandle): OperationStatus = {
    getOperation(opHandle).getStatus
  }

  @throws[HiveSQLException]
  def cancelOperation(opHandle: OperationHandle): Unit = {
    val operation: Operation = getOperation(opHandle)
    val opState: OperationState = operation.getStatus.getState
    if ((opState eq OperationState.CANCELED) ||
      (opState eq OperationState.CLOSED) ||
      (opState eq OperationState.FINISHED) ||
      (opState eq OperationState.ERROR) ||
      (opState eq OperationState.UNKNOWN)) { // Cancel should be a no-op in either cases
      logDebug(opHandle + ": Operation is already aborted in state - " + opState)
    } else {
      logDebug(opHandle + ": Attempting to cancel from state - " + opState)
      operation.cancel
    }
  }

  @throws[HiveSQLException]
  def closeOperation(opHandle: OperationHandle): Unit = {
    val operation: Operation = removeOperation(opHandle)
    if (operation == null) {
      throw new HiveSQLException("Operation does not exist!")
    }
    operation.close
  }

  @throws[HiveSQLException]
  def getOperationResultSetSchema(opHandle: OperationHandle): TableSchema = {
    getOperation(opHandle).getResultSetSchema
  }

  @throws[HiveSQLException]
  def getOperationNextRowSet(opHandle: OperationHandle): RowSet = {
    getOperation(opHandle).getNextRowSet
  }

  @throws[HiveSQLException]
  def getOperationNextRowSet(opHandle: OperationHandle,
                             orientation: FetchOrientation,
                             maxRows: Long): RowSet = {
    getOperation(opHandle).getNextRowSet(orientation, maxRows)
  }

  @throws[HiveSQLException]
  def getOperationLogRowSet(opHandle: OperationHandle,
                            orientation: FetchOrientation,
                            maxRows: Long): RowSet = {
    // get the OperationLog object from the operation
    val operationLog: OperationLog = getOperation(opHandle).getOperationLog
    if (operationLog == null) {
      throw new HiveSQLException("Couldn't find log associated " +
        "with operation handle: " + opHandle)
    }
    // read logs
    var logs: util.List[String] = null
    try {
      logs = operationLog.readOperationLog(isFetchFirst(orientation), maxRows)
    } catch {
      case e: SQLException =>
        throw new HiveSQLException(e.getMessage, e.getCause)
    }
    // convert logs to RowSet
    // convert logs to RowSet
    val tableSchema: TableSchema = new TableSchema(getLogSchema)
    val rowSet: RowSet =
      RowSetFactory.create(tableSchema, getOperation(opHandle).getProtocolVersion, false)
    for (log <- logs.asScala) {
      rowSet.addRow(Array[AnyRef](log))
    }
    rowSet
  }

  private def getLogSchema = {
    val schema = new Schema
    val fieldSchema = new FieldSchema
    fieldSchema.setName("operation_log")
    fieldSchema.setType("string")
    schema.addToFieldSchemas(fieldSchema)
    schema
  }

  private def isFetchFirst(fetchOrientation: FetchOrientation): Boolean = {
    // TODO: Since OperationLog is moved to package o.a.h.h.ql.session,
    // we may add a Enum there and map FetchOrientation to it.
    if (fetchOrientation.equals(FetchOrientation.FETCH_FIRST)) {
      return true
    }
    false
  }

  def getOperationLogByThread: OperationLog = {
    OperationLog.getCurrentOperationLog
  }

  def removeExpiredOperations(handles: Array[OperationHandle]): JList[Operation] = {
    val removed: util.List[Operation] = new util.ArrayList[Operation]
    handles.foreach(handle => {
      val operation: Operation = removeTimedOutOperation(handle)
      if (operation != null) {
        logWarning("Operation " + handle + " is timed-out and will be closed")
        removed.add(operation)
      }
    })
    removed
  }
}
