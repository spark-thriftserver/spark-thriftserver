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

package org.apache.spark.sql.thriftserver.cli.operation

import java.sql.SQLException
import java.util.{ArrayList => JArrayList, List => JList, Map => JMap}
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

import org.apache.log4j.Logger

import org.apache.spark.internal.Logging
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.thriftserver.AbstractService
import org.apache.spark.sql.thriftserver.cli.{FetchOrientation, OperationHandle, OperationState, OperationStatus, RowSet, RowSetFactory, ServiceSQLException, SessionHandle, TableSchema}
import org.apache.spark.sql.thriftserver.cli.session.ServiceSession
import org.apache.spark.sql.thriftserver.internal.ServiceConf
import org.apache.spark.sql.types.{StringType, StructField, StructType}

class OperationManager
  extends AbstractService(classOf[OperationManager].getSimpleName)
    with Logging {

  private[this] val handleToOperation = new ConcurrentHashMap[OperationHandle, Operation]
  val sessionToActivePool = new ConcurrentHashMap[SessionHandle, String]()

  override def init(conf: SQLConf): Unit = synchronized {
    if (conf.getConf(ServiceConf.THRIFTSERVER_LOGGING_OPERATION_ENABLE)) {
      initOperationLogCapture(conf.getConf(ServiceConf.THRIFTSERVER_LOGGING_OPERATION_LEVEL))
    } else {
      logDebug("Operation level logging is turned off")
    }
    super.init(conf)
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


  def newExecuteStatementOperation(
      parentSession: ServiceSession,
      statement: String,
      confOverlay: JMap[String, String],
      async: Boolean,
      queryTimeOut: Long): SparkExecuteStatementOperation = synchronized {
    val conf = parentSession.getSQLContext.sessionState.conf
    val runInBackground = async && conf.getConf(ServiceConf.THRIFTSERVER_ASYNC)
    val operation = new SparkExecuteStatementOperation(parentSession, statement, confOverlay,
      runInBackground)(sessionToActivePool)
    handleToOperation.put(operation.getHandle, operation)
    logDebug(s"Created Operation for $statement with session=$parentSession, " +
      s"runInBackground=$runInBackground")
    operation
  }

  def newGetTypeInfoOperation(session: ServiceSession): SparkGetTypeInfoOperation = synchronized {
    val operation = new SparkGetTypeInfoOperation(session)
    handleToOperation.put(operation.getHandle, operation)
    logDebug(s"Created GetTypeInfoOperation with session=$session.")
    operation
  }

  def newGetCatalogsOperation(session: ServiceSession): SparkGetCatalogsOperation = synchronized {
    val operation = new SparkGetCatalogsOperation(session)
    handleToOperation.put(operation.getHandle, operation)
    logDebug(s"Created GetCatalogsOperation with session=$session.")
    operation
  }

  def newGetSchemasOperation(
      session: ServiceSession,
      catalogName: String,
      schemaName: String): SparkGetSchemasOperation = synchronized {
    val operation = new SparkGetSchemasOperation(session, catalogName, schemaName)
    handleToOperation.put(operation.getHandle, operation)
    logDebug(s"Created GetSchemasOperation with session=$session.")
    operation
  }

  def newGetTablesOperation(
      session: ServiceSession,
      catalogName: String,
      schemaName: String,
      tableName: String,
      tableTypes: JList[String]): SparkMetadataOperation = synchronized {
    val operation = new SparkGetTablesOperation(session,
      catalogName, schemaName, tableName, tableTypes)
    handleToOperation.put(operation.getHandle, operation)
    logDebug(s"Created GetTablesOperation with session=$session.")
    operation
  }

  def newGetColumnsOperation(
      session: ServiceSession,
      catalogName: String,
      schemaName: String,
      tableName: String,
      columnName: String): SparkGetColumnsOperation = synchronized {
    val operation = new SparkGetColumnsOperation(session,
      catalogName, schemaName, tableName, columnName)
    handleToOperation.put(operation.getHandle, operation)
    logDebug(s"Created GetColumnsOperation with session=$session.")
    operation
  }

  def newGetTableTypesOperation(
       session: ServiceSession): SparkGetTableTypesOperation = synchronized {
    val operation = new SparkGetTableTypesOperation(session)
    handleToOperation.put(operation.getHandle, operation)
    logDebug(s"Created GetTableTypesOperation with session=$session.")
    operation
  }

  def newGetFunctionsOperation(
      session: ServiceSession,
      catalogName: String,
      schemaName: String,
      functionName: String): SparkGetFunctionsOperation = synchronized {
    val operation = new SparkGetFunctionsOperation(session,
      catalogName, schemaName, functionName)
    handleToOperation.put(operation.getHandle, operation)
    logDebug(s"Created GetFunctionsOperation with session=$session.")
    operation
  }

  def newGetPrimaryKeysOperation(
      session: ServiceSession,
      catalogName: String,
      schemaName: String,
      tableName: String): Operation = {
    throw new ServiceSQLException("GetPrimaryKeysOperation is not supported yet")
  }

  def newGetCrossReferenceOperation(
      parentSession: ServiceSession,
      primaryCatalog: String,
      primarySchema: String,
      primaryTable: String,
      foreignCatalog: String,
      foreignSchema: String,
      foreignTable: String): Operation = {
    throw new ServiceSQLException("GetCrossReferenceOperation is not supported yet")
  }

  def setConfMap(conf: SQLConf, confMap: java.util.Map[String, String]): Unit = {
    val iterator = confMap.entrySet().iterator()
    while (iterator.hasNext) {
      val kv = iterator.next()
      conf.setConfString(kv.getKey, kv.getValue)
    }
  }

  @throws[ServiceSQLException]
  def getOperation(operationHandle: OperationHandle): Operation = {
    val operation: Operation = getOperationInternal(operationHandle)
    if (operation == null) {
      throw new ServiceSQLException("Invalid OperationHandle: " + operationHandle)
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

  @throws[ServiceSQLException]
  def getOperationStatus(opHandle: OperationHandle): OperationStatus = {
    getOperation(opHandle).getStatus
  }

  @throws[ServiceSQLException]
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

  @throws[ServiceSQLException]
  def closeOperation(opHandle: OperationHandle): Unit = {
    val operation: Operation = removeOperation(opHandle)
    if (operation == null) {
      throw new ServiceSQLException("Operation does not exist!")
    }
    operation.close
  }

  @throws[ServiceSQLException]
  def getOperationResultSetSchema(opHandle: OperationHandle): TableSchema = {
    getOperation(opHandle).getResultSetSchema
  }

  @throws[ServiceSQLException]
  def getOperationNextRowSet(opHandle: OperationHandle): RowSet = {
    getOperation(opHandle).getNextRowSet
  }

  @throws[ServiceSQLException]
  def getOperationNextRowSet(
      opHandle: OperationHandle,
      orientation: FetchOrientation,
      maxRows: Long): RowSet = {
    getOperation(opHandle).getNextRowSet(orientation, maxRows)
  }

  @throws[ServiceSQLException]
  def getOperationLogRowSet(
      opHandle: OperationHandle,
      orientation: FetchOrientation,
      maxRows: Long): RowSet = {
    // get the OperationLog object from the operation
    val operationLog: OperationLog = getOperation(opHandle).getOperationLog
    if (operationLog == null) {
      throw new ServiceSQLException("Couldn't find log associated " +
        "with operation handle: " + opHandle)
    }
    // read logs
    var logs: JList[String] = null
    try {
      logs = operationLog.readOperationLog(isFetchFirst(orientation), maxRows)
    } catch {
      case e: SQLException =>
        throw new ServiceSQLException(e.getMessage, e.getCause)
    }
    // convert logs to RowSet
    val tableSchema = new TableSchema(StructType(StructField("operation_log", StringType) :: Nil))
    val rowSet: RowSet =
      RowSetFactory.create(tableSchema, getOperation(opHandle).getProtocolVersion, false)
    for (log <- logs.asScala) {
      rowSet.addRow(Array[AnyRef](log))
    }
    rowSet
  }

  private def isFetchFirst(fetchOrientation: FetchOrientation): Boolean = {
    // TODO: Since OperationLog is moved to package o.a.h.h.ql.session,
    // we may add a Enum there and map FetchOrientation to it.
    fetchOrientation.equals(FetchOrientation.FETCH_FIRST)
  }

  def getOperationLogByThread: OperationLog = {
    OperationLog.getCurrentOperationLog
  }

  def removeExpiredOperations(handles: Array[OperationHandle]): JList[Operation] = {
    val removed: JList[Operation] = new JArrayList[Operation]
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
