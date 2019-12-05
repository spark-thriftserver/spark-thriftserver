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

import java.sql.DatabaseMetaData
import java.util.UUID

import org.apache.commons.lang3.exception.ExceptionUtils

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.service.SparkThriftServer2
import org.apache.spark.sql.service.cli._
import org.apache.spark.sql.service.cli.session.ServiceSession
import org.apache.spark.util.{Utils => SparkUtils}

/**
 * Spark's own GetFunctionsOperation
 *
 * @param sqlContext SQLContext to use
 * @param parentSession a ServiceSession from SessionManager
 * @param catalogName catalog name. null if not applicable
 * @param schemaName database name, null or a concrete database name
 * @param functionName function name pattern
 */
private[service] class SparkGetFunctionsOperation(
    parentSession: ServiceSession,
    catalogName: String,
    schemaName: String,
    functionName: String)
  extends SparkMetadataOperation(parentSession, OperationType.GET_FUNCTIONS) with Logging {

  private var statementId: String = _

  RESULT_SET_SCHEMA = new TableSchema()
    .addPrimitiveColumn("FUNCTION_CAT", Type.STRING_TYPE,
      "Function catalog (may be null)")
    .addPrimitiveColumn("FUNCTION_SCHEM", Type.STRING_TYPE,
      "Function schema (may be null)")
    .addPrimitiveColumn("FUNCTION_NAME", Type.STRING_TYPE,
      "Function name. This is the name used to invoke the function")
    .addPrimitiveColumn("REMARKS", Type.STRING_TYPE,
      "Explanatory comment on the function")
    .addPrimitiveColumn("FUNCTION_TYPE", Type.INT_TYPE,
      "Kind of function.")
    .addPrimitiveColumn("SPECIFIC_NAME", Type.STRING_TYPE,
      "The name which uniquely identifies this function within its schema")

  private val rowSet: RowSet = RowSetFactory.create(RESULT_SET_SCHEMA, getProtocolVersion, false)

  override def close(): Unit = {
    super.close()
    SparkThriftServer2.listener.onOperationClosed(statementId)
  }

  override def runInternal(): Unit = {
    statementId = UUID.randomUUID().toString
    // Do not change cmdStr. It's used for Hive auditing and authorization.
    val cmdStr = s"catalog : $catalogName, schemaPattern : $schemaName"
    val logMsg = s"Listing functions '$cmdStr, functionName : $functionName'"
    logInfo(s"$logMsg with $statementId")
    setState(OperationState.RUNNING)
    // Always use the latest class loader provided by executionHive's state.
    val executionHiveClassLoader = sqlContext.sharedState.jarClassLoader
    Thread.currentThread().setContextClassLoader(executionHiveClassLoader)

    val catalog = sqlContext.sessionState.catalog
    // get databases for schema pattern
    val schemaPattern = convertSchemaPattern(schemaName)
    val matchingDbs = catalog.listDatabases(schemaPattern)
    val functionPattern = CLIServiceUtils.patternToRegex(functionName)

    SparkThriftServer2.listener.onStatementStart(
      statementId,
      parentSession.getSessionHandle.getSessionId.toString,
      logMsg,
      statementId,
      parentSession.getUsername)

    try {
      matchingDbs.foreach { db =>
        catalog.listFunctions(db, functionPattern).foreach {
          case (funcIdentifier, _) =>
            val info = catalog.lookupFunctionInfo(funcIdentifier)
            val rowData = Array[AnyRef](
              DEFAULT_HIVE_CATALOG, // FUNCTION_CAT
              db, // FUNCTION_SCHEM
              funcIdentifier.funcName, // FUNCTION_NAME
              info.getUsage, // REMARKS
              DatabaseMetaData.functionResultUnknown.asInstanceOf[AnyRef], // FUNCTION_TYPE
              info.getClassName) // SPECIFIC_NAME
            rowSet.addRow(rowData);
        }
      }
      setState(OperationState.FINISHED)
    } catch {
      case e: Throwable =>
        logError(s"Error executing get functions operation with $statementId", e)
        setState(OperationState.ERROR)
        e match {
          case hiveException: ServiceSQLException =>
            SparkThriftServer2.listener.onStatementError(
              statementId, hiveException.getMessage, SparkUtils.exceptionString(hiveException))
            throw hiveException
          case _ =>
            val root = ExceptionUtils.getRootCause(e)
            SparkThriftServer2.listener.onStatementError(
              statementId, root.getMessage, SparkUtils.exceptionString(root))
            throw new ServiceSQLException("Error getting functions: " + root.toString, root)
        }
    }
    SparkThriftServer2.listener.onStatementFinish(statementId)
  }

  override def getResultSetSchema: TableSchema = {
    assertState(OperationState.FINISHED)
    RESULT_SET_SCHEMA
  }

  override def getNextRowSet(orientation: FetchOrientation, maxRows: Long): RowSet = {
    assertState(OperationState.FINISHED)
    validateDefaultFetchOrientation(orientation)
    if (orientation == FetchOrientation.FETCH_FIRST) {
      rowSet.setStartOffset(0)
    }
    rowSet.extractSubset(maxRows.toInt)
  }
}
