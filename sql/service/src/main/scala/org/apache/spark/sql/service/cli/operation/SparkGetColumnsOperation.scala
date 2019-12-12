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

import java.util.UUID
import java.util.regex.Pattern

import org.apache.commons.lang3.exception.ExceptionUtils

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.service.SparkThriftServer
import org.apache.spark.sql.service.cli._
import org.apache.spark.sql.service.cli.session.ServiceSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.{Utils => SparkUtils}

/**
 * Spark's own SparkGetColumnsOperation
 *
 * @param sqlContext SQLContext to use
 * @param parentSession a ServiceSession from SessionManager
 * @param catalogName catalog name. NULL if not applicable.
 * @param schemaName database name, NULL or a concrete database name
 * @param tableName table name
 * @param columnName column name
 */
private[service] class SparkGetColumnsOperation(
    parentSession: ServiceSession,
    catalogName: String,
    schemaName: String,
    tableName: String,
    columnName: String)
  extends SparkMetadataOperation(parentSession, OperationType.GET_COLUMNS)
    with Logging {

  val catalog: SessionCatalog = sqlContext.sessionState.catalog

  private var statementId: String = _

  RESULT_SET_SCHEMA =
    new TableSchema()
      .addPrimitiveColumn(
        "TABLE_CAT",
        Type.STRING_TYPE,
        "Catalog name. NULL if not applicable")
      .addPrimitiveColumn(
        "TABLE_SCHEM",
        Type.STRING_TYPE,
        "Schema name")
      .addPrimitiveColumn(
        "TABLE_NAME",
        Type.STRING_TYPE,
        "Table name")
      .addPrimitiveColumn("COLUMN_NAME",
        Type.STRING_TYPE, "Column name")
      .addPrimitiveColumn("DATA_TYPE",
        Type.INT_TYPE,
        "SQL type from java.sql.Types")
      .addPrimitiveColumn(
        "TYPE_NAME",
        Type.STRING_TYPE, "Data source dependent type name, " +
          "for a UDT the type name is fully qualified")
      .addPrimitiveColumn("COLUMN_SIZE",
        Type.INT_TYPE,
        "Column size. For char or date types this is the maximum number of characters, " +
          "for numeric or decimal types this is precision.")
      .addPrimitiveColumn("BUFFER_LENGTH",
        Type.TINYINT_TYPE, "Unused")
      .addPrimitiveColumn("DECIMAL_DIGITS",
        Type.INT_TYPE, "The number of fractional digits")
      .addPrimitiveColumn("NUM_PREC_RADIX",
        Type.INT_TYPE, "Radix (typically either 10 or 2)")
      .addPrimitiveColumn("NULLABLE",
        Type.INT_TYPE, "Is NULL allowed")
      .addPrimitiveColumn("REMARKS",
        Type.STRING_TYPE, "Comment describing column (may be null)")
      .addPrimitiveColumn("COLUMN_DEF",
        Type.STRING_TYPE, "Default value (may be null)")
      .addPrimitiveColumn("SQL_DATA_TYPE",
        Type.INT_TYPE, "Unused")
      .addPrimitiveColumn("SQL_DATETIME_SUB",
        Type.INT_TYPE, "Unused")
      .addPrimitiveColumn("CHAR_OCTET_LENGTH",
        Type.INT_TYPE, "For char types the maximum" +
          " number of bytes in the column")
      .addPrimitiveColumn("ORDINAL_POSITION",
        Type.INT_TYPE, "Index of column in table (starting at 1)")
      .addPrimitiveColumn("IS_NULLABLE",
        Type.STRING_TYPE,
        "\"NO\" means column definitely does not allow NULL values; \"YES\" means the " +
          "column might allow NULL values. An empty string means nobody knows.")
      .addPrimitiveColumn("SCOPE_CATALOG",
        Type.STRING_TYPE,
        "Catalog of table that is the scope of a reference attribute " +
          "(null if DATA_TYPE isn't REF)")
      .addPrimitiveColumn("SCOPE_SCHEMA",
        Type.STRING_TYPE,
        "Schema of table that is the scope of a reference attribute " +
          "(null if the DATA_TYPE isn't REF)")
      .addPrimitiveColumn("SCOPE_TABLE",
        Type.STRING_TYPE,
        "Table name that this the scope of a reference attribure (null if the DATA_TYPE isn't REF)")
      .addPrimitiveColumn("SOURCE_DATA_TYPE",
        Type.SMALLINT_TYPE,
        "Source type of a distinct type or user-generated Ref type, SQL type from java.sql.Types " +
          "(null if DATA_TYPE isn't DISTINCT or user-generated REF)")
      .addPrimitiveColumn("IS_AUTO_INCREMENT",
        Type.STRING_TYPE,
        "Indicates whether this column is auto incremented.")


  private val rowSet: RowSet = RowSetFactory.create(RESULT_SET_SCHEMA, getProtocolVersion, false)

  override def close(): Unit = {
    super.close()
    SparkThriftServer.listener.onOperationClosed(statementId)
  }

  override def runInternal(): Unit = {
    statementId = UUID.randomUUID().toString
    // Do not change cmdStr. It's used for Hive auditing and authorization.
    val cmdStr = s"catalog : $catalogName, schemaPattern : $schemaName, tablePattern : $tableName"
    val logMsg = s"Listing columns '$cmdStr, columnName : $columnName'"
    logInfo(s"$logMsg with $statementId")

    setState(OperationState.RUNNING)
    // Always use the latest class loader provided by executionHive's state.
    val executionHiveClassLoader = sqlContext.sharedState.jarClassLoader
    Thread.currentThread().setContextClassLoader(executionHiveClassLoader)

    SparkThriftServer.listener.onStatementStart(
      statementId,
      parentSession.getSessionHandle.getSessionId.toString,
      logMsg,
      statementId,
      parentSession.getUsername)

    val schemaPattern = convertSchemaPattern(schemaName)
    val tablePattern = convertIdentifierPattern(tableName, true)

    var columnPattern: Pattern = null
    if (columnName != null) {
      columnPattern = Pattern.compile(convertIdentifierPattern(columnName, false))
    }

    val db2Tabs = catalog.listDatabases(schemaPattern).map { dbName =>
      (dbName, catalog.listTables(dbName, tablePattern, includeLocalTempViews = false))
    }.toMap

    try {
      // Tables and views
      db2Tabs.foreach {
        case (dbName, tables) =>
          catalog.getTablesByName(tables).foreach { catalogTable =>
            addToRowSet(columnPattern, dbName, catalogTable.identifier.table, catalogTable.schema)
          }
      }

      // Global temporary views
      val globalTempViewDb = catalog.globalTempViewManager.database
      val databasePattern = Pattern.compile(CLIServiceUtils.patternToRegex(schemaName))
      if (databasePattern.matcher(globalTempViewDb).matches()) {
        catalog.globalTempViewManager.listViewNames(tablePattern).foreach { globalTempView =>
          catalog.globalTempViewManager.get(globalTempView).foreach { plan =>
            addToRowSet(columnPattern, globalTempViewDb, globalTempView, plan.schema)
          }
        }
      }

      // Temporary views
      catalog.listLocalTempViews(tablePattern).foreach { localTempView =>
        catalog.getTempView(localTempView.table).foreach { plan =>
          addToRowSet(columnPattern, null, localTempView.table, plan.schema)
        }
      }
      setState(OperationState.FINISHED)
    } catch {
      case e: Throwable =>
        logError(s"Error executing get columns operation with $statementId", e)
        setState(OperationState.ERROR)
        e match {
          case hiveException: ServiceSQLException =>
            SparkThriftServer.listener.onStatementError(
              statementId, hiveException.getMessage, SparkUtils.exceptionString(hiveException))
            throw hiveException
          case _ =>
            val root = ExceptionUtils.getRootCause(e)
            SparkThriftServer.listener.onStatementError(
              statementId, root.getMessage, SparkUtils.exceptionString(root))
            throw new ServiceSQLException("Error getting columns: " + root.toString, root)
        }
    }
    SparkThriftServer.listener.onStatementFinish(statementId)
  }

  private def addToRowSet(
      columnPattern: Pattern,
      dbName: String,
      tableName: String,
      schema: StructType): Unit = {
    schema.foreach { column =>
      if (columnPattern != null && !columnPattern.matcher(column.name).matches()) {
      } else {
        val rowData = Array[AnyRef](
          null, // TABLE_CAT
          dbName, // TABLE_SCHEM
          tableName, // TABLE_NAME
          column.name, // COLUMN_NAME
          Type.getType(column.dataType.sql).toJavaSQLType.asInstanceOf[AnyRef], // DATA_TYPE
          column.dataType.sql, // TYPE_NAME
          null, // COLUMN_SIZE
          null, // BUFFER_LENGTH, unused
          null, // DECIMAL_DIGITS
          null, // NUM_PREC_RADIX
          (if (column.nullable) 1 else 0).asInstanceOf[AnyRef], // NULLABLE
          column.getComment().getOrElse(""), // REMARKS
          null, // COLUMN_DEF
          null, // SQL_DATA_TYPE
          null, // SQL_DATETIME_SUB
          null, // CHAR_OCTET_LENGTH
          null, // ORDINAL_POSITION
          "YES", // IS_NULLABLE
          null, // SCOPE_CATALOG
          null, // SCOPE_SCHEMA
          null, // SCOPE_TABLE
          null, // SOURCE_DATA_TYPE
          "NO" // IS_AUTO_INCREMENT
        )
        rowSet.addRow(rowData)
      }
    }
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
