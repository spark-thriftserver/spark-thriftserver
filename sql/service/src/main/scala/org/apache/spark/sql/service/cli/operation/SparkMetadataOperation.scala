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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.catalyst.catalog.CatalogTableType.{EXTERNAL, MANAGED, VIEW}
import org.apache.spark.sql.service.cli.{OperationState, OperationType, ServiceSQLException, TableSchema}
import org.apache.spark.sql.service.cli.session.ServiceSession


private[service] abstract class SparkMetadataOperation(
    session: ServiceSession,
    opType: OperationType)
  extends Operation(session, opType) with Logging {

  protected val DEFAULT_HIVE_CATALOG: String = ""
  protected var RESULT_SET_SCHEMA: TableSchema = null
  protected val SEARCH_STRING_ESCAPE: Char = '\\'

  protected val sqlContext: SQLContext = session.getSQLContext
  setHasResultSet(true)

  @throws[ServiceSQLException]
  override def close(): Unit = {
    setState(OperationState.CLOSED)
    cleanupOperationLog()
  }

  def tableTypeString(tableType: CatalogTableType): String = tableType match {
    case EXTERNAL | MANAGED => "TABLE"
    case VIEW => "VIEW"
    case t =>
      throw new IllegalArgumentException(s"Unknown table type is found: $t")
  }

  /**
   * Convert wildchars and escape sequence from JDBC format to datanucleous/regex
   */
  protected def convertIdentifierPattern(pattern: String, datanucleusFormat: Boolean): String =
    if (pattern == null) {
      convertPattern("%", true)
    } else {
      convertPattern(pattern, datanucleusFormat)
    }

  /**
   * Convert wildchars and escape sequence of schema pattern from JDBC format to datanucleous/regex
   * The schema pattern treats empty string also as wildchar
   */
  protected def convertSchemaPattern(pattern: String): String =
    if ((pattern == null) || pattern.isEmpty) {
      convertPattern("%", true)
    } else {
      convertPattern(pattern, true)
    }

  /**
   * Convert a pattern containing JDBC catalog search wildcards into
   * Java regex patterns.
   *
   * @param pattern input which may contain '%' or '_' wildcard characters, or
   *                these characters escaped using { @link #getSearchStringEscape()}.
   * @return replace %/_ with regex search characters, also handle escaped
   *         characters.
   *
   *         The datanucleus module expects the wildchar as '*'. The columns search on the
   *         other hand is done locally inside the hive code and that requires the regex wildchar
   *         format '.*'  This is driven by the datanucleusFormat flag.
   */
  private def convertPattern(pattern: String, datanucleusFormat: Boolean): String = {
    var wStr: String = null
    if (datanucleusFormat) wStr = "*"
    else wStr = ".*"
    pattern.replaceAll("([^\\\\])%", "$1" + wStr)
      .replaceAll("\\\\%", "%").replaceAll("^%", wStr)
      .replaceAll("([^\\\\])_", "$1.")
      .replaceAll("\\\\_", "_")
      .replaceAll("^_", ".")
  }
}
