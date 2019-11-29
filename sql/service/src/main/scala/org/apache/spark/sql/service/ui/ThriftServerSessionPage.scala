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

package org.apache.spark.sql.service.ui

import java.util.Calendar
import javax.servlet.http.HttpServletRequest

import scala.collection.JavaConverters._
import scala.xml.Node

import org.apache.spark.internal.Logging
import org.apache.spark.ui._
import org.apache.spark.ui.UIUtils._
import org.apache.spark.util.Utils

/** Page for Spark Web UI that shows statistics of jobs running in the thrift server */
private[ui] class ThriftServerSessionPage(parent: ThriftServerTab)
  extends WebUIPage("session") with Logging {

  private val listener = parent.listener
  private val startTime = Calendar.getInstance().getTime()

  /** Render the page */
  def render(request: HttpServletRequest): Seq[Node] = {
    val parameterId = request.getParameter("id")
    require(parameterId != null && parameterId.nonEmpty, "Missing id parameter")

    val content =
      listener.synchronized { // make sure all parts in this page are consistent
        val sessionStat = listener.getSession(parameterId).getOrElse(null)
        require(sessionStat != null, "Invalid sessionID[" + parameterId + "]")

        generateBasicStats() ++
        <br/> ++
        <h4>
        User {sessionStat.userName},
        IP {sessionStat.ip},
        Session created at {formatDate(sessionStat.startTimestamp)},
        Total run {sessionStat.totalExecution} SQL
        </h4> ++
        generateSQLStatsTable(request, sessionStat.sessionId)
      }
    UIUtils.headerSparkPage(request, "JDBC/ODBC Session", content, parent)
  }

  /** Generate basic stats of the thrift server program */
  private def generateBasicStats(): Seq[Node] = {
    val timeSinceStart = System.currentTimeMillis() - startTime.getTime
    <ul class ="unstyled">
      <li>
        <strong>Started at: </strong> {formatDate(startTime)}
      </li>
      <li>
        <strong>Time since start: </strong>{formatDurationVerbose(timeSinceStart)}
      </li>
    </ul>
  }

  /** Generate stats of batch statements of the thrift server program */
  private def generateSQLStatsTable(request: HttpServletRequest, sessionID: String): Seq[Node] = {
    val executionList = listener.getExecutionList
      .filter(_.sessionId == sessionID)
    val numStatement = executionList.size
    val table = if (numStatement > 0) {

      val sqlTableTag = "sqlsessionstat"

      val parameterOtherTable = request.getParameterMap().asScala
        .filterNot(_._1.startsWith(sqlTableTag))
        .map { case (name, vals) =>
          name + "=" + vals(0)
        }

      val parameterSqlTablePage = request.getParameter(s"$sqlTableTag.page")
      val parameterSqlTableSortColumn = request.getParameter(s"$sqlTableTag.sort")
      val parameterSqlTableSortDesc = request.getParameter(s"$sqlTableTag.desc")
      val parameterSqlPageSize = request.getParameter(s"$sqlTableTag.pageSize")

      val sqlTablePage = Option(parameterSqlTablePage).map(_.toInt).getOrElse(1)
      val sqlTableSortColumn = Option(parameterSqlTableSortColumn).map { sortColumn =>
        UIUtils.decodeURLParameter(sortColumn)
      }.getOrElse("Start Time")
      val sqlTableSortDesc = Option(parameterSqlTableSortDesc).map(_.toBoolean).getOrElse(
        // New executions should be shown above old executions by default.
        sqlTableSortColumn == "Start Time"
      )
      val sqlTablePageSize = Option(parameterSqlPageSize).map(_.toInt).getOrElse(100)

      try {
        Some(new SqlStatsPagedTable(
          request,
          parent,
          executionList,
          "sqlserver/session",
          UIUtils.prependBaseUri(request, parent.basePath),
          parameterOtherTable,
          sqlTableTag,
          pageSize = sqlTablePageSize,
          sortColumn = sqlTableSortColumn,
          desc = sqlTableSortDesc
        ).table(sqlTablePage))
      } catch {
        case e@(_: IllegalArgumentException | _: IndexOutOfBoundsException) =>
          Some(<div class="alert alert-error">
            <p>Error while rendering job table:</p>
            <pre>
              {Utils.exceptionString(e)}
            </pre>
          </div>)
      }
    } else {
      None
    }

    val content =
      <h5>SQL Statistics</h5> ++
        <div>
          <ul class="unstyled">
            {table.getOrElse("No statistics have been generated yet.")}
          </ul>
        </div>

    content
  }
}
