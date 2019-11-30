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

import org.apache.hadoop.hive.conf.HiveConf

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveUtils
import org.apache.spark.sql.service.ReflectionUtils._
import org.apache.spark.sql.service.SparkThriftServer2
import org.apache.spark.sql.service.cli.{ReflectedCompositeService, SessionHandle}
import org.apache.spark.sql.service.cli.operation.OperationManager
import org.apache.spark.sql.service.rpc.thrift.TProtocolVersion
import org.apache.spark.sql.service.server.SparkServer2


private[service] class SparkSQLSessionManager(sparkServer: SparkServer2, sqlContext: SQLContext)
  extends SessionManager(sparkServer)
  with ReflectedCompositeService
  with Logging {

  private lazy val sparkSqlOperationManager = new OperationManager()

  override def init(hiveConf: HiveConf): Unit = {
    setSuperField(this, "operationManager", sparkSqlOperationManager)
    super.init(hiveConf)
  }

  override def openSession(
      protocol: TProtocolVersion,
      username: String,
      passwd: String,
      ipAddress: String,
      sessionConf: java.util.Map[String, String],
      withImpersonation: Boolean,
      delegationToken: String): SessionHandle = {
    val ctx = if (sqlContext.conf.hiveThriftServerSingleSession) {
      logWarning("single session")
      sqlContext
    } else {
      logWarning("No single session")
      sqlContext.newSession()
    }

    val sessionHandle =
      super.openSession(protocol, username, passwd, ipAddress, sessionConf, withImpersonation,
          delegationToken, ctx)
    val session = super.getSession(sessionHandle)
    logWarning(s"super.getSession ${session.getSessionHandle}")
    SparkThriftServer2.listener.onSessionCreated(
      session.getIpAddress, sessionHandle.getSessionId.toString, session.getUsername)
    logWarning("create new session context")
    logWarning(s"sqlContext = ${sqlContext}")

    ctx.setConf(HiveUtils.FAKE_HIVE_VERSION.key, HiveUtils.builtinHiveVersion)
    val hiveSessionState = session.getSessionState
    setConfMap(ctx, hiveSessionState.getOverriddenConfigurations)
    setConfMap(ctx, hiveSessionState.getHiveVariables)
    if (sessionConf != null && sessionConf.containsKey("use:database")) {
      ctx.sql(s"use ${sessionConf.get("use:database")}")
    }
    sessionHandle
  }

  override def closeSession(sessionHandle: SessionHandle): Unit = {
    SparkThriftServer2.listener.onSessionClosed(sessionHandle.getSessionId.toString)
    super.closeSession(sessionHandle)
    sparkSqlOperationManager.sessionToActivePool.remove(sessionHandle)
  }

  def setConfMap(conf: SQLContext, confMap: java.util.Map[String, String]): Unit = {
    val iterator = confMap.entrySet().iterator()
    while (iterator.hasNext) {
      val kv = iterator.next()
      conf.setConf(kv.getKey, kv.getValue)
    }
  }
}
