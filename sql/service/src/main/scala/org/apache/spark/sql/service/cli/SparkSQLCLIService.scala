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

package org.apache.spark.sql.service.cli

import java.io.IOException
import java.util.{List => JList}
import javax.security.auth.login.LoginException

import scala.collection.JavaConverters._

import org.apache.hadoop.security.{SecurityUtil, UserGroupInformation}
import org.slf4j.Logger

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.service.{AbstractService, Service, ServiceException}
import org.apache.spark.sql.service.ReflectionUtils._
import org.apache.spark.sql.service.Service.STATE
import org.apache.spark.sql.service.auth.SparkAuthFactory
import org.apache.spark.sql.service.cli.session.SessionManager
import org.apache.spark.sql.service.internal.ServiceConf
import org.apache.spark.sql.service.server.SparkServer2
import org.apache.spark.sql.service.utils.Utils

private[service] class SparkSQLCLIService(
     sparkServer: SparkServer2,
     sqlContext: SQLContext)
  extends CLIService(sparkServer, sqlContext)
  with ReflectedCompositeService {

  override def init(sqlConf: SQLConf): Unit = {
    setSuperField(this, "sqlConf", sqlConf)

    val sessionManager = new SessionManager(sparkServer, sqlContext)
    setSuperField(this, "sessionManager", sessionManager)
    addService(sessionManager)
    var sparkServiceUGI: UserGroupInformation = null
    var httpUGI: UserGroupInformation = null

    if (UserGroupInformation.isSecurityEnabled) {
      try {
        val principal = sqlConf.getConf(ServiceConf.THRIFTSERVER_KERBEROS_PRINCIPAL)
        val keyTabFile = sqlConf.getConf(ServiceConf.THRIFTSERVER_KERBEROS_KEYTAB)
        if (principal.isEmpty || keyTabFile.isEmpty) {
          throw new IOException(
            "SparkServer2 Kerberos principal or keytab is not correctly configured")
        }

        val originalUgi = UserGroupInformation.getCurrentUser
        sparkServiceUGI = if (SparkAuthFactory.needUgiLogin(originalUgi,
          SecurityUtil.getServerPrincipal(principal, "0.0.0.0"), keyTabFile)) {
          SparkAuthFactory.loginFromKeytab(sqlConf)
          Utils.getUGI()
        } else {
          originalUgi
        }

        setSuperField(this, "serviceUGI", sparkServiceUGI)
      } catch {
        case e @ (_: IOException | _: LoginException) =>
          throw new ServiceException("Unable to login to kerberos with given principal/keytab", e)
      }

      // Try creating spnego UGI if it is configured.
      val principal = sqlConf.getConf(ServiceConf.THRIFTSERVER_SPNEGO_PRINCIPAL).trim
      val keyTabFile = sqlConf.getConf(ServiceConf.THRIFTSERVER_SPNEGO_KEYTAB).trim
      if (principal.nonEmpty && keyTabFile.nonEmpty) {
        try {
          httpUGI = SparkAuthFactory.loginFromSpnegoKeytabAndReturnUGI(sqlConf)
          setSuperField(this, "httpUGI", httpUGI)
        } catch {
          case e: IOException =>
            throw new ServiceException("Unable to login to spnego with given principal " +
              s"$principal and keytab $keyTabFile: $e", e)
        }
      }
    }

    initCompositeService(sqlConf)
  }

  override def getInfo(sessionHandle: SessionHandle, getInfoType: GetInfoType): GetInfoValue = {
    getInfoType match {
      case GetInfoType.CLI_SERVER_NAME => new GetInfoValue("Spark SQL")
      case GetInfoType.CLI_DBMS_NAME => new GetInfoValue("Spark SQL")
      case GetInfoType.CLI_DBMS_VER => new GetInfoValue(sqlContext.sparkContext.version)
      case _ => super.getInfo(sessionHandle, getInfoType)
    }
  }
}

private[service] trait ReflectedCompositeService { this: AbstractService =>
  def initCompositeService(sqlConf: SQLConf): Unit = {
    // Emulating `CompositeService.init(hiveConf)`
    val serviceList = getAncestorField[JList[Service]](this, 2, "serviceList")
    serviceList.asScala.foreach(_.init(sqlConf))

    // Emulating `AbstractService.init(hiveConf)`
    invoke(classOf[AbstractService], this, "ensureCurrentState", classOf[STATE] -> STATE.NOTINITED)
    setAncestorField(this, 3, "sqlConf", sqlConf)
    invoke(classOf[AbstractService], this, "changeState", classOf[STATE] -> STATE.INITED)
    getAncestorField[Logger](this, 3, "LOG").info(s"Service: $getName is inited.")
  }
}
