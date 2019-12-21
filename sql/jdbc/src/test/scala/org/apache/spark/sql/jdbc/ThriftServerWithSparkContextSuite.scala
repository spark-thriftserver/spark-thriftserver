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

package org.apache.spark.sql.jdbc

import java.sql.{DriverManager, Statement}

import scala.util.{Random, Try}

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.service.SparkThriftServer
import org.apache.spark.sql.service.internal.ServiceConf
import org.apache.spark.sql.test.SharedSparkSession

class ThriftServerWithSparkContextSuite extends QueryTest with SharedSparkSession {

  private var sparkServer: SparkThriftServer = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    // Chooses a random port between 10000 and 19999
    var listeningPort = 10000 + Random.nextInt(10000)

    // Retries up to 3 times with different port numbers if the server fails to start
    (1 to 3).foldLeft(Try(startThriftServer(listeningPort, 0))) { case (started, attempt) =>
      started.orElse {
        listeningPort += 1
        Try(startThriftServer(listeningPort, attempt))
      }
    }.recover {
      case cause: Throwable =>
        throw cause
    }.get
    logInfo("SparkThriftServer started successfully")
  }

  override def afterAll(): Unit = {
    try {
      sparkServer.stop()
    } finally {
      super.afterAll()
    }
  }

  test("Simple JDBC test") {
    withJdbcStatement { statement =>
      val rs = statement.executeQuery("select 1L")
      rs.next()
      assert(rs.getLong(1) === 1L)
    }
  }

  private def startThriftServer(port: Int, attempt: Int): Unit = {
    logInfo(s"Trying to start SparkThriftServer: port=$port, attempt=$attempt")
    val sqlContext = spark.newSession().sqlContext
    sqlContext.sparkContext.conf.set(ServiceConf.THRIFTSERVER_THRIFT_PORT, port)
    sparkServer = SparkThriftServer.startWithContext(sqlContext)
  }

  private def withJdbcStatement(fs: (Statement => Unit)*): Unit = {
    val user = System.getProperty("user.name")

    val serverPort = sparkServer.getSparkConf.get(ServiceConf.THRIFTSERVER_THRIFT_PORT)
    val connections =
      fs.map { _ => DriverManager.getConnection(s"jdbc:spark://localhost:$serverPort", user, "") }
    val statements = connections.map(_.createStatement())

    try {
      statements.zip(fs).foreach { case (s, f) => f(s) }
    } finally {
      statements.foreach(_.close())
      connections.foreach(_.close())
    }
  }
}
