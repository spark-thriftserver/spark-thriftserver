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

package org.apache.spark.sql.thriftserver

import java.io.{ByteArrayOutputStream, File, FileInputStream}
import java.nio.charset.Charset
import java.nio.charset.StandardCharsets.UTF_8
import java.sql.{DriverManager, SQLException, Statement, Timestamp}
import java.util.{Locale, MissingFormatArgumentException}

import scala.util.{Random, Try}
import scala.util.control.NonFatal

import org.apache.spark.SparkException
import org.apache.spark.sql.SQLQueryTestSuite
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.util.fileToString
import org.apache.spark.sql.execution.HiveResult
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

/**
 * Re-run all the tests in SQLQueryTestSuite via Thrift Server.
 *
 * To run the entire test suite:
 * {{{
 *   build/sbt "hive-thriftserver/test-only *ThriftServerQueryTestSuite" -Phive-thriftserver
 * }}}
 *
 * This test suite won't generate golden files. To re-generate golden files for entire suite, run:
 * {{{
 *   SPARK_GENERATE_GOLDEN_FILES=1 build/sbt "sql/test-only *SQLQueryTestSuite"
 * }}}
 *
 * TODO:
 *   1. Support UDF testing.
 *   2. Support DESC command.
 *   3. Support SHOW command.
 */
object ThriftServerQueryTest2Suite {

  protected val inputFilePath = new File(java.nio.file.Paths.get(
    "sql", "core", "src", "test", "resources", "sql-tests").toFile, "inputs").getAbsolutePath

  val sqlFiles = new File(inputFilePath).listFiles().filterNot(_.isDirectory)

  def main(args: Array[String]): Unit = {
    val jdbcUrl = args(0)
    // scalastyle:off classforname
    Class.forName("org.apache.hive.jdbc.HiveDriver")
    // scalastyle:on classforname
    val conn = DriverManager.getConnection(s"$jdbcUrl", "b_carmel", "")
    val stat = conn.createStatement()
    stat.executeQuery("set role admin")
    sqlFiles.foreach { sqlFile =>
      val sql = fileToString(sqlFile)
      val rs = stat.executeQuery(sql)
      rs.next()
    }
  }

  def fileToString(file: File, encoding: Charset = UTF_8): String = {
    val inStream = new FileInputStream(file)
    val outStream = new ByteArrayOutputStream
    try {
      var reading = true
      while ( reading ) {
        inStream.read() match {
          case -1 => reading = false
          case c => outStream.write(c)
        }
      }
      outStream.flush()
    }
    finally {
      inStream.close()
    }
    new String(outStream.toByteArray, encoding)
  }
}
