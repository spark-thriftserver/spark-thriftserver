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

import java.io.File
import java.sql.{DriverManager, SQLException, Statement, Timestamp}
import java.util.{Locale, MissingFormatArgumentException}

import scala.util.{Random, Try}
import scala.util.control.NonFatal

import org.apache.commons.lang3.exception.ExceptionUtils

import org.apache.spark.SparkException
import org.apache.spark.sql.SQLQueryTestSuite
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.util.fileToString
import org.apache.spark.sql.execution.HiveResult
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.service.SparkThriftServer2
import org.apache.spark.sql.service.internal.ServiceConf
import org.apache.spark.sql.types._

/**
 * Re-run all the tests in SQLQueryTestSuite via Thrift Server.
 * Note that this TestSuite does not support maven.
 *
 * TODO:
 *   1. Support UDF testing.
 *   2. Support DESC command.
 *   3. Support SHOW command.
 */
class ThriftServerQueryTestSuite extends SQLQueryTestSuite {

  private var sparkServer2: SparkThriftServer2 = _

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
    logInfo("SparkThriftServer2 started successfully")
  }

  override def afterAll(): Unit = {
    try {
      sparkServer2.stop()
    } finally {
      super.afterAll()
    }
  }

  override val isTestWithConfigSets = false

  /** List of test cases to ignore, in lower cases. */
  override def blackList: Set[String] = super.blackList ++ Set(
    // Missing UDF
    "postgreSQL/boolean.sql",
    "postgreSQL/case.sql",
    // SPARK-28620
    "postgreSQL/float4.sql",
    // SPARK-28636
    "literals.sql"
  )

  override def runQueries(
      queries: Seq[String],
      testCase: TestCase,
      configSet: Option[Seq[(String, String)]]): Unit = {
    // We do not test with configSet.
    withJdbcStatement { statement =>

      loadTestData(statement)

      testCase match {
        case _: PgSQLTest =>
          // PostgreSQL enabled cartesian product by default.
          statement.execute(s"SET ${SQLConf.CROSS_JOINS_ENABLED.key} = true")
          statement.execute(s"SET ${SQLConf.ANSI_ENABLED.key} = true")
          statement.execute(s"SET ${SQLConf.DIALECT.key} = ${SQLConf.Dialect.POSTGRESQL.toString}")
        case _ =>
      }

      // Run the SQL queries preparing them for comparison.
      val outputs: Seq[QueryOutput] = queries.map { sql =>
        val (_, output) = handleExceptions(getNormalizedResult(statement, sql))
        // We might need to do some query canonicalization in the future.
        QueryOutput(
          sql = sql,
          schema = "",
          output = output.mkString("\n").replaceAll("\\s+$", ""))
      }

      // Read back the golden file.
      val expectedOutputs: Seq[QueryOutput] = {
        val goldenOutput = fileToString(new File(testCase.resultFile))
        val segments = goldenOutput.split("-- !query.+\n")

        // each query has 3 segments, plus the header
        assert(segments.size == outputs.size * 3 + 1,
          s"Expected ${outputs.size * 3 + 1} blocks in result file but got ${segments.size}. " +
            "Try regenerate the result files.")
        Seq.tabulate(outputs.size) { i =>
          val sql = segments(i * 3 + 1).trim
          val schema = segments(i * 3 + 2).trim
          val originalOut = segments(i * 3 + 3)
          val output = if (schema != emptySchema && isNeedSort(sql)) {
            originalOut.split("\n").sorted.mkString("\n")
          } else {
            originalOut
          }
          QueryOutput(
            sql = sql,
            schema = "",
            output = output.replaceAll("\\s+$", "")
          )
        }
      }

      // Compare results.
      assertResult(expectedOutputs.size, s"Number of queries should be ${expectedOutputs.size}") {
        outputs.size
      }

      outputs.zip(expectedOutputs).zipWithIndex.foreach { case ((output, expected), i) =>
        assertResult(expected.sql, s"SQL query did not match for query #$i\n${expected.sql}") {
          output.sql
        }

        expected match {
          // Skip desc command, see HiveResult.hiveResultString
          case d if d.sql.toUpperCase(Locale.ROOT).startsWith("DESC ")
            || d.sql.toUpperCase(Locale.ROOT).startsWith("DESC\n")
            || d.sql.toUpperCase(Locale.ROOT).startsWith("DESCRIBE ")
            || d.sql.toUpperCase(Locale.ROOT).startsWith("DESCRIBE\n") =>

          // Skip show command, see HiveResult.hiveResultString
          case s if s.sql.toUpperCase(Locale.ROOT).startsWith("SHOW ")
            || s.sql.toUpperCase(Locale.ROOT).startsWith("SHOW\n") =>

          case _ if output.output.startsWith(classOf[NoSuchTableException].getPackage.getName) =>
            assert(expected.output.startsWith(classOf[NoSuchTableException].getPackage.getName),
              s"Exception did not match for query #$i\n${expected.sql}, " +
                s"expected: ${expected.output}, but got: ${output.output}")

          case _ if output.output.startsWith(classOf[SparkException].getName) &&
            output.output.contains("overflow") =>
            assert(expected.output.contains(classOf[ArithmeticException].getName) &&
              expected.output.contains("overflow"),
              s"Exception did not match for query #$i\n${expected.sql}, " +
                s"expected: ${expected.output}, but got: ${output.output}")

          case _ if output.output.startsWith(classOf[RuntimeException].getName) =>
            assert(expected.output.contains("Exception"),
              s"Exception did not match for query #$i\n${expected.sql}, " +
                s"expected: ${expected.output}, but got: ${output.output}")

          case _ if output.output.startsWith(classOf[ArithmeticException].getName) &&
            output.output.contains("causes overflow") =>
            assert(expected.output.contains(classOf[ArithmeticException].getName) &&
              expected.output.contains("causes overflow"),
              s"Exception did not match for query #$i\n${expected.sql}, " +
                s"expected: ${expected.output}, but got: ${output.output}")

          case _ if output.output.startsWith(classOf[MissingFormatArgumentException].getName) &&
            output.output.contains("Format specifier") =>
            assert(expected.output.contains(classOf[MissingFormatArgumentException].getName) &&
              expected.output.contains("Format specifier"),
              s"Exception did not match for query #$i\n${expected.sql}, " +
                s"expected: ${expected.output}, but got: ${output.output}")

          // SQLException should not exactly match. We only assert the result contains Exception.
          case _ if output.output.startsWith(classOf[SQLException].getName) =>
            assert(expected.output.contains("Exception"),
              s"Exception did not match for query #$i\n${expected.sql}, " +
                s"expected: ${expected.output}, but got: ${output.output}")

          case _ =>
            assertResult(expected.output, s"Result did not match for query #$i\n${expected.sql}") {
              output.output
            }
        }
      }
    }
  }

  override def createScalaTestCase(testCase: TestCase): Unit = {
    if (blackList.exists(t =>
      testCase.name.toLowerCase(Locale.ROOT).contains(t.toLowerCase(Locale.ROOT)))) {
      // Create a test case to ignore this case.
      ignore(testCase.name) { /* Do nothing */ }
    } else {
      // Create a test case to run this case.
      test(testCase.name) {
        runTest(testCase)
      }
    }
  }

  override def listTestCases(): Seq[TestCase] = {
    listFilesRecursively(new File(inputFilePath)).flatMap { file =>
      val resultFile = file.getAbsolutePath.replace(inputFilePath, goldenFilePath) + ".out"
      val absPath = file.getAbsolutePath
      val testCaseName = absPath.stripPrefix(inputFilePath).stripPrefix(File.separator)

      if (file.getAbsolutePath.startsWith(s"$inputFilePath${File.separator}udf")) {
        Seq.empty
      } else if (file.getAbsolutePath.startsWith(s"$inputFilePath${File.separator}postgreSQL")) {
        PgSQLTestCase(testCaseName, absPath, resultFile) :: Nil
      } else {
        RegularTestCase(testCaseName, absPath, resultFile) :: Nil
      }
    }
  }

  test("Check if ThriftServer can work") {
    withJdbcStatement { statement =>
      val rs = statement.executeQuery("select 1L")
      rs.next()
      assert(rs.getLong(1) === 1L)
    }
  }

  /** ThriftServer wraps the root exception, so it needs to be extracted. */
  override def handleExceptions(result: => (String, Seq[String])): (String, Seq[String]) = {
    super.handleExceptions {
      try {
        result
      } catch {
        case NonFatal(e) => throw ExceptionUtils.getRootCause(e)
      }
    }
  }

  private def getNormalizedResult(statement: Statement, sql: String): (String, Seq[String]) = {
    val rs = statement.executeQuery(sql)
    val cols = rs.getMetaData.getColumnCount
    val buildStr = () => (for (i <- 1 to cols) yield {
      getHiveResult(rs.getObject(i))
    }).mkString("\t")

    val answer = Iterator.continually(rs.next()).takeWhile(identity).map(_ => buildStr()).toSeq
      .map(replaceNotIncludedMsg)
    if (isNeedSort(sql)) {
      ("", answer.sorted)
    } else {
      ("", answer)
    }
  }

  private def startThriftServer(port: Int, attempt: Int): Unit = {
    logInfo(s"Trying to start SparkThriftServer2: port=$port, attempt=$attempt")
    val sqlContext = spark.newSession().sqlContext
    sqlContext.setConf(ServiceConf.THRIFTSERVER_THRIFT_PORT, port)
    sparkServer2 = SparkThriftServer2.startWithContext(sqlContext)
  }

  private def withJdbcStatement(fs: (Statement => Unit)*): Unit = {
    val user = System.getProperty("user.name")

    val serverPort = sparkServer2.getSqlConf.getConf(ServiceConf.THRIFTSERVER_THRIFT_PORT)
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

  /** Load built-in test tables. */
  private def loadTestData(statement: Statement): Unit = {
    // Prepare the data
    statement.execute(
      """
        |CREATE OR REPLACE TEMPORARY VIEW testdata as
        |SELECT id AS key, CAST(id AS string) AS value FROM range(1, 101)
      """.stripMargin)
    statement.execute(
      """
        |CREATE OR REPLACE TEMPORARY VIEW arraydata as
        |SELECT * FROM VALUES
        |(ARRAY(1, 2, 3), ARRAY(ARRAY(1, 2, 3))),
        |(ARRAY(2, 3, 4), ARRAY(ARRAY(2, 3, 4))) AS v(arraycol, nestedarraycol)
      """.stripMargin)
    statement.execute(
      """
        |CREATE OR REPLACE TEMPORARY VIEW mapdata as
        |SELECT * FROM VALUES
        |MAP(1, 'a1', 2, 'b1', 3, 'c1', 4, 'd1', 5, 'e1'),
        |MAP(1, 'a2', 2, 'b2', 3, 'c2', 4, 'd2'),
        |MAP(1, 'a3', 2, 'b3', 3, 'c3'),
        |MAP(1, 'a4', 2, 'b4'),
        |MAP(1, 'a5') AS v(mapcol)
      """.stripMargin)
    statement.execute(
      s"""
         |CREATE TEMPORARY VIEW aggtest
         |  (a int, b float)
         |USING csv
         |OPTIONS (path '${baseResourcePath.getParent}/test-data/postgresql/agg.data',
         |  header 'false', delimiter '\t')
      """.stripMargin)
    statement.execute(
      s"""
         |CREATE OR REPLACE TEMPORARY VIEW onek
         |  (unique1 int, unique2 int, two int, four int, ten int, twenty int, hundred int,
         |    thousand int, twothousand int, fivethous int, tenthous int, odd int, even int,
         |    stringu1 string, stringu2 string, string4 string)
         |USING csv
         |OPTIONS (path '${baseResourcePath.getParent}/test-data/postgresql/onek.data',
         |  header 'false', delimiter '\t')
      """.stripMargin)
    statement.execute(
      s"""
         |CREATE OR REPLACE TEMPORARY VIEW tenk1
         |  (unique1 int, unique2 int, two int, four int, ten int, twenty int, hundred int,
         |    thousand int, twothousand int, fivethous int, tenthous int, odd int, even int,
         |    stringu1 string, stringu2 string, string4 string)
         |USING csv
         |  OPTIONS (path '${baseResourcePath.getParent}/test-data/postgresql/tenk.data',
         |  header 'false', delimiter '\t')
      """.stripMargin)
  }

  // Returns true if sql is retrieving data.
  private def isNeedSort(sql: String): Boolean = {
    val upperCase = sql.toUpperCase(Locale.ROOT)
    upperCase.startsWith("SELECT ") || upperCase.startsWith("SELECT\n") ||
      upperCase.startsWith("WITH ") || upperCase.startsWith("WITH\n") ||
      upperCase.startsWith("VALUES ") || upperCase.startsWith("VALUES\n") ||
      // postgreSQL/union.sql
      upperCase.startsWith("(")
  }

  private def getHiveResult(obj: Object): String = {
    obj match {
      case null =>
        HiveResult.toHiveString((null, StringType))
      case d: java.sql.Date =>
        HiveResult.toHiveString((d, DateType))
      case t: Timestamp =>
        HiveResult.toHiveString((t, TimestampType))
      case d: java.math.BigDecimal =>
        HiveResult.toHiveString((d, DecimalType.fromBigDecimal(d)))
      case bin: Array[Byte] =>
        HiveResult.toHiveString((bin, BinaryType))
      case other =>
        other.toString
    }
  }
}
