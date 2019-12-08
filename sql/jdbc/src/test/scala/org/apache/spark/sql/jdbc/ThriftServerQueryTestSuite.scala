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
class ThriftServerQueryTestSuite extends org.apache.spark.sql.service.ThriftServerQueryTestSuite {
  override def jdbcDriver: String = classOf[SparkDriver].getCanonicalName
}
