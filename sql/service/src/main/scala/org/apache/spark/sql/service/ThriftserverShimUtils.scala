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

package org.apache.spark.sql.service

import org.apache.spark.sql.service.cli.Type
import org.apache.spark.sql.service.cli.Type._
import org.apache.spark.sql.service.rpc.thrift.TProtocolVersion._

/**
 * Various utilities for hive-thriftserver used to upgrade the built-in Hive.
 */
private[spark] object ThriftserverShimUtils {

  private[spark] def supportedType(): Seq[Type] = {
    Seq(NULL_TYPE, BOOLEAN_TYPE, STRING_TYPE, BINARY_TYPE,
      TINYINT_TYPE, SMALLINT_TYPE, INT_TYPE, BIGINT_TYPE,
      FLOAT_TYPE, DOUBLE_TYPE, DECIMAL_TYPE,
      DATE_TYPE, TIMESTAMP_TYPE,
      ARRAY_TYPE, MAP_TYPE, STRUCT_TYPE)
  }

  private[spark] val testedProtocolVersions = Seq(
    HIVE_CLI_SERVICE_PROTOCOL_V1,
    HIVE_CLI_SERVICE_PROTOCOL_V2,
    HIVE_CLI_SERVICE_PROTOCOL_V3,
    HIVE_CLI_SERVICE_PROTOCOL_V4,
    HIVE_CLI_SERVICE_PROTOCOL_V5,
    HIVE_CLI_SERVICE_PROTOCOL_V6,
    HIVE_CLI_SERVICE_PROTOCOL_V7,
    HIVE_CLI_SERVICE_PROTOCOL_V8,
    HIVE_CLI_SERVICE_PROTOCOL_V9,
    HIVE_CLI_SERVICE_PROTOCOL_V10)
}
