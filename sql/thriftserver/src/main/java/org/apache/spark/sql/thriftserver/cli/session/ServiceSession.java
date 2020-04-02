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

package org.apache.spark.sql.thriftserver.cli.session;

import java.util.List;
import java.util.Map;

import org.apache.spark.sql.thriftserver.auth.SparkAuthFactory;
import org.apache.spark.sql.thriftserver.cli.*;

public interface ServiceSession extends ServiceSessionBase {

  void open(Map<String, String> sessionConfMap) throws Exception;

  GetInfoValue getInfo(GetInfoType getInfoType) throws ServiceSQLException;

  OperationHandle executeStatement(
      String statement,
      Map<String, String> confOverlay) throws ServiceSQLException;

  OperationHandle executeStatement(
      String statement,
      Map<String, String> confOverlay,
      long queryTimeout) throws ServiceSQLException;

  OperationHandle executeStatementAsync(
      String statement,
      Map<String, String> confOverlay) throws ServiceSQLException;

  OperationHandle executeStatementAsync(
      String statement,
      Map<String, String> confOverlay,
      long queryTimeout) throws ServiceSQLException;

  OperationHandle getTypeInfo() throws ServiceSQLException;

  OperationHandle getCatalogs() throws ServiceSQLException;

  OperationHandle getSchemas(
      String catalogName,
      String schemaName) throws ServiceSQLException;

  OperationHandle getTables(
      String catalogName,
      String schemaName,
      String tableName,
      List<String> tableTypes) throws ServiceSQLException;

  OperationHandle getTableTypes() throws ServiceSQLException;

  OperationHandle getColumns(
      String catalogName,
      String schemaName,
      String tableName,
      String columnName)  throws ServiceSQLException;

  OperationHandle getFunctions(
      String catalogName,
      String schemaName,
      String functionName) throws ServiceSQLException;

  OperationHandle getPrimaryKeys(
      String catalog,
      String schema,
      String table) throws ServiceSQLException;

  OperationHandle getCrossReference(
      String primaryCatalog,
      String primarySchema,
      String primaryTable,
      String foreignCatalog,
      String foreignSchema,
      String foreignTable) throws ServiceSQLException;

  void close() throws ServiceSQLException;

  void cancelOperation(OperationHandle opHandle) throws ServiceSQLException;

  void closeOperation(OperationHandle opHandle) throws ServiceSQLException;

  TableSchema getResultSetMetadata(OperationHandle opHandle)
      throws ServiceSQLException;

  RowSet fetchResults(
      OperationHandle opHandle,
      FetchOrientation orientation,
      long maxRows,
      FetchType fetchType) throws ServiceSQLException;

  String getDelegationToken(
      SparkAuthFactory authFactory,
      String owner,
      String renewer) throws ServiceSQLException;

  void cancelDelegationToken(
      SparkAuthFactory authFactory,
      String tokenStr) throws ServiceSQLException;

  void renewDelegationToken(
      SparkAuthFactory authFactory,
      String tokenStr) throws ServiceSQLException;

  void closeExpiredOperations();

  long getNoOperationTime();
}
