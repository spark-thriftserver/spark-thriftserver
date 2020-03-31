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

package org.apache.spark.sql.thriftserver.cli;

import java.util.List;
import java.util.Map;

import org.apache.spark.sql.thriftserver.auth.SparkAuthFactory;
import org.apache.spark.sql.thriftserver.rpc.thrift.TOperationHandle;

public interface ICLIService {

  SessionHandle openSession(String username, String password,
      Map<String, String> configuration)
          throws ServiceSQLException;

  SessionHandle openSessionWithImpersonation(String username, String password,
      Map<String, String> configuration)
          throws ServiceSQLException;

  void closeSession(SessionHandle sessionHandle)
      throws ServiceSQLException;

  GetInfoValue getInfo(SessionHandle sessionHandle, GetInfoType infoType)
      throws ServiceSQLException;

  OperationHandle executeStatement(SessionHandle sessionHandle, String statement,
      Map<String, String> confOverlay) throws ServiceSQLException;

  OperationHandle executeStatement(SessionHandle sessionHandle, String statement,
      Map<String, String> confOverlay, long queryTimeout) throws ServiceSQLException;

  OperationHandle executeStatementAsync(SessionHandle sessionHandle, String statement,
      Map<String, String> confOverlay) throws ServiceSQLException;
  OperationHandle executeStatementAsync(SessionHandle sessionHandle, String statement,
      Map<String, String> confOverlay, long queryTimeout) throws ServiceSQLException;

  OperationHandle getTypeInfo(SessionHandle sessionHandle)
      throws ServiceSQLException;

  OperationHandle getCatalogs(SessionHandle sessionHandle)
      throws ServiceSQLException;

  OperationHandle getSchemas(SessionHandle sessionHandle,
      String catalogName, String schemaName)
          throws ServiceSQLException;

  OperationHandle getTables(SessionHandle sessionHandle,
      String catalogName, String schemaName, String tableName, List<String> tableTypes)
          throws ServiceSQLException;

  OperationHandle getTableTypes(SessionHandle sessionHandle)
      throws ServiceSQLException;

  OperationHandle getColumns(SessionHandle sessionHandle,
      String catalogName, String schemaName, String tableName, String columnName)
          throws ServiceSQLException;

  OperationHandle getFunctions(SessionHandle sessionHandle,
      String catalogName, String schemaName, String functionName)
          throws ServiceSQLException;

  OperationStatus getOperationStatus(OperationHandle opHandle)
      throws ServiceSQLException;

  String getQueryId(TOperationHandle operationHandle) throws ServiceSQLException;

  void cancelOperation(OperationHandle opHandle)
      throws ServiceSQLException;

  void closeOperation(OperationHandle opHandle)
      throws ServiceSQLException;

  TableSchema getResultSetMetadata(OperationHandle opHandle)
      throws ServiceSQLException;

  RowSet fetchResults(OperationHandle opHandle)
      throws ServiceSQLException;

  RowSet fetchResults(OperationHandle opHandle, FetchOrientation orientation,
      long maxRows, FetchType fetchType) throws ServiceSQLException;

  String getDelegationToken(SessionHandle sessionHandle, SparkAuthFactory authFactory,
      String owner, String renewer) throws ServiceSQLException;

  void cancelDelegationToken(SessionHandle sessionHandle, SparkAuthFactory authFactory,
      String tokenStr) throws ServiceSQLException;

  void renewDelegationToken(SessionHandle sessionHandle, SparkAuthFactory authFactory,
      String tokenStr) throws ServiceSQLException;

  OperationHandle getPrimaryKeys(SessionHandle sessionHandle, String catalog,
      String schema, String table) throws ServiceSQLException;

  OperationHandle getCrossReference(SessionHandle sessionHandle,
      String primaryCatalog, String primarySchema, String primaryTable,
      String foreignCatalog, String foreignSchema, String foreignTable) throws ServiceSQLException;
}
