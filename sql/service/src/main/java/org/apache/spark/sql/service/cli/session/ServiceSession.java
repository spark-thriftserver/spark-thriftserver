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

package org.apache.spark.sql.service.cli.session;

import java.util.List;
import java.util.Map;

import org.apache.spark.sql.service.auth.SparkAuthFactory;
import org.apache.spark.sql.service.cli.*;

public interface ServiceSession extends ServiceSessionBase {

  void open(Map<String, String> sessionConfMap) throws Exception;

  /**
   * getInfo operation handler
   * @param getInfoType
   * @return
   * @throws ServiceSQLException
   */
  GetInfoValue getInfo(GetInfoType getInfoType) throws ServiceSQLException;

  /**
   * execute operation handler
   * @param statement
   * @param confOverlay
   * @return
   * @throws ServiceSQLException
   */
  OperationHandle executeStatement(String statement,
                                   Map<String, String> confOverlay) throws ServiceSQLException;

  /**
   * execute operation handler
   * @param statement
   * @param confOverlay
   * @param queryTimeout
   * @return
   * @throws ServiceSQLException
   */
  OperationHandle executeStatement(String statement, Map<String, String> confOverlay,
      long queryTimeout) throws ServiceSQLException;

  /**
   * execute operation handler
   * @param statement
   * @param confOverlay
   * @return
   * @throws ServiceSQLException
   */
  OperationHandle executeStatementAsync(String statement, Map<String, String> confOverlay)
      throws ServiceSQLException;

  /**
   * execute operation handler
   * @param statement
   * @param confOverlay
   * @param queryTimeout
   * @return
   * @throws ServiceSQLException
   */
  OperationHandle executeStatementAsync(String statement, Map<String, String> confOverlay,
      long queryTimeout) throws ServiceSQLException;

  /**
   * getTypeInfo operation handler
   * @return
   * @throws ServiceSQLException
   */
  OperationHandle getTypeInfo() throws ServiceSQLException;

  /**
   * getCatalogs operation handler
   * @return
   * @throws ServiceSQLException
   */
  OperationHandle getCatalogs() throws ServiceSQLException;

  /**
   * getSchemas operation handler
   * @param catalogName
   * @param schemaName
   * @return
   * @throws ServiceSQLException
   */
  OperationHandle getSchemas(String catalogName, String schemaName)
      throws ServiceSQLException;

  /**
   * getTables operation handler
   * @param catalogName
   * @param schemaName
   * @param tableName
   * @param tableTypes
   * @return
   * @throws ServiceSQLException
   */
  OperationHandle getTables(String catalogName, String schemaName,
      String tableName, List<String> tableTypes) throws ServiceSQLException;

  /**
   * getTableTypes operation handler
   * @return
   * @throws ServiceSQLException
   */
  OperationHandle getTableTypes() throws ServiceSQLException;

  /**
   * getColumns operation handler
   * @param catalogName
   * @param schemaName
   * @param tableName
   * @param columnName
   * @return
   * @throws ServiceSQLException
   */
  OperationHandle getColumns(String catalogName, String schemaName,
      String tableName, String columnName)  throws ServiceSQLException;

  /**
   * getFunctions operation handler
   * @param catalogName
   * @param schemaName
   * @param functionName
   * @return
   * @throws ServiceSQLException
   */
  OperationHandle getFunctions(String catalogName, String schemaName,
      String functionName) throws ServiceSQLException;

  /**
   * getPrimaryKeys operation handler
   * @param catalog
   * @param schema
   * @param table
   * @return
   * @throws ServiceSQLException
   */
  OperationHandle getPrimaryKeys(String catalog, String schema,
      String table) throws ServiceSQLException;


  /**
   * getCrossReference operation handler
   * @param primaryCatalog
   * @param primarySchema
   * @param primaryTable
   * @param foreignCatalog
   * @param foreignSchema
   * @param foreignTable
   * @return
   * @throws ServiceSQLException
   */
  OperationHandle getCrossReference(String primaryCatalog,
      String primarySchema, String primaryTable, String foreignCatalog,
      String foreignSchema, String foreignTable) throws ServiceSQLException;

  /**
   * close the session
   * @throws ServiceSQLException
   */
  void close() throws ServiceSQLException;

  void cancelOperation(OperationHandle opHandle) throws ServiceSQLException;

  void closeOperation(OperationHandle opHandle) throws ServiceSQLException;

  TableSchema getResultSetMetadata(OperationHandle opHandle)
      throws ServiceSQLException;

  RowSet fetchResults(OperationHandle opHandle, FetchOrientation orientation,
                      long maxRows, FetchType fetchType) throws ServiceSQLException;

  String getDelegationToken(SparkAuthFactory authFactory, String owner,
                            String renewer) throws ServiceSQLException;

  void cancelDelegationToken(SparkAuthFactory authFactory, String tokenStr)
      throws ServiceSQLException;

  void renewDelegationToken(SparkAuthFactory authFactory, String tokenStr)
      throws ServiceSQLException;

  void closeExpiredOperations();

  long getNoOperationTime();
}
