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

package org.apache.spark.sql.service.cli.thrift;

import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;

import org.apache.spark.sql.service.auth.SparkAuthFactory;
import org.apache.spark.sql.service.cli.*;
import org.apache.spark.sql.service.rpc.thrift.*;

/**
 * ThriftCLIServiceClient.
 *
 */
public class ThriftCLIServiceClient extends CLIServiceClient {
  private final TCLIService.Iface cliService;

  public ThriftCLIServiceClient(TCLIService.Iface cliService) {
    this.cliService = cliService;
  }

  public void checkStatus(TStatus status) throws ServiceSQLException {
    if (TStatusCode.ERROR_STATUS.equals(status.getStatusCode())) {
      throw new ServiceSQLException(status);
    }
  }

  /* (non-Javadoc)
   * @see ICLIService#openSession(java.lang.String, java.lang.String, java.util.Map)
   */
  @Override
  public SessionHandle openSession(String username, String password,
      Map<String, String> configuration)
          throws ServiceSQLException {
    try {
      TOpenSessionReq req = new TOpenSessionReq();
      req.setUsername(username);
      req.setPassword(password);
      req.setConfiguration(configuration);
      TOpenSessionResp resp = cliService.OpenSession(req);
      checkStatus(resp.getStatus());
      return new SessionHandle(resp.getSessionHandle(), resp.getServerProtocolVersion());
    } catch (ServiceSQLException e) {
      throw e;
    } catch (Exception e) {
      throw new ServiceSQLException(e);
    }
  }

  /* (non-Javadoc)
   * @see ICLIService#closeSession(SessionHandle)
   */
  @Override
  public SessionHandle openSessionWithImpersonation(String username, String password,
      Map<String, String> configuration, String delegationToken) throws ServiceSQLException {
    throw new ServiceSQLException("open with impersonation operation " +
        "is not supported in the client");
  }

  /* (non-Javadoc)
   * @see ICLIService#closeSession(SessionHandle)
   */
  @Override
  public void closeSession(SessionHandle sessionHandle) throws ServiceSQLException {
    try {
      TCloseSessionReq req = new TCloseSessionReq(sessionHandle.toTSessionHandle());
      TCloseSessionResp resp = cliService.CloseSession(req);
      checkStatus(resp.getStatus());
    } catch (ServiceSQLException e) {
      throw e;
    } catch (Exception e) {
      throw new ServiceSQLException(e);
    }
  }

  /* (non-Javadoc)
   * @see ICLIService#getInfo(SessionHandle, java.util.List)
   */
  @Override
  public GetInfoValue getInfo(SessionHandle sessionHandle, GetInfoType infoType)
      throws ServiceSQLException {
    try {
      // FIXME extract the right info type
      TGetInfoReq req =
          new TGetInfoReq(sessionHandle.toTSessionHandle(), infoType.toTGetInfoType());
      TGetInfoResp resp = cliService.GetInfo(req);
      checkStatus(resp.getStatus());
      return new GetInfoValue(resp.getInfoValue());
    } catch (ServiceSQLException e) {
      throw e;
    } catch (Exception e) {
      throw new ServiceSQLException(e);
    }
  }

  /* (non-Javadoc)
   * @see ICLIService#executeStatement(SessionHandle, java.lang.String, java.util.Map)
   */
  @Override
  public OperationHandle executeStatement(SessionHandle sessionHandle, String statement,
      Map<String, String> confOverlay) throws ServiceSQLException {
    return executeStatementInternal(sessionHandle, statement, confOverlay, false, 0);
  }

  @Override
  public OperationHandle executeStatement(SessionHandle sessionHandle, String statement,
      Map<String, String> confOverlay, long queryTimeout) throws ServiceSQLException {
    return executeStatementInternal(sessionHandle, statement, confOverlay, false, queryTimeout);
  }

  @Override
  public OperationHandle executeStatementAsync(SessionHandle sessionHandle, String statement,
      Map<String, String> confOverlay) throws ServiceSQLException {
    return executeStatementInternal(sessionHandle, statement, confOverlay, true, 0);
  }

  /* (non-Javadoc)
   * @see ICLIService#executeStatementAsync(SessionHandle, java.lang.String, java.util.Map)
   */
  @Override
  public OperationHandle executeStatementAsync(SessionHandle sessionHandle, String statement,
      Map<String, String> confOverlay, long queryTimeout) throws ServiceSQLException {
    return executeStatementInternal(sessionHandle, statement, confOverlay, true, queryTimeout);
  }

  private OperationHandle executeStatementInternal(SessionHandle sessionHandle, String statement,
      Map<String, String> confOverlay, boolean isAsync, long queryTimeout)
      throws ServiceSQLException {
    try {
      TExecuteStatementReq req =
          new TExecuteStatementReq(sessionHandle.toTSessionHandle(), statement);
      req.setConfOverlay(confOverlay);
      req.setRunAsync(isAsync);
      req.setQueryTimeout(queryTimeout);
      TExecuteStatementResp resp = cliService.ExecuteStatement(req);
      checkStatus(resp.getStatus());
      TProtocolVersion protocol = sessionHandle.getProtocolVersion();
      return new OperationHandle(resp.getOperationHandle(), protocol);
    } catch (ServiceSQLException e) {
      throw e;
    } catch (Exception e) {
      throw new ServiceSQLException(e);
    }
  }

  /* (non-Javadoc)
   * @see ICLIService#getTypeInfo(SessionHandle)
   */
  @Override
  public OperationHandle getTypeInfo(SessionHandle sessionHandle) throws ServiceSQLException {
    try {
      TGetTypeInfoReq req = new TGetTypeInfoReq(sessionHandle.toTSessionHandle());
      TGetTypeInfoResp resp = cliService.GetTypeInfo(req);
      checkStatus(resp.getStatus());
      TProtocolVersion protocol = sessionHandle.getProtocolVersion();
      return new OperationHandle(resp.getOperationHandle(), protocol);
    } catch (ServiceSQLException e) {
      throw e;
    } catch (Exception e) {
      throw new ServiceSQLException(e);
    }
  }

  /* (non-Javadoc)
   * @see ICLIService#getCatalogs(SessionHandle)
   */
  @Override
  public OperationHandle getCatalogs(SessionHandle sessionHandle) throws ServiceSQLException {
    try {
      TGetCatalogsReq req = new TGetCatalogsReq(sessionHandle.toTSessionHandle());
      TGetCatalogsResp resp = cliService.GetCatalogs(req);
      checkStatus(resp.getStatus());
      TProtocolVersion protocol = sessionHandle.getProtocolVersion();
      return new OperationHandle(resp.getOperationHandle(), protocol);
    } catch (ServiceSQLException e) {
      throw e;
    } catch (Exception e) {
      throw new ServiceSQLException(e);
    }
  }

  /* (non-Javadoc)
   * @see ICLIService#getSchemas(SessionHandle, java.lang.String, java.lang.String)
   */
  @Override
  public OperationHandle getSchemas(SessionHandle sessionHandle, String catalogName,
      String schemaName)
          throws ServiceSQLException {
    try {
      TGetSchemasReq req = new TGetSchemasReq(sessionHandle.toTSessionHandle());
      req.setCatalogName(catalogName);
      req.setSchemaName(schemaName);
      TGetSchemasResp resp = cliService.GetSchemas(req);
      checkStatus(resp.getStatus());
      TProtocolVersion protocol = sessionHandle.getProtocolVersion();
      return new OperationHandle(resp.getOperationHandle(), protocol);
    } catch (ServiceSQLException e) {
      throw e;
    } catch (Exception e) {
      throw new ServiceSQLException(e);
    }
  }

  /* (non-Javadoc)
   * @see ICLIService#getTables(SessionHandle,
   *                            java.lang.String,
   *                            java.lang.String,
   *                            java.lang.String,
   *                            java.util.List)
   */
  @Override
  public OperationHandle getTables(SessionHandle sessionHandle, String catalogName,
      String schemaName, String tableName, List<String> tableTypes)
          throws ServiceSQLException {
    try {
      TGetTablesReq req = new TGetTablesReq(sessionHandle.toTSessionHandle());
      req.setTableName(tableName);
      req.setTableTypes(tableTypes);
      req.setSchemaName(schemaName);
      TGetTablesResp resp = cliService.GetTables(req);
      checkStatus(resp.getStatus());
      TProtocolVersion protocol = sessionHandle.getProtocolVersion();
      return new OperationHandle(resp.getOperationHandle(), protocol);
    } catch (ServiceSQLException e) {
      throw e;
    } catch (Exception e) {
      throw new ServiceSQLException(e);
    }
  }

  /* (non-Javadoc)
   * @see ICLIService#getTableTypes(SessionHandle)
   */
  @Override
  public OperationHandle getTableTypes(SessionHandle sessionHandle) throws ServiceSQLException {
    try {
      TGetTableTypesReq req = new TGetTableTypesReq(sessionHandle.toTSessionHandle());
      TGetTableTypesResp resp = cliService.GetTableTypes(req);
      checkStatus(resp.getStatus());
      TProtocolVersion protocol = sessionHandle.getProtocolVersion();
      return new OperationHandle(resp.getOperationHandle(), protocol);
    } catch (ServiceSQLException e) {
      throw e;
    } catch (Exception e) {
      throw new ServiceSQLException(e);
    }
  }

  /* (non-Javadoc)
   * @see ICLIService#getColumns(SessionHandle)
   */
  @Override
  public OperationHandle getColumns(SessionHandle sessionHandle,
      String catalogName, String schemaName, String tableName, String columnName)
          throws ServiceSQLException {
    try {
      TGetColumnsReq req = new TGetColumnsReq();
      req.setSessionHandle(sessionHandle.toTSessionHandle());
      req.setCatalogName(catalogName);
      req.setSchemaName(schemaName);
      req.setTableName(tableName);
      req.setColumnName(columnName);
      TGetColumnsResp resp = cliService.GetColumns(req);
      checkStatus(resp.getStatus());
      TProtocolVersion protocol = sessionHandle.getProtocolVersion();
      return new OperationHandle(resp.getOperationHandle(), protocol);
    } catch (ServiceSQLException e) {
      throw e;
    } catch (Exception e) {
      throw new ServiceSQLException(e);
    }
  }

  /* (non-Javadoc)
   * @see ICLIService#getFunctions(SessionHandle)
   */
  @Override
  public OperationHandle getFunctions(SessionHandle sessionHandle,
      String catalogName, String schemaName, String functionName) throws ServiceSQLException {
    try {
      TGetFunctionsReq req = new TGetFunctionsReq(sessionHandle.toTSessionHandle(), functionName);
      req.setCatalogName(catalogName);
      req.setSchemaName(schemaName);
      TGetFunctionsResp resp = cliService.GetFunctions(req);
      checkStatus(resp.getStatus());
      TProtocolVersion protocol = sessionHandle.getProtocolVersion();
      return new OperationHandle(resp.getOperationHandle(), protocol);
    } catch (ServiceSQLException e) {
      throw e;
    } catch (Exception e) {
      throw new ServiceSQLException(e);
    }
  }

  /* (non-Javadoc)
   * @see ICLIService#getOperationStatus(OperationHandle)
   */
  @Override
  public OperationStatus getOperationStatus(OperationHandle opHandle) throws ServiceSQLException {
    try {
      TGetOperationStatusReq req = new TGetOperationStatusReq(opHandle.toTOperationHandle());
      TGetOperationStatusResp resp = cliService.GetOperationStatus(req);
      // Checks the status of the RPC call, throws an exception in case of error
      checkStatus(resp.getStatus());
      OperationState opState = OperationState.getOperationState(resp.getOperationState());
      ServiceSQLException opException = null;
      if (opState == OperationState.ERROR) {
        opException = new ServiceSQLException(resp.getErrorMessage(),
            resp.getSqlState(), resp.getErrorCode());
      }
      return new OperationStatus(opState, opException);
    } catch (ServiceSQLException e) {
      throw e;
    } catch (Exception e) {
      throw new ServiceSQLException(e);
    }
  }

  /* (non-Javadoc)
   * @see ICLIService#cancelOperation(OperationHandle)
   */
  @Override
  public void cancelOperation(OperationHandle opHandle) throws ServiceSQLException {
    try {
      TCancelOperationReq req = new TCancelOperationReq(opHandle.toTOperationHandle());
      TCancelOperationResp resp = cliService.CancelOperation(req);
      checkStatus(resp.getStatus());
    } catch (ServiceSQLException e) {
      throw e;
    } catch (Exception e) {
      throw new ServiceSQLException(e);
    }
  }

  /* (non-Javadoc)
   * @see ICLIService#closeOperation(OperationHandle)
   */
  @Override
  public void closeOperation(OperationHandle opHandle)
      throws ServiceSQLException {
    try {
      TCloseOperationReq req  = new TCloseOperationReq(opHandle.toTOperationHandle());
      TCloseOperationResp resp = cliService.CloseOperation(req);
      checkStatus(resp.getStatus());
    } catch (ServiceSQLException e) {
      throw e;
    } catch (Exception e) {
      throw new ServiceSQLException(e);
    }
  }

  /* (non-Javadoc)
   * @see ICLIService#getResultSetMetadata(OperationHandle)
   */
  @Override
  public TableSchema getResultSetMetadata(OperationHandle opHandle)
      throws ServiceSQLException {
    try {
      TGetResultSetMetadataReq req = new TGetResultSetMetadataReq(opHandle.toTOperationHandle());
      TGetResultSetMetadataResp resp = cliService.GetResultSetMetadata(req);
      checkStatus(resp.getStatus());
      return new TableSchema(resp.getSchema());
    } catch (ServiceSQLException e) {
      throw e;
    } catch (Exception e) {
      throw new ServiceSQLException(e);
    }
  }

  @Override
  public RowSet fetchResults(OperationHandle opHandle, FetchOrientation orientation, long maxRows,
                             FetchType fetchType) throws ServiceSQLException {
    try {
      TFetchResultsReq req = new TFetchResultsReq();
      req.setOperationHandle(opHandle.toTOperationHandle());
      req.setOrientation(orientation.toTFetchOrientation());
      req.setMaxRows(maxRows);
      req.setFetchType(fetchType.toTFetchType());
      TFetchResultsResp resp = cliService.FetchResults(req);
      checkStatus(resp.getStatus());
      return RowSetFactory.create(resp.getResults(), opHandle.getProtocolVersion());
    } catch (ServiceSQLException e) {
      throw e;
    } catch (Exception e) {
      throw new ServiceSQLException(e);
    }
  }

  /* (non-Javadoc)
   * @see ICLIService#fetchResults(OperationHandle)
   */
  @Override
  public RowSet fetchResults(OperationHandle opHandle) throws ServiceSQLException {
    // TODO: set the correct default fetch size
    return fetchResults(opHandle, FetchOrientation.FETCH_NEXT, 10000, FetchType.QUERY_OUTPUT);
  }

  @Override
  public String getDelegationToken(SessionHandle sessionHandle, SparkAuthFactory authFactory,
      String owner, String renewer) throws ServiceSQLException {
    TGetDelegationTokenReq req = new TGetDelegationTokenReq(
        sessionHandle.toTSessionHandle(), owner, renewer);
    try {
      TGetDelegationTokenResp tokenResp = cliService.GetDelegationToken(req);
      checkStatus(tokenResp.getStatus());
      return tokenResp.getDelegationToken();
    } catch (Exception e) {
      throw new ServiceSQLException(e);
    }
  }

  @Override
  public void cancelDelegationToken(SessionHandle sessionHandle, SparkAuthFactory authFactory,
      String tokenStr) throws ServiceSQLException {
    TCancelDelegationTokenReq cancelReq = new TCancelDelegationTokenReq(
          sessionHandle.toTSessionHandle(), tokenStr);
    try {
      TCancelDelegationTokenResp cancelResp =
        cliService.CancelDelegationToken(cancelReq);
      checkStatus(cancelResp.getStatus());
      return;
    } catch (TException e) {
      throw new ServiceSQLException(e);
    }
  }

  @Override
  public void renewDelegationToken(SessionHandle sessionHandle, SparkAuthFactory authFactory,
      String tokenStr) throws ServiceSQLException {
    TRenewDelegationTokenReq cancelReq = new TRenewDelegationTokenReq(
        sessionHandle.toTSessionHandle(), tokenStr);
    try {
      TRenewDelegationTokenResp renewResp =
        cliService.RenewDelegationToken(cancelReq);
      checkStatus(renewResp.getStatus());
      return;
    } catch (Exception e) {
      throw new ServiceSQLException(e);
    }
  }

  @Override
  public OperationHandle getPrimaryKeys(SessionHandle sessionHandle,
      String catalog, String schema, String table) throws ServiceSQLException {
    try {
      TGetPrimaryKeysReq req = new TGetPrimaryKeysReq(sessionHandle.toTSessionHandle());
      req.setCatalogName(catalog);
      req.setSchemaName(schema);
      req.setTableName(table);
      TGetPrimaryKeysResp resp = cliService.GetPrimaryKeys(req);
      checkStatus(resp.getStatus());
      TProtocolVersion protocol = sessionHandle.getProtocolVersion();
      return new OperationHandle(resp.getOperationHandle(), protocol);
    } catch (ServiceSQLException e) {
      throw e;
    } catch (Exception e) {
      throw new ServiceSQLException(e);
    }
  }

  @Override
  public OperationHandle getCrossReference(SessionHandle sessionHandle,
      String primaryCatalog, String primarySchema, String primaryTable,
      String foreignCatalog, String foreignSchema, String foreignTable) throws ServiceSQLException {
    try {
      TGetCrossReferenceReq req = new TGetCrossReferenceReq(sessionHandle.toTSessionHandle());
      req.setParentCatalogName(primaryCatalog);
      req.setParentSchemaName(primarySchema);
      req.setParentTableName(primaryTable);
      req.setForeignCatalogName(foreignCatalog);
      req.setForeignSchemaName(foreignSchema);
      req.setForeignTableName(foreignTable);
      TGetCrossReferenceResp resp = cliService.GetCrossReference(req);
      checkStatus(resp.getStatus());
      TProtocolVersion protocol = sessionHandle.getProtocolVersion();
      return new OperationHandle(resp.getOperationHandle(), protocol);
    } catch (ServiceSQLException e) {
      throw e;
    } catch (Exception e) {
      throw new ServiceSQLException(e);
    }
  }
}
