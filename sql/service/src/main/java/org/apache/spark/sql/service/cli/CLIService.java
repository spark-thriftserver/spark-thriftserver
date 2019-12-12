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

package org.apache.spark.sql.service.cli;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.security.auth.login.LoginException;

import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.service.CompositeService;
import org.apache.spark.sql.service.ServiceException;
import org.apache.spark.sql.service.SparkThriftServer;
import org.apache.spark.sql.service.auth.SparkAuthFactory;
import org.apache.spark.sql.service.cli.operation.Operation;
import org.apache.spark.sql.service.cli.session.ServiceSession;
import org.apache.spark.sql.service.cli.session.SessionManager;
import org.apache.spark.sql.service.internal.ServiceConf;
import org.apache.spark.sql.service.rpc.thrift.TProtocolVersion;
import org.apache.spark.sql.service.utils.Utils;

/**
 * CLIService.
 *
 */
public class CLIService extends CompositeService implements ICLIService {

  public static final TProtocolVersion SERVER_VERSION;

  static {
    TProtocolVersion[] protocols = TProtocolVersion.values();
    SERVER_VERSION = protocols[protocols.length - 1];
  }

  private final Logger LOG = LoggerFactory.getLogger(CLIService.class.getName());

  private SQLConf sqlConf;
  private SQLContext sqlContext;
  private SessionManager sessionManager;
  private UserGroupInformation serviceUGI;
  private UserGroupInformation httpUGI;
  // The SparkThriftServer instance running this service
  private final SparkThriftServer sparkServer;

  public CLIService(SparkThriftServer sparkServer, SQLContext sqlContext) {
    super(CLIService.class.getSimpleName());
    this.sparkServer = sparkServer;
    this.sqlContext = sqlContext;
  }

  @Override
  public synchronized void init(SQLConf sqlConf) {
    this.sqlConf = sqlConf;
    sessionManager = new SessionManager(sqlContext);
    addService(sessionManager);
    //  If the hadoop cluster is secure, do a kerberos login for the service from the keytab
    if (UserGroupInformation.isSecurityEnabled()) {
      try {
        SparkAuthFactory.loginFromKeytab(sqlConf);
        this.serviceUGI = Utils.getUGI();
      } catch (IOException e) {
        throw new ServiceException("Unable to login to kerberos with given principal/keytab", e);
      } catch (LoginException e) {
        throw new ServiceException("Unable to login to kerberos with given principal/keytab", e);
      }

      // Also try creating a UGI object for the SPNego principal
      String principal = sqlConf.getConf(ServiceConf.THRIFTSERVER_SPNEGO_PRINCIPAL());
      String keyTabFile = sqlConf.getConf(ServiceConf.THRIFTSERVER_SPNEGO_KEYTAB());
      if (principal.isEmpty() || keyTabFile.isEmpty()) {
        LOG.info("SPNego httpUGI not created, spNegoPrincipal: " + principal +
            ", ketabFile: " + keyTabFile);
      } else {
        try {
          this.httpUGI = SparkAuthFactory.loginFromSpnegoKeytabAndReturnUGI(sqlConf);
          LOG.info("SPNego httpUGI successfully created.");
        } catch (IOException e) {
          LOG.warn("SPNego httpUGI creation failed: ", e);
        }
      }
    }
    super.init(sqlConf);
  }

  public UserGroupInformation getServiceUGI() {
    return this.serviceUGI;
  }

  public UserGroupInformation getHttpUGI() {
    return this.httpUGI;
  }

  @Override
  public synchronized void start() {
    super.start();
  }

  @Override
  public synchronized void stop() {
    super.stop();
  }

  /**
   * @deprecated  Use {@link #openSession(TProtocolVersion, String, String, String, Map)}
   */
  @Deprecated
  public SessionHandle openSession(TProtocolVersion protocol, String username, String password,
      Map<String, String> configuration) throws ServiceSQLException {
    SessionHandle sessionHandle =
        sessionManager.openSession(protocol, username, password, null,
            configuration, false, null);
    LOG.debug(sessionHandle + ": openSession()");
    return sessionHandle;
  }

  /**
   * @deprecated  Use {@link #openSessionWithImpersonation(TProtocolVersion, String,
   *                              String, String, Map, String)}
   */
  @Deprecated
  public SessionHandle openSessionWithImpersonation(TProtocolVersion protocol, String username,
      String password, Map<String, String> configuration, String delegationToken)
          throws ServiceSQLException {
    SessionHandle sessionHandle = sessionManager.openSession(protocol, username, password,
        null, configuration, true, delegationToken);
    LOG.debug(sessionHandle + ": openSessionWithImpersonation()");
    return sessionHandle;
  }

  public SessionHandle openSession(TProtocolVersion protocol, String username, String password,
      String ipAddress, Map<String, String> configuration) throws ServiceSQLException {
    SessionHandle sessionHandle = sessionManager.openSession(protocol, username, password,
        ipAddress, configuration, false, null);
    LOG.debug(sessionHandle + ": openSession()");
    return sessionHandle;
  }

  public SessionHandle openSessionWithImpersonation(TProtocolVersion protocol, String username,
      String password, String ipAddress, Map<String, String> configuration, String delegationToken)
          throws ServiceSQLException {
    SessionHandle sessionHandle = sessionManager.openSession(protocol, username, password,
        ipAddress, configuration, true, delegationToken);
    LOG.debug(sessionHandle + ": openSession()");
    return sessionHandle;
  }

  /* (non-Javadoc)
   * @see ICLIService#openSession(java.lang.String, java.lang.String, java.util.Map)
   */
  @Override
  public SessionHandle openSession(String username, String password,
      Map<String, String> configuration) throws ServiceSQLException {
    SessionHandle sessionHandle = sessionManager.openSession(SERVER_VERSION, username, password,
        null, configuration, false, null);
    LOG.debug(sessionHandle + ": openSession()");
    return sessionHandle;
  }

  /* (non-Javadoc)
   * @see ICLIService#openSession(java.lang.String, java.lang.String, java.util.Map)
   */
  @Override
  public SessionHandle openSessionWithImpersonation(String username, String password,
      Map<String, String> configuration, String delegationToken) throws ServiceSQLException {
    SessionHandle sessionHandle = sessionManager.openSession(SERVER_VERSION, username, password,
        null, configuration, true, delegationToken);
    LOG.debug(sessionHandle + ": openSession()");
    return sessionHandle;
  }

  /* (non-Javadoc)
   * @see ICLIService#closeSession(SessionHandle)
   */
  @Override
  public void closeSession(SessionHandle sessionHandle)
      throws ServiceSQLException {
    sessionManager.closeSession(sessionHandle);
    LOG.debug(sessionHandle + ": closeSession()");
  }

  /* (non-Javadoc)
   * @see ICLIService#getInfo(SessionHandle, java.util.List)
   */
  @Override
  public GetInfoValue getInfo(SessionHandle sessionHandle, GetInfoType getInfoType)
      throws ServiceSQLException {
    GetInfoValue infoValue = null;
    switch (getInfoType) {
      case CLI_SERVER_NAME:
        infoValue = new GetInfoValue("Spark SQL");
        break;
      case CLI_DBMS_NAME:
        infoValue = new GetInfoValue("Spark SQL");
        break;
      case CLI_DBMS_VER:
        infoValue = new GetInfoValue(sqlContext.sparkContext().version());
        break;
      default:
        infoValue = sessionManager.getSession(sessionHandle)
            .getInfo(getInfoType);
    }
    LOG.debug(sessionHandle + ": getInfo()");
    return infoValue;
  }

  /* (non-Javadoc)
   * @see ICLIService#executeStatement(SessionHandle,
   *  java.lang.String, java.util.Map)
   */
  @Override
  public OperationHandle executeStatement(SessionHandle sessionHandle, String statement,
      Map<String, String> confOverlay) throws ServiceSQLException {
    ServiceSession session = sessionManager.getSession(sessionHandle);
    OperationHandle opHandle = session.executeStatement(statement, confOverlay);
    LOG.debug(sessionHandle + ": executeStatement()");
    return opHandle;
  }

  /**
   * Execute statement on the server with a timeout. This is a blocking call.
   */
  @Override
  public OperationHandle executeStatement(SessionHandle sessionHandle, String statement,
        Map<String, String> confOverlay, long queryTimeout) throws ServiceSQLException {
    ServiceSession session = sessionManager.getSession(sessionHandle);
    OperationHandle opHandle = session.executeStatement(statement, confOverlay, queryTimeout);
    LOG.debug(sessionHandle + ": executeStatement()");
    return opHandle;
  }

  /**
   * Execute statement asynchronously on the server. This is a non-blocking call
   */
  @Override
  public OperationHandle executeStatementAsync(SessionHandle sessionHandle, String statement,
      Map<String, String> confOverlay) throws ServiceSQLException {
    ServiceSession session = sessionManager.getSession(sessionHandle);
    OperationHandle opHandle = session.executeStatementAsync(statement, confOverlay);
    LOG.debug(sessionHandle + ": executeStatementAsync()");
    return opHandle;
  }

  /**
   * Execute statement asynchronously on the server with a timeout. This is a non-blocking call
   */
  @Override
  public OperationHandle executeStatementAsync(SessionHandle sessionHandle, String statement,
      Map<String, String> confOverlay, long queryTimeout) throws ServiceSQLException {
    ServiceSession session = sessionManager.getSession(sessionHandle);
    OperationHandle opHandle = session.executeStatementAsync(statement, confOverlay, queryTimeout);
    LOG.debug(sessionHandle + ": executeStatementAsync()");
    return opHandle;
  }


  /* (non-Javadoc)
   * @see ICLIService#getTypeInfo(SessionHandle)
   */
  @Override
  public OperationHandle getTypeInfo(SessionHandle sessionHandle)
      throws ServiceSQLException {
    OperationHandle opHandle = sessionManager.getSession(sessionHandle)
        .getTypeInfo();
    LOG.debug(sessionHandle + ": getTypeInfo()");
    return opHandle;
  }

  /* (non-Javadoc)
   * @see ICLIService#getCatalogs(SessionHandle)
   */
  @Override
  public OperationHandle getCatalogs(SessionHandle sessionHandle)
      throws ServiceSQLException {
    OperationHandle opHandle = sessionManager.getSession(sessionHandle)
        .getCatalogs();
    LOG.debug(sessionHandle + ": getCatalogs()");
    return opHandle;
  }

  /* (non-Javadoc)
   * @see ICLIService#getSchemas(SessionHandle, java.lang.String, java.lang.String)
   */
  @Override
  public OperationHandle getSchemas(SessionHandle sessionHandle,
      String catalogName, String schemaName)
          throws ServiceSQLException {
    OperationHandle opHandle = sessionManager.getSession(sessionHandle)
        .getSchemas(catalogName, schemaName);
    LOG.debug(sessionHandle + ": getSchemas()");
    return opHandle;
  }

  /* (non-Javadoc)
   * @see ICLIService#getTables(SessionHandle, java.lang.String,
   *                            java.lang.String, java.lang.String, java.util.List)
   */
  @Override
  public OperationHandle getTables(SessionHandle sessionHandle,
      String catalogName, String schemaName, String tableName, List<String> tableTypes)
          throws ServiceSQLException {
    OperationHandle opHandle = sessionManager.getSession(sessionHandle)
        .getTables(catalogName, schemaName, tableName, tableTypes);
    LOG.debug(sessionHandle + ": getTables()");
    return opHandle;
  }

  /* (non-Javadoc)
   * @see ICLIService#getTableTypes(SessionHandle)
   */
  @Override
  public OperationHandle getTableTypes(SessionHandle sessionHandle)
      throws ServiceSQLException {
    OperationHandle opHandle = sessionManager.getSession(sessionHandle)
        .getTableTypes();
    LOG.debug(sessionHandle + ": getTableTypes()");
    return opHandle;
  }

  /* (non-Javadoc)
   * @see ICLIService#getColumns(SessionHandle)
   */
  @Override
  public OperationHandle getColumns(SessionHandle sessionHandle,
      String catalogName, String schemaName, String tableName, String columnName)
          throws ServiceSQLException {
    OperationHandle opHandle = sessionManager.getSession(sessionHandle)
        .getColumns(catalogName, schemaName, tableName, columnName);
    LOG.debug(sessionHandle + ": getColumns()");
    return opHandle;
  }

  /* (non-Javadoc)
   * @see ICLIService#getFunctions(SessionHandle)
   */
  @Override
  public OperationHandle getFunctions(SessionHandle sessionHandle,
      String catalogName, String schemaName, String functionName)
          throws ServiceSQLException {
    OperationHandle opHandle = sessionManager.getSession(sessionHandle)
        .getFunctions(catalogName, schemaName, functionName);
    LOG.debug(sessionHandle + ": getFunctions()");
    return opHandle;
  }

  /* (non-Javadoc)
   * @see ICLIService#getPrimaryKeys(SessionHandle)
   */
  @Override
  public OperationHandle getPrimaryKeys(SessionHandle sessionHandle,
      String catalog, String schema, String table) throws ServiceSQLException {
    OperationHandle opHandle = sessionManager.getSession(sessionHandle)
        .getPrimaryKeys(catalog, schema, table);
    LOG.debug(sessionHandle + ": getPrimaryKeys()");
    return opHandle;
  }

  /* (non-Javadoc)
   * @see ICLIService#getCrossReference(SessionHandle)
   */
  @Override
  public OperationHandle getCrossReference(SessionHandle sessionHandle,
      String primaryCatalog, String primarySchema, String primaryTable, String foreignCatalog,
      String foreignSchema, String foreignTable) throws ServiceSQLException {
    OperationHandle opHandle = sessionManager.getSession(sessionHandle)
        .getCrossReference(primaryCatalog, primarySchema, primaryTable,
         foreignCatalog,
         foreignSchema, foreignTable);
    LOG.debug(sessionHandle + ": getCrossReference()");
    return opHandle;
  }

  /* (non-Javadoc)
   * @see ICLIService#getOperationStatus(OperationHandle)
   */
  @Override
  public OperationStatus getOperationStatus(OperationHandle opHandle)
      throws ServiceSQLException {
    Operation operation = sessionManager.getOperationManager().getOperation(opHandle);
    /**
     * If this is a background operation run asynchronously,
     * we block for a configured duration, before we return
     * (duration: THRIFTSERVER_LONG_POLLING_TIMEOUT).
     * However, if the background operation is complete, we return immediately.
     */
    if (operation.shouldRunAsync()) {
      SQLConf conf = operation.getParentSession().getSQLConf();
      long timeout = (long) conf.getConf(ServiceConf.THRIFTSERVER_LONG_POLLING_TIMEOUT());
      try {
        operation.getBackgroundHandle().get(timeout, TimeUnit.MILLISECONDS);
      } catch (TimeoutException e) {
        // No Op, return to the caller since long polling timeout has expired
        LOG.trace(opHandle + ": Long polling timed out");
      } catch (CancellationException e) {
        // The background operation thread was cancelled
        LOG.trace(opHandle + ": The background operation was cancelled", e);
      } catch (ExecutionException e) {
        // The background operation thread was aborted
        LOG.warn(opHandle + ": The background operation was aborted", e);
      } catch (InterruptedException e) {
        // No op, this thread was interrupted
        // In this case, the call might return sooner than long polling timeout
      }
    }
    OperationStatus opStatus = operation.getStatus();
    LOG.debug(opHandle + ": getOperationStatus()");
    return opStatus;
  }

  public SQLConf getSessionConf(SessionHandle sessionHandle) throws ServiceSQLException {
    return sessionManager.getSession(sessionHandle).getSQLConf();
  }

  /* (non-Javadoc)
   * @see ICLIService#cancelOperation(OperationHandle)
   */
  @Override
  public void cancelOperation(OperationHandle opHandle)
      throws ServiceSQLException {
    sessionManager.getOperationManager().getOperation(opHandle)
    .getParentSession().cancelOperation(opHandle);
    LOG.debug(opHandle + ": cancelOperation()");
  }

  /* (non-Javadoc)
   * @see ICLIService#closeOperation(OperationHandle)
   */
  @Override
  public void closeOperation(OperationHandle opHandle)
      throws ServiceSQLException {
    sessionManager.getOperationManager().getOperation(opHandle)
    .getParentSession().closeOperation(opHandle);
    LOG.debug(opHandle + ": closeOperation");
  }

  /* (non-Javadoc)
   * @see ICLIService#getResultSetMetadata(OperationHandle)
   */
  @Override
  public TableSchema getResultSetMetadata(OperationHandle opHandle)
      throws ServiceSQLException {
    TableSchema tableSchema = sessionManager.getOperationManager()
        .getOperation(opHandle).getParentSession().getResultSetMetadata(opHandle);
    LOG.debug(opHandle + ": getResultSetMetadata()");
    return tableSchema;
  }

  /* (non-Javadoc)
   * @see ICLIService#fetchResults(OperationHandle)
   */
  @Override
  public RowSet fetchResults(OperationHandle opHandle)
      throws ServiceSQLException {
    return fetchResults(opHandle, Operation.DEFAULT_FETCH_ORIENTATION,
        Operation.DEFAULT_FETCH_MAX_ROWS, FetchType.QUERY_OUTPUT);
  }

  @Override
  public RowSet fetchResults(OperationHandle opHandle, FetchOrientation orientation,
                             long maxRows, FetchType fetchType) throws ServiceSQLException {
    RowSet rowSet = sessionManager.getOperationManager().getOperation(opHandle)
        .getParentSession().fetchResults(opHandle, orientation, maxRows, fetchType);
    LOG.debug(opHandle + ": fetchResults()");
    return rowSet;
  }

  @Override
  public String getDelegationToken(SessionHandle sessionHandle, SparkAuthFactory authFactory,
      String owner, String renewer) throws ServiceSQLException {
    String delegationToken = sessionManager.getSession(sessionHandle)
        .getDelegationToken(authFactory, owner, renewer);
    LOG.info(sessionHandle  + ": getDelegationToken()");
    return delegationToken;
  }

  @Override
  public void cancelDelegationToken(SessionHandle sessionHandle, SparkAuthFactory authFactory,
      String tokenStr) throws ServiceSQLException {
    sessionManager.getSession(sessionHandle).cancelDelegationToken(authFactory, tokenStr);
    LOG.info(sessionHandle  + ": cancelDelegationToken()");
  }

  @Override
  public void renewDelegationToken(SessionHandle sessionHandle, SparkAuthFactory authFactory,
      String tokenStr) throws ServiceSQLException {
    sessionManager.getSession(sessionHandle).renewDelegationToken(authFactory, tokenStr);
    LOG.info(sessionHandle  + ": renewDelegationToken()");
  }

  public SessionManager getSessionManager() {
    return sessionManager;
  }
}
