/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.service.cli.thrift;

import javax.security.auth.login.LoginException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.service.AbstractService;
import org.apache.spark.sql.service.ServiceException;
import org.apache.spark.sql.service.ServiceUtils;
import org.apache.spark.sql.service.SparkThriftServer;
import org.apache.spark.sql.service.auth.SparkAuthFactory;
import org.apache.spark.sql.service.auth.TSetIpAddressProcessor;
import org.apache.spark.sql.service.cli.*;
import org.apache.spark.sql.service.cli.session.SessionManager;
import org.apache.spark.sql.service.internal.ServiceConf;
import org.apache.spark.sql.service.rpc.thrift.*;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.ServerContext;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServerEventHandler;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ThriftCLIService.
 *
 */
public abstract class ThriftCLIService extends AbstractService
    implements TCLIService.Iface, Runnable {

  public static final Logger LOG = LoggerFactory.getLogger(ThriftCLIService.class.getName());

  protected CLIService cliService;
  private static final TStatus OK_STATUS = new TStatus(TStatusCode.SUCCESS_STATUS);
  protected static SparkAuthFactory sparkAuthFactory;

  protected int portNum;
  protected InetAddress serverIPAddress;
  protected String sparkHost;
  protected TServer server;
  protected org.eclipse.jetty.server.Server httpServer;

  private boolean isStarted = false;
  protected boolean isEmbedded = false;

  protected SQLConf sqlConf;
  protected SQLContext sqlContext;

  protected int minWorkerThreads;
  protected int maxWorkerThreads;
  protected long workerKeepAliveTime;

  protected TServerEventHandler serverEventHandler;
  protected ThreadLocal<ServerContext> currentServerContext;

  static class ThriftCLIServerContext implements ServerContext {
    private SessionHandle sessionHandle = null;

    public void setSessionHandle(SessionHandle sessionHandle) {
      this.sessionHandle = sessionHandle;
    }

    public SessionHandle getSessionHandle() {
      return sessionHandle;
    }
  }

  public ThriftCLIService(CLIService service, SQLContext sqlContext, String serviceName) {
    super(serviceName);
    this.cliService = service;
    this.sqlContext = sqlContext;
    currentServerContext = new ThreadLocal<ServerContext>();
    serverEventHandler = new TServerEventHandler() {
      @Override
      public ServerContext createContext(
          TProtocol input, TProtocol output) {
        return new ThriftCLIServerContext();
      }

      @Override
      public void deleteContext(ServerContext serverContext,
          TProtocol input, TProtocol output) {
        ThriftCLIServerContext context = (ThriftCLIServerContext)serverContext;
        SessionHandle sessionHandle = context.getSessionHandle();
        if (sessionHandle != null) {
          LOG.info("Session disconnected without closing properly, close it now");
          try {
            cliService.closeSession(sessionHandle);
          } catch (ServiceSQLException e) {
            LOG.warn("Failed to close session: " + e, e);
          }
        }
      }

      @Override
      public void preServe() {
      }

      @Override
      public void processContext(ServerContext serverContext,
          TTransport input, TTransport output) {
        currentServerContext.set(serverContext);
      }
    };
  }

  @Override
  public synchronized void init(SQLConf sqlConf) {
    this.sqlConf = sqlConf;
    // Initialize common server configs needed in both binary & http modes
    String portString;
    sparkHost = System.getenv("THRIFTSERVER_THRIFT_BIND_HOST");
    if (sparkHost == null) {
      sparkHost = sqlConf.getConf(ServiceConf.THRIFTSERVER_THRIFT_BIND_HOST());
    }
    try {
      if (sparkHost != null && !sparkHost.isEmpty()) {
        serverIPAddress = InetAddress.getByName(sparkHost);
      } else {
        serverIPAddress = InetAddress.getLocalHost();
      }
    } catch (UnknownHostException e) {
      throw new ServiceException(e);
    }
    if (SparkThriftServer.isHTTPTransportMode(sqlConf)) {
      // HTTP mode
      workerKeepAliveTime =
          (long) sqlConf.getConf(ServiceConf.THRIFTSERVER_THRIFT_HTTP_WORKER_KEEPALIVE_TIME());
      portString = System.getenv("THRIFTSERVER_THRIFT_HTTP_PORT");
      if (portString != null) {
        portNum = Integer.valueOf(portString);
      } else {
        portNum = (int) sqlConf.getConf(ServiceConf.THRIFTSERVER_HTTP_PORT());
      }
    } else {
      // Binary mode
      workerKeepAliveTime =
          (long) sqlConf.getConf(ServiceConf.THRIFTSERVER_THRIFT_WORKER_KEEPALIVE_TIME());
      portString = System.getenv("THRIFTSERVER_THRIFT_PORT");
      if (portString != null) {
        portNum = Integer.valueOf(portString);
      } else {
        portNum = (int) sqlConf.getConf(ServiceConf.THRIFTSERVER_THRIFT_PORT());
      }
    }
    minWorkerThreads = (int) sqlConf.getConf(ServiceConf.THRIFTSERVER_THRIFT_MIN_WORKER_THREADS());
    maxWorkerThreads = (int) sqlConf.getConf(ServiceConf.THRIFTSERVER_THRIFT_MAX_WORKER_THREADS());
    super.init(sqlConf);
  }

  @Override
  public synchronized void start() {
    super.start();
    if (!isStarted && !isEmbedded) {
      new Thread(this).start();
      isStarted = true;
    }
  }

  @Override
  public synchronized void stop() {
    if (isStarted && !isEmbedded) {
      if(server != null) {
        server.stop();
        LOG.info("Thrift server has stopped");
      }
      if((httpServer != null) && httpServer.isStarted()) {
        try {
          httpServer.stop();
          LOG.info("Http server has stopped");
        } catch (Exception e) {
          LOG.error("Error stopping Http server: ", e);
        }
      }
      isStarted = false;
    }
    super.stop();
  }

  public int getPortNumber() {
    return portNum;
  }

  public InetAddress getServerIPAddress() {
    return serverIPAddress;
  }

  @Override
  public TGetDelegationTokenResp GetDelegationToken(TGetDelegationTokenReq req)
      throws TException {
    TGetDelegationTokenResp resp = new TGetDelegationTokenResp();

    if (sparkAuthFactory == null || !sparkAuthFactory.isSASLKerberosUser()) {
      resp.setStatus(unsecureTokenErrorStatus());
    } else {
      try {
        String token = cliService.getDelegationToken(
            new SessionHandle(req.getSessionHandle()),
            sparkAuthFactory, req.getOwner(), req.getRenewer());
        resp.setDelegationToken(token);
        resp.setStatus(OK_STATUS);
      } catch (ServiceSQLException e) {
        LOG.error("Error obtaining delegation token", e);
        TStatus tokenErrorStatus = ServiceSQLException.toTStatus(e);
        tokenErrorStatus.setSqlState("42000");
        resp.setStatus(tokenErrorStatus);
      }
    }
    return resp;
  }

  @Override
  public TCancelDelegationTokenResp CancelDelegationToken(TCancelDelegationTokenReq req)
      throws TException {
    TCancelDelegationTokenResp resp = new TCancelDelegationTokenResp();

    if (sparkAuthFactory == null || !sparkAuthFactory.isSASLKerberosUser()) {
      resp.setStatus(unsecureTokenErrorStatus());
    } else {
      try {
        cliService.cancelDelegationToken(new SessionHandle(req.getSessionHandle()),
            sparkAuthFactory, req.getDelegationToken());
        resp.setStatus(OK_STATUS);
      } catch (ServiceSQLException e) {
        LOG.error("Error canceling delegation token", e);
        resp.setStatus(ServiceSQLException.toTStatus(e));
      }
    }
    return resp;
  }

  @Override
  public TRenewDelegationTokenResp RenewDelegationToken(TRenewDelegationTokenReq req)
      throws TException {
    TRenewDelegationTokenResp resp = new TRenewDelegationTokenResp();
    if (sparkAuthFactory == null || !sparkAuthFactory.isSASLKerberosUser()) {
      resp.setStatus(unsecureTokenErrorStatus());
    } else {
      try {
        cliService.renewDelegationToken(new SessionHandle(req.getSessionHandle()),
            sparkAuthFactory, req.getDelegationToken());
        resp.setStatus(OK_STATUS);
      } catch (ServiceSQLException e) {
        LOG.error("Error obtaining renewing token", e);
        resp.setStatus(ServiceSQLException.toTStatus(e));
      }
    }
    return resp;
  }

  private TStatus unsecureTokenErrorStatus() {
    TStatus errorStatus = new TStatus(TStatusCode.ERROR_STATUS);
    errorStatus.setErrorMessage("Delegation token only supported over remote " +
        "client with kerberos authentication");
    return errorStatus;
  }

  private TStatus notSupportTokenErrorStatus() {
    TStatus errorStatus = new TStatus(TStatusCode.ERROR_STATUS);
    errorStatus.setErrorMessage("Delegation token is not supported");
    return errorStatus;
  }

  @Override
  public TOpenSessionResp OpenSession(TOpenSessionReq req) throws TException {
    LOG.info("Client protocol version: " + req.getClient_protocol());
    TOpenSessionResp resp = new TOpenSessionResp();
    try {
      SessionHandle sessionHandle = getSessionHandle(req, resp);
      resp.setSessionHandle(sessionHandle.toTSessionHandle());
      // TODO: set real configuration map
      resp.setConfiguration(new HashMap<String, String>());
      resp.setStatus(OK_STATUS);
      ThriftCLIServerContext context =
        (ThriftCLIServerContext)currentServerContext.get();
      if (context != null) {
        context.setSessionHandle(sessionHandle);
      }
    } catch (Exception e) {
      LOG.warn("Error opening session: ", e);
      resp.setStatus(ServiceSQLException.toTStatus(e));
    }
    return resp;
  }

  private String getIpAddress() {
    String clientIpAddress;
    // Http transport mode.
    // We set the thread local ip address, in ThriftHttpServlet.
    if (cliService.getSqlConf().getConf(
        ServiceConf.THRIFTSERVER_TRANSPORT_MODE()).equalsIgnoreCase("http")) {
      clientIpAddress = SessionManager.getIpAddress();
    }
    else {
      // Kerberos
      if (isKerberosAuthMode()) {
        clientIpAddress = sparkAuthFactory.getIpAddress();
      }
      // Except kerberos, NOSASL
      else {
        clientIpAddress = TSetIpAddressProcessor.getUserIpAddress();
      }
    }
    LOG.debug("Client's IP Address: " + clientIpAddress);
    return clientIpAddress;
  }

  /**
   * Returns the effective username.
   * 1. If spark.sql.thriftserver.allow.user.substitution = false:
   *          the username of the connecting user
   * 2. If spark.sql.thriftserver.allow.user.substitution = true:
   *          the username of the end user,
   * that the connecting user is trying to proxy for.
   * This includes a check whether the connecting user is allowed to proxy for the end user.
   * @param req
   * @return
   * @throws ServiceSQLException
   */
  private String getUserName(TOpenSessionReq req) throws ServiceSQLException {
    String userName = null;
    // Kerberos
    if (isKerberosAuthMode()) {
      userName = sparkAuthFactory.getRemoteUser();
    }
    // Except kerberos, NOSASL
    if (userName == null) {
      userName = TSetIpAddressProcessor.getUserName();
    }
    // Http transport mode.
    // We set the thread local username, in ThriftHttpServlet.
    if (cliService.getSqlConf().getConf(
            ServiceConf.THRIFTSERVER_TRANSPORT_MODE()).equalsIgnoreCase("http")) {
      userName = SessionManager.getUserName();
    }
    if (userName == null) {
      userName = req.getUsername();
    }

    userName = getShortName(userName);
    String effectiveClientUser = getProxyUser(userName, req.getConfiguration(), getIpAddress());
    LOG.debug("Client's username: " + effectiveClientUser);
    return effectiveClientUser;
  }

  private String getShortName(String userName) {
    String ret = null;
    if (userName != null) {
      int indexOfDomainMatch = ServiceUtils.indexOfDomainMatch(userName);
      ret = (indexOfDomainMatch <= 0) ? userName :
          userName.substring(0, indexOfDomainMatch);
    }

    return ret;
  }

  /**
   * Create a session handle
   * @param req
   * @param res
   * @return
   * @throws ServiceSQLException
   * @throws LoginException
   * @throws IOException
   */
  SessionHandle getSessionHandle(TOpenSessionReq req, TOpenSessionResp res)
      throws ServiceSQLException, LoginException, IOException {
    String userName = getUserName(req);
    String ipAddress = getIpAddress();
    TProtocolVersion protocol = getMinVersion(CLIService.SERVER_VERSION,
        req.getClient_protocol());
    res.setServerProtocolVersion(protocol);
    SessionHandle sessionHandle;
    if (((boolean) cliService.getSqlConf().getConf(ServiceConf.THRIFTSERVER_ENABLE_DOAS())) &&
        (userName != null)) {
      String delegationTokenStr = "";
      sessionHandle = cliService.openSessionWithImpersonation(protocol, userName,
          req.getPassword(), ipAddress, req.getConfiguration(), delegationTokenStr);
    } else {
      sessionHandle = cliService.openSession(protocol, userName, req.getPassword(),
          ipAddress, req.getConfiguration());
    }
    return sessionHandle;
  }

  private TProtocolVersion getMinVersion(TProtocolVersion... versions) {
    TProtocolVersion[] values = TProtocolVersion.values();
    int current = values[values.length - 1].getValue();
    for (TProtocolVersion version : versions) {
      if (current > version.getValue()) {
        current = version.getValue();
      }
    }
    for (TProtocolVersion version : values) {
      if (version.getValue() == current) {
        return version;
      }
    }
    throw new IllegalArgumentException("never");
  }

  @Override
  public TCloseSessionResp CloseSession(TCloseSessionReq req) throws TException {
    TCloseSessionResp resp = new TCloseSessionResp();
    try {
      SessionHandle sessionHandle = new SessionHandle(req.getSessionHandle());
      cliService.closeSession(sessionHandle);
      resp.setStatus(OK_STATUS);
      ThriftCLIServerContext context =
        (ThriftCLIServerContext)currentServerContext.get();
      if (context != null) {
        context.setSessionHandle(null);
      }
    } catch (Exception e) {
      LOG.warn("Error closing session: ", e);
      resp.setStatus(ServiceSQLException.toTStatus(e));
    }
    return resp;
  }

  @Override
  public TGetInfoResp GetInfo(TGetInfoReq req) throws TException {
    TGetInfoResp resp = new TGetInfoResp();
    try {
      GetInfoValue getInfoValue =
          cliService.getInfo(new SessionHandle(req.getSessionHandle()),
              GetInfoType.getGetInfoType(req.getInfoType()));
      resp.setInfoValue(getInfoValue.toTGetInfoValue());
      resp.setStatus(OK_STATUS);
    } catch (Exception e) {
      LOG.warn("Error getting info: ", e);
      resp.setStatus(ServiceSQLException.toTStatus(e));
    }
    return resp;
  }

  @Override
  public TExecuteStatementResp ExecuteStatement(TExecuteStatementReq req) throws TException {
    TExecuteStatementResp resp = new TExecuteStatementResp();
    try {
      SessionHandle sessionHandle = new SessionHandle(req.getSessionHandle());
      String statement = req.getStatement();
      Map<String, String> confOverlay = req.getConfOverlay();
      Boolean runAsync = req.isRunAsync();
      long queryTimeout = req.getQueryTimeout();
      OperationHandle operationHandle = runAsync ?
          cliService.executeStatementAsync(sessionHandle, statement, confOverlay, queryTimeout)
          : cliService.executeStatement(sessionHandle, statement, confOverlay, queryTimeout);
          resp.setOperationHandle(operationHandle.toTOperationHandle());
          resp.setStatus(OK_STATUS);
    } catch (Exception e) {
      LOG.warn("Error executing statement: ", e);
      resp.setStatus(ServiceSQLException.toTStatus(e));
    }
    return resp;
  }

  @Override
  public TGetTypeInfoResp GetTypeInfo(TGetTypeInfoReq req) throws TException {
    TGetTypeInfoResp resp = new TGetTypeInfoResp();
    try {
      OperationHandle operationHandle =
          cliService.getTypeInfo(new SessionHandle(req.getSessionHandle()));
      resp.setOperationHandle(operationHandle.toTOperationHandle());
      resp.setStatus(OK_STATUS);
    } catch (Exception e) {
      LOG.warn("Error getting type info: ", e);
      resp.setStatus(ServiceSQLException.toTStatus(e));
    }
    return resp;
  }

  @Override
  public TGetCatalogsResp GetCatalogs(TGetCatalogsReq req) throws TException {
    TGetCatalogsResp resp = new TGetCatalogsResp();
    try {
      OperationHandle opHandle = cliService.getCatalogs(new SessionHandle(req.getSessionHandle()));
      resp.setOperationHandle(opHandle.toTOperationHandle());
      resp.setStatus(OK_STATUS);
    } catch (Exception e) {
      LOG.warn("Error getting catalogs: ", e);
      resp.setStatus(ServiceSQLException.toTStatus(e));
    }
    return resp;
  }

  @Override
  public TGetSchemasResp GetSchemas(TGetSchemasReq req) throws TException {
    TGetSchemasResp resp = new TGetSchemasResp();
    try {
      OperationHandle opHandle = cliService.getSchemas(
          new SessionHandle(req.getSessionHandle()), req.getCatalogName(), req.getSchemaName());
      resp.setOperationHandle(opHandle.toTOperationHandle());
      resp.setStatus(OK_STATUS);
    } catch (Exception e) {
      LOG.warn("Error getting schemas: ", e);
      resp.setStatus(ServiceSQLException.toTStatus(e));
    }
    return resp;
  }

  @Override
  public TGetTablesResp GetTables(TGetTablesReq req) throws TException {
    TGetTablesResp resp = new TGetTablesResp();
    try {
      OperationHandle opHandle = cliService
          .getTables(new SessionHandle(req.getSessionHandle()), req.getCatalogName(),
              req.getSchemaName(), req.getTableName(), req.getTableTypes());
      resp.setOperationHandle(opHandle.toTOperationHandle());
      resp.setStatus(OK_STATUS);
    } catch (Exception e) {
      LOG.warn("Error getting tables: ", e);
      resp.setStatus(ServiceSQLException.toTStatus(e));
    }
    return resp;
  }

  @Override
  public TGetTableTypesResp GetTableTypes(TGetTableTypesReq req) throws TException {
    TGetTableTypesResp resp = new TGetTableTypesResp();
    try {
      OperationHandle opHandle =
          cliService.getTableTypes(new SessionHandle(req.getSessionHandle()));
      resp.setOperationHandle(opHandle.toTOperationHandle());
      resp.setStatus(OK_STATUS);
    } catch (Exception e) {
      LOG.warn("Error getting table types: ", e);
      resp.setStatus(ServiceSQLException.toTStatus(e));
    }
    return resp;
  }

  @Override
  public TGetColumnsResp GetColumns(TGetColumnsReq req) throws TException {
    TGetColumnsResp resp = new TGetColumnsResp();
    try {
      OperationHandle opHandle = cliService.getColumns(
          new SessionHandle(req.getSessionHandle()),
          req.getCatalogName(),
          req.getSchemaName(),
          req.getTableName(),
          req.getColumnName());
      resp.setOperationHandle(opHandle.toTOperationHandle());
      resp.setStatus(OK_STATUS);
    } catch (Exception e) {
      LOG.warn("Error getting columns: ", e);
      resp.setStatus(ServiceSQLException.toTStatus(e));
    }
    return resp;
  }

  @Override
  public TGetFunctionsResp GetFunctions(TGetFunctionsReq req) throws TException {
    TGetFunctionsResp resp = new TGetFunctionsResp();
    try {
      OperationHandle opHandle = cliService.getFunctions(
          new SessionHandle(req.getSessionHandle()), req.getCatalogName(),
          req.getSchemaName(), req.getFunctionName());
      resp.setOperationHandle(opHandle.toTOperationHandle());
      resp.setStatus(OK_STATUS);
    } catch (Exception e) {
      LOG.warn("Error getting functions: ", e);
      resp.setStatus(ServiceSQLException.toTStatus(e));
    }
    return resp;
  }

  @Override
  public TGetOperationStatusResp GetOperationStatus(TGetOperationStatusReq req) throws TException {
    TGetOperationStatusResp resp = new TGetOperationStatusResp();
    try {
      OperationStatus operationStatus = cliService.getOperationStatus(
          new OperationHandle(req.getOperationHandle()));
      resp.setOperationState(operationStatus.getState().toTOperationState());
      ServiceSQLException opException = operationStatus.getOperationException();
      if (opException != null) {
        resp.setSqlState(opException.getSQLState());
        resp.setErrorCode(opException.getErrorCode());
        resp.setErrorMessage(opException.getMessage());
      }
      resp.setStatus(OK_STATUS);
    } catch (Exception e) {
      LOG.warn("Error getting operation status: ", e);
      resp.setStatus(ServiceSQLException.toTStatus(e));
    }
    return resp;
  }

  @Override
  public TCancelOperationResp CancelOperation(TCancelOperationReq req) throws TException {
    TCancelOperationResp resp = new TCancelOperationResp();
    try {
      cliService.cancelOperation(new OperationHandle(req.getOperationHandle()));
      resp.setStatus(OK_STATUS);
    } catch (Exception e) {
      LOG.warn("Error cancelling operation: ", e);
      resp.setStatus(ServiceSQLException.toTStatus(e));
    }
    return resp;
  }

  @Override
  public TCloseOperationResp CloseOperation(TCloseOperationReq req) throws TException {
    TCloseOperationResp resp = new TCloseOperationResp();
    try {
      cliService.closeOperation(new OperationHandle(req.getOperationHandle()));
      resp.setStatus(OK_STATUS);
    } catch (Exception e) {
      LOG.warn("Error closing operation: ", e);
      resp.setStatus(ServiceSQLException.toTStatus(e));
    }
    return resp;
  }

  @Override
  public TGetResultSetMetadataResp GetResultSetMetadata(TGetResultSetMetadataReq req)
      throws TException {
    TGetResultSetMetadataResp resp = new TGetResultSetMetadataResp();
    try {
      TableSchema schema =
          cliService.getResultSetMetadata(new OperationHandle(req.getOperationHandle()));
      resp.setSchema(schema.toTTableSchema());
      resp.setStatus(OK_STATUS);
    } catch (Exception e) {
      LOG.warn("Error getting result set metadata: ", e);
      resp.setStatus(ServiceSQLException.toTStatus(e));
    }
    return resp;
  }

  @Override
  public TFetchResultsResp FetchResults(TFetchResultsReq req) throws TException {
    TFetchResultsResp resp = new TFetchResultsResp();
    try {
      RowSet rowSet = cliService.fetchResults(
          new OperationHandle(req.getOperationHandle()),
          FetchOrientation.getFetchOrientation(req.getOrientation()),
          req.getMaxRows(),
          FetchType.getFetchType(req.getFetchType()));
      resp.setResults(rowSet.toTRowSet());
      resp.setHasMoreRows(false);
      resp.setStatus(OK_STATUS);
    } catch (Exception e) {
      LOG.warn("Error fetching results: ", e);
      resp.setStatus(ServiceSQLException.toTStatus(e));
    }
    return resp;
  }

  @Override
  public TGetPrimaryKeysResp GetPrimaryKeys(TGetPrimaryKeysReq req)
      throws TException {
    TGetPrimaryKeysResp resp = new TGetPrimaryKeysResp();
    try {
      OperationHandle opHandle = cliService.getPrimaryKeys(
      new SessionHandle(req.getSessionHandle()), req.getCatalogName(),
      req.getSchemaName(), req.getTableName());
      resp.setOperationHandle(opHandle.toTOperationHandle());
      resp.setStatus(OK_STATUS);
    } catch (Exception e) {
     LOG.warn("Error getting functions: ", e);
     resp.setStatus(ServiceSQLException.toTStatus(e));
    }
    return resp;
  }

  @Override
  public TGetCrossReferenceResp GetCrossReference(TGetCrossReferenceReq req)
      throws TException {
    TGetCrossReferenceResp resp = new TGetCrossReferenceResp();
    try {
      OperationHandle opHandle = cliService.getCrossReference(
        new SessionHandle(req.getSessionHandle()), req.getParentCatalogName(),
          req.getParentSchemaName(), req.getParentTableName(),
          req.getForeignCatalogName(), req.getForeignSchemaName(), req.getForeignTableName());
          resp.setOperationHandle(opHandle.toTOperationHandle());
          resp.setStatus(OK_STATUS);
    } catch (Exception e) {
      LOG.warn("Error getting functions: ", e);
      resp.setStatus(ServiceSQLException.toTStatus(e));
    }
    return resp;
  }

  @Override
  public abstract void run();

  /**
   * If the proxy user name is provided then check privileges to substitute the user.
   * @param realUser
   * @param sessionConf
   * @param ipAddress
   * @return
   * @throws ServiceSQLException
   */
  private String getProxyUser(String realUser, Map<String, String> sessionConf,
      String ipAddress) throws ServiceSQLException {
    String proxyUser = null;
    // Http transport mode.
    // We set the thread local proxy username, in ThriftHttpServlet.
    if (cliService.getSqlConf().getConf(
        ServiceConf.THRIFTSERVER_TRANSPORT_MODE()).equalsIgnoreCase("http")) {
      proxyUser = SessionManager.getProxyUserName();
      LOG.debug("Proxy user from query string: " + proxyUser);
    }

    if (proxyUser == null && sessionConf != null &&
        sessionConf.containsKey(SparkAuthFactory.SS2_PROXY_USER)) {
      String proxyUserFromThriftBody = sessionConf.get(SparkAuthFactory.SS2_PROXY_USER);
      LOG.debug("Proxy user from thrift body: " + proxyUserFromThriftBody);
      proxyUser = proxyUserFromThriftBody;
    }

    if (proxyUser == null) {
      return realUser;
    }

    // check whether substitution is allowed
    if (!((boolean) sqlConf.getConf(ServiceConf.THRIFTSERVER_ALLOW_USER_SUBSTITUTION()))) {
      throw new ServiceSQLException("Proxy user substitution is not allowed");
    }

    // If there's no authentication, then directly substitute the user
    if (SparkAuthFactory.AuthTypes.NONE.toString()
        .equalsIgnoreCase(sqlConf.getConf(ServiceConf.THRIFTSERVER_AUTHENTICATION()))) {
      return proxyUser;
    }

    // Verify proxy user privilege of the realUser for the proxyUser
    SparkAuthFactory.verifyProxyAccess(realUser, proxyUser, ipAddress,
        sqlContext.sparkContext().hadoopConfiguration());
    LOG.debug("Verified proxy user: " + proxyUser);
    return proxyUser;
  }

  private boolean isKerberosAuthMode() {
    return cliService.getSqlConf().getConf(ServiceConf.THRIFTSERVER_AUTHENTICATION())
        .equalsIgnoreCase(SparkAuthFactory.AuthTypes.KERBEROS.toString());
  }
}
