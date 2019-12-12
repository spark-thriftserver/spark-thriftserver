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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.internal.VariableSubstitution;
import org.apache.spark.sql.service.auth.SparkAuthFactory;
import org.apache.spark.sql.service.cli.*;
import org.apache.spark.sql.service.cli.ServiceSQLException;
import org.apache.spark.sql.service.cli.file.ISparkFileProcessor;
import org.apache.spark.sql.service.cli.file.SparkFileProcessor;
import org.apache.spark.sql.service.cli.operation.*;
import org.apache.spark.sql.service.internal.ServiceConf;
import org.apache.spark.sql.service.rpc.thrift.TProtocolVersion;

/**
 * ServiceSession
 *
 */
public class ServiceSessionImpl implements ServiceSession {

  public static final String ENV_PREFIX = "env:";
  public static final String SYSTEM_PREFIX = "system:";
  public static final String SPARKCONF_PREFIX = "sparkconf:";
  public static final String SPARKVAR_PREFIX = "spark:";
  public static final String HIVECONF_PREFIX = "hiveconf:";
  public static final String HIVEVAR_PREFIX = "hivevar:";
  public static final String SET_COLUMN_NAME = "set:";

  private final SessionHandle sessionHandle;
  private String username;
  private final String password;
  private SQLConf sqlConf;
  private SQLContext sqlContext;
  private String ipAddress;
  private static final Logger LOG = LoggerFactory.getLogger(ServiceSessionImpl.class);
  private SessionManager sessionManager;
  private OperationManager operationManager;
  private final Set<OperationHandle> opHandleSet = new HashSet<OperationHandle>();
  private boolean isOperationLogEnabled;
  private File sessionLogDir;
  private volatile long lastAccessTime;
  private volatile long lastIdleTime;

  public ServiceSessionImpl(TProtocolVersion protocol, String username, String password,
                            SQLContext sqlContext, String ipAddress) {
    this.username = username;
    this.password = password;
    this.sessionHandle = new SessionHandle(protocol);
    this.ipAddress = ipAddress;
    this.sqlConf = sqlContext.conf();
    this.sqlContext = sqlContext;
  }

  @Override
  /**
   * Opens a new SparkThriftServer session for the client connection.
   * Creates a new SessionState object that will be associated with this SparkThriftServer session.
   * When the server executes multiple queries in the same session,
   * this SessionState object is reused across multiple queries.
   * Note that if doAs is true, this call goes through a proxy object,
   * which wraps the method logic in a UserGroupInformation#doAs.
   * That's why it is important to create SessionState here rather than in the constructor.
   */
  public void open(Map<String, String> sessionConfMap) throws ServiceSQLException {
    // Process global init file: .sparkrc
    processGlobalInitFile();
    if (sessionConfMap != null) {
      configureSession(sessionConfMap);
    }
    lastAccessTime = System.currentTimeMillis();
    lastIdleTime = lastAccessTime;
  }

  /**
   * It is used for processing sparkrc file from SparkThriftServer side.
   */
  private class GlobalSparkrcFileProcessor extends SparkFileProcessor {
    @Override
    protected BufferedReader loadFile(String fileName) throws IOException {
      FileInputStream initStream = null;
      BufferedReader bufferedReader = null;
      initStream = new FileInputStream(fileName);
      bufferedReader = new BufferedReader(new InputStreamReader(initStream));
      return bufferedReader;
    }

    @Override
    protected int processCmd(String cmd) {
      int rc = 0;
      String cmd_trimed = cmd.trim();
      try {
        executeStatementInternal(cmd_trimed, null, false, 0);
      } catch (ServiceSQLException e) {
        rc = -1;
        LOG.warn("Failed to execute HQL command in global .sparkrc file.", e);
      }
      return rc;
    }
  }

  private void processGlobalInitFile() {
    ISparkFileProcessor processor = new GlobalSparkrcFileProcessor();

    try {
      String sparkrc = sqlConf.getConf(ServiceConf.THRIFTSERVER_GLOABLE_INIT_FILE_LOCATION());
      if (sparkrc != null) {
        File sparkrcFile = new File(sparkrc);
        if (sparkrcFile.isDirectory()) {
          sparkrcFile = new File(sparkrcFile, SessionManager.SPARKRCFILE);
        }
        if (sparkrcFile.isFile()) {
          LOG.info("Running global init file: " + sparkrcFile);
          int rc = processor.processFile(sparkrcFile.getAbsolutePath());
          if (rc != 0) {
            LOG.error("Failed on initializing global .sparkrc file");
          }
        } else {
          LOG.debug("Global init file " + sparkrcFile + " does not exist");
        }
      }
    } catch (IOException e) {
      LOG.warn("Failed on initializing global .sparkrc file", e);
    }
  }

  private void configureSession(Map<String, String> sessionConfMap) throws ServiceSQLException {
    for (Map.Entry<String, String> entry : sessionConfMap.entrySet()) {
      String key = entry.getKey();
      if (key.startsWith(SET_COLUMN_NAME)) {
        try {
          setVariable(key.substring(4), entry.getValue());
        } catch (Exception e) {
          throw new ServiceSQLException(e);
        }
      }
    }
  }

  // Copy from org.apache.hadoop.hive.ql.processors.SetProcessor, only change:
  // setConf(varname, propName, varvalue, true) when varname.startsWith(SPARKCONF_PREFIX)
  private int setVariable(String varname, String varvalue) throws Exception {
    VariableSubstitution substitution = new VariableSubstitution(sqlConf);
    if (varvalue.contains("\n")){
      LOG.error("Warning: Value had a \\n character in it.");
    }
    varname = varname.trim();
    if (varname.startsWith(ENV_PREFIX)) {
      LOG.error("env:* variables can not be set.");
      return 1;
    } else if (varname.startsWith(SYSTEM_PREFIX)) {
      String propName = varname.substring(SYSTEM_PREFIX.length());
      System.getProperties().setProperty(propName, substitution.substitute(varvalue));
    } else if (varname.startsWith(SPARKCONF_PREFIX)) {
      String propName = varname.substring(SPARKCONF_PREFIX.length());
      sqlContext.setConf(propName, substitution.substitute(varvalue));
    } else if (varname.startsWith(SPARKVAR_PREFIX)) {
      String propName = varname.substring(SPARKVAR_PREFIX.length());
      sqlContext.setConf(propName, substitution.substitute(varvalue));
    } else if (varname.startsWith(HIVECONF_PREFIX)) {
      String propName = varname.substring(HIVECONF_PREFIX.length());
      sqlContext.setConf(propName, substitution.substitute(varvalue));
    } else if (varname.startsWith(HIVEVAR_PREFIX)) {
      String propName = varname.substring(HIVEVAR_PREFIX.length());
      sqlContext.setConf(propName, substitution.substitute(varvalue));
    } else {
      sqlContext.setConf(varname, substitution.substitute(varvalue));
    }
    return 0;
  }

  @Override
  public void setOperationLogSessionDir(File operationLogRootDir) {
    if (!operationLogRootDir.exists()) {
      LOG.warn("The operation log root directory is removed, recreating: " +
          operationLogRootDir.getAbsolutePath());
      if (!operationLogRootDir.mkdirs()) {
        LOG.warn("Unable to create operation log root directory: " +
            operationLogRootDir.getAbsolutePath());
      }
    }
    if (!operationLogRootDir.canWrite()) {
      LOG.warn("The operation log root directory is not writable: " +
          operationLogRootDir.getAbsolutePath());
    }
    sessionLogDir = new File(operationLogRootDir, sessionHandle.getHandleIdentifier().toString());
    isOperationLogEnabled = true;
    if (!sessionLogDir.exists()) {
      if (!sessionLogDir.mkdir()) {
        LOG.warn("Unable to create operation log session directory: " +
            sessionLogDir.getAbsolutePath());
        isOperationLogEnabled = false;
      }
    }
    if (isOperationLogEnabled) {
      LOG.info("Operation log session directory is created: " + sessionLogDir.getAbsolutePath());
    }
  }

  @Override
  public boolean isOperationLogEnabled() {
    return isOperationLogEnabled;
  }

  @Override
  public File getOperationLogSessionDir() {
    return sessionLogDir;
  }

  @Override
  public TProtocolVersion getProtocolVersion() {
    return sessionHandle.getProtocolVersion();
  }

  @Override
  public SessionManager getSessionManager() {
    return sessionManager;
  }

  @Override
  public void setSessionManager(SessionManager sessionManager) {
    this.sessionManager = sessionManager;
  }

  private OperationManager getOperationManager() {
    return operationManager;
  }

  @Override
  public void setOperationManager(OperationManager operationManager) {
    this.operationManager = operationManager;
  }

  protected synchronized void acquire(boolean userAccess) {
    if (userAccess) {
      lastAccessTime = System.currentTimeMillis();
    }
  }

  protected synchronized void release(boolean userAccess) {
    if (userAccess) {
      lastAccessTime = System.currentTimeMillis();
    }
    if (opHandleSet.isEmpty()) {
      lastIdleTime = System.currentTimeMillis();
    } else {
      lastIdleTime = 0;
    }
  }

  @Override
  public SessionHandle getSessionHandle() {
    return sessionHandle;
  }

  @Override
  public String getUsername() {
    return username;
  }

  @Override
  public String getPassword() {
    return password;
  }

  @Override
  public SQLConf getSQLConf() {
    return sqlConf;
  }

  @Override
  public SQLContext getSQLContext() {
    return sqlContext;
  }

  @Override
  public GetInfoValue getInfo(GetInfoType getInfoType)
      throws ServiceSQLException {
    acquire(true);
    try {
      throw new ServiceSQLException("Unrecognized GetInfoType value: " + getInfoType.toString());
    } finally {
      release(true);
    }
  }

  @Override
  public OperationHandle executeStatement(String statement, Map<String, String> confOverlay)
      throws ServiceSQLException {
    return executeStatementInternal(statement, confOverlay, false, 0);
  }

  @Override
  public OperationHandle executeStatement(String statement, Map<String, String> confOverlay,
      long queryTimeout) throws ServiceSQLException {
    return executeStatementInternal(statement, confOverlay, false, queryTimeout);
  }

  @Override
  public OperationHandle executeStatementAsync(String statement, Map<String, String> confOverlay)
      throws ServiceSQLException {
    return executeStatementInternal(statement, confOverlay, true, 0);
  }

  @Override
  public OperationHandle executeStatementAsync(String statement, Map<String, String> confOverlay,
      long queryTimeout) throws ServiceSQLException {
    return executeStatementInternal(statement, confOverlay, true, queryTimeout);
  }

  private OperationHandle executeStatementInternal(String statement,
      Map<String, String> confOverlay, boolean runAsync,
      long queryTimeout) throws ServiceSQLException {
    acquire(true);

    OperationManager operationManager = getOperationManager();
    ExecuteStatementOperation operation = operationManager
        .newExecuteStatementOperation(getSession(), statement, confOverlay, runAsync, queryTimeout);
    OperationHandle opHandle = operation.getHandle();
    try {
      operation.run();
      opHandleSet.add(opHandle);
      return opHandle;
    } catch (ServiceSQLException e) {
      // Referring to SQLOperation.java, there is no chance that a ServiceSQLException throws
      // and the asyn background operation submits to thread pool successfully at the same time.
      // So, Cleanup opHandle directly when got ServiceSQLException
      operationManager.closeOperation(opHandle);
      throw e;
    } finally {
      release(true);
    }
  }

  @Override
  public OperationHandle getTypeInfo()
      throws ServiceSQLException {
    acquire(true);

    OperationManager operationManager = getOperationManager();
    SparkGetTypeInfoOperation operation = operationManager.newGetTypeInfoOperation(getSession());
    OperationHandle opHandle = operation.getHandle();
    try {
      operation.run();
      opHandleSet.add(opHandle);
      return opHandle;
    } catch (ServiceSQLException e) {
      operationManager.closeOperation(opHandle);
      throw e;
    } finally {
      release(true);
    }
  }

  @Override
  public OperationHandle getCatalogs()
      throws ServiceSQLException {
    acquire(true);

    OperationManager operationManager = getOperationManager();
    SparkGetCatalogsOperation operation = operationManager.newGetCatalogsOperation(getSession());
    OperationHandle opHandle = operation.getHandle();
    try {
      operation.run();
      opHandleSet.add(opHandle);
      return opHandle;
    } catch (ServiceSQLException e) {
      operationManager.closeOperation(opHandle);
      throw e;
    } finally {
      release(true);
    }
  }

  @Override
  public OperationHandle getSchemas(String catalogName, String schemaName)
      throws ServiceSQLException {
    acquire(true);

    OperationManager operationManager = getOperationManager();
    SparkGetSchemasOperation operation =
        operationManager.newGetSchemasOperation(getSession(), catalogName, schemaName);
    OperationHandle opHandle = operation.getHandle();
    try {
      operation.run();
      opHandleSet.add(opHandle);
      return opHandle;
    } catch (ServiceSQLException e) {
      operationManager.closeOperation(opHandle);
      throw e;
    } finally {
      release(true);
    }
  }

  @Override
  public OperationHandle getTables(String catalogName, String schemaName, String tableName,
      List<String> tableTypes)
          throws ServiceSQLException {
    acquire(true);

    OperationManager operationManager = getOperationManager();
    SparkMetadataOperation operation =
        operationManager.newGetTablesOperation(getSession(), catalogName,
            schemaName, tableName, tableTypes);
    OperationHandle opHandle = operation.getHandle();
    try {
      operation.run();
      opHandleSet.add(opHandle);
      return opHandle;
    } catch (ServiceSQLException e) {
      operationManager.closeOperation(opHandle);
      throw e;
    } finally {
      release(true);
    }
  }

  @Override
  public OperationHandle getTableTypes()
      throws ServiceSQLException {
    acquire(true);

    OperationManager operationManager = getOperationManager();
    SparkGetTableTypesOperation operation =
        operationManager.newGetTableTypesOperation(getSession());
    OperationHandle opHandle = operation.getHandle();
    try {
      operation.run();
      opHandleSet.add(opHandle);
      return opHandle;
    } catch (ServiceSQLException e) {
      operationManager.closeOperation(opHandle);
      throw e;
    } finally {
      release(true);
    }
  }

  @Override
  public OperationHandle getColumns(String catalogName, String schemaName,
      String tableName, String columnName)  throws ServiceSQLException {
    acquire(true);
    OperationManager operationManager = getOperationManager();
    SparkGetColumnsOperation operation = operationManager.newGetColumnsOperation(getSession(),
        catalogName, schemaName, tableName, columnName);
    OperationHandle opHandle = operation.getHandle();
    try {
      operation.run();
      opHandleSet.add(opHandle);
      return opHandle;
    } catch (ServiceSQLException e) {
      operationManager.closeOperation(opHandle);
      throw e;
    } finally {
      release(true);
    }
  }

  @Override
  public OperationHandle getFunctions(String catalogName, String schemaName, String functionName)
      throws ServiceSQLException {
    acquire(true);

    OperationManager operationManager = getOperationManager();
    SparkGetFunctionsOperation operation = operationManager
        .newGetFunctionsOperation(getSession(), catalogName, schemaName, functionName);
    OperationHandle opHandle = operation.getHandle();
    try {
      operation.run();
      opHandleSet.add(opHandle);
      return opHandle;
    } catch (ServiceSQLException e) {
      operationManager.closeOperation(opHandle);
      throw e;
    } finally {
      release(true);
    }
  }

  @Override
  public void close() throws ServiceSQLException {
    try {
      acquire(true);
      // Iterate through the opHandles and close their operations
      for (OperationHandle opHandle : opHandleSet) {
        operationManager.closeOperation(opHandle);
      }
      opHandleSet.clear();
      // Cleanup session log directory.
      cleanupSessionLogDir();
    } catch (Exception e) {
      throw new ServiceSQLException("Failure to close", e);
    } finally {
      release(true);
    }
  }

  private void cleanupSessionLogDir() {
    if (isOperationLogEnabled) {
      try {
        FileUtils.forceDelete(sessionLogDir);
      } catch (Exception e) {
        LOG.error("Failed to cleanup session log dir: " + sessionHandle, e);
      }
    }
  }

  @Override
  public String getUserName() {
    return username;
  }

  @Override
  public void setUserName(String userName) {
    this.username = userName;
  }

  @Override
  public long getLastAccessTime() {
    return lastAccessTime;
  }

  @Override
  public void closeExpiredOperations() {
    OperationHandle[] handles = opHandleSet.toArray(new OperationHandle[opHandleSet.size()]);
    if (handles.length > 0) {
      List<Operation> operations = operationManager.removeExpiredOperations(handles);
      if (!operations.isEmpty()) {
        closeTimedOutOperations(operations);
      }
    }
  }

  @Override
  public long getNoOperationTime() {
    return lastIdleTime > 0 ? System.currentTimeMillis() - lastIdleTime : 0;
  }

  private void closeTimedOutOperations(List<Operation> operations) {
    acquire(false);
    try {
      for (Operation operation : operations) {
        opHandleSet.remove(operation.getHandle());
        try {
          operation.close();
        } catch (Exception e) {
          LOG.warn("Exception is thrown closing timed-out operation " + operation.getHandle(), e);
        }
      }
    } finally {
      release(false);
    }
  }

  @Override
  public void cancelOperation(OperationHandle opHandle) throws ServiceSQLException {
    acquire(true);
    try {
      sessionManager.getOperationManager().cancelOperation(opHandle);
    } finally {
      release(true);
    }
  }

  @Override
  public void closeOperation(OperationHandle opHandle) throws ServiceSQLException {
    acquire(true);
    try {
      operationManager.closeOperation(opHandle);
      opHandleSet.remove(opHandle);
    } finally {
      release(true);
    }
  }

  @Override
  public TableSchema getResultSetMetadata(OperationHandle opHandle) throws ServiceSQLException {
    acquire(true);
    try {
      return sessionManager.getOperationManager().getOperationResultSetSchema(opHandle);
    } finally {
      release(true);
    }
  }

  @Override
  public RowSet fetchResults(OperationHandle opHandle, FetchOrientation orientation,
      long maxRows, FetchType fetchType) throws ServiceSQLException {
    acquire(true);
    try {
      if (fetchType == FetchType.QUERY_OUTPUT) {
        return operationManager.getOperationNextRowSet(opHandle, orientation, maxRows);
      }
      return operationManager.getOperationLogRowSet(opHandle, orientation, maxRows);
    } finally {
      release(true);
    }
  }

  protected ServiceSession getSession() {
    return this;
  }

  @Override
  public String getIpAddress() {
    return ipAddress;
  }

  @Override
  public void setIpAddress(String ipAddress) {
    this.ipAddress = ipAddress;
  }

  @Override
  public String getDelegationToken(SparkAuthFactory authFactory, String owner, String renewer)
      throws ServiceSQLException {
    SparkAuthFactory.verifyProxyAccess(getUsername(), owner, getIpAddress(),
        sqlContext.sparkContext().hadoopConfiguration());
    return authFactory.getDelegationToken(owner, renewer, getIpAddress());
  }

  @Override
  public void cancelDelegationToken(SparkAuthFactory authFactory, String tokenStr)
      throws ServiceSQLException {
    SparkAuthFactory.verifyProxyAccess(getUsername(), getUserFromToken(authFactory, tokenStr),
        getIpAddress(), sqlContext.sparkContext().hadoopConfiguration());
    authFactory.cancelDelegationToken(tokenStr);
  }

  @Override
  public void renewDelegationToken(SparkAuthFactory authFactory, String tokenStr)
      throws ServiceSQLException {
    SparkAuthFactory.verifyProxyAccess(getUsername(), getUserFromToken(authFactory, tokenStr),
        getIpAddress(), sqlContext.sparkContext().hadoopConfiguration());
    authFactory.renewDelegationToken(tokenStr);
  }

  // extract the real user from the given token string
  private String getUserFromToken(SparkAuthFactory authFactory, String tokenStr)
      throws ServiceSQLException {
    return authFactory.getUserFromToken(tokenStr);
  }

  @Override
  public OperationHandle getPrimaryKeys(String catalog, String schema,
      String table) throws ServiceSQLException {
    acquire(true);
    throw new ServiceSQLException("GetPrimaryKeys is not supported yet");
  }

  @Override
  public OperationHandle getCrossReference(String primaryCatalog,
      String primarySchema, String primaryTable, String foreignCatalog,
      String foreignSchema, String foreignTable) throws ServiceSQLException {
    acquire(true);
    throw new ServiceSQLException("GetCrossReference is not supported yet");
  }
}
