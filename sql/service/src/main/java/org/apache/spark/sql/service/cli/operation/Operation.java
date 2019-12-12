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

package org.apache.spark.sql.service.cli.operation;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.service.cli.*;
import org.apache.spark.sql.service.cli.ServiceSQLException;
import org.apache.spark.sql.service.cli.session.ServiceSession;
import org.apache.spark.sql.service.internal.ServiceConf;
import org.apache.spark.sql.service.rpc.thrift.TProtocolVersion;

public abstract class Operation {
  protected final ServiceSession parentSession;
  private OperationState state = OperationState.INITIALIZED;
  private final OperationHandle opHandle;
  private SQLConf configuration;
  public static final Logger LOG = LoggerFactory.getLogger(Operation.class.getName());
  public static final FetchOrientation DEFAULT_FETCH_ORIENTATION = FetchOrientation.FETCH_NEXT;
  public static final long DEFAULT_FETCH_MAX_ROWS = 100;
  protected boolean hasResultSet;
  protected volatile ServiceSQLException operationException;
  protected final boolean runAsync;
  protected volatile Future<?> backgroundHandle;
  protected OperationLog operationLog;
  protected boolean isOperationLogEnabled;
  protected Map<String, String> confOverlay = new HashMap<String, String>();

  private long operationTimeout;
  private long lastAccessTime;

  protected static final EnumSet<FetchOrientation> DEFAULT_FETCH_ORIENTATION_SET =
      EnumSet.of(
          FetchOrientation.FETCH_NEXT,
          FetchOrientation.FETCH_FIRST,
          FetchOrientation.FETCH_PRIOR);

  protected Operation(ServiceSession parentSession, OperationType opType) {
    this(parentSession, null, opType);
  }

  protected Operation(ServiceSession parentSession, Map<String, String> confOverlay,
                      OperationType opType) {
    this(parentSession, confOverlay, opType, false);
  }

  protected Operation(ServiceSession parentSession,
                      Map<String, String> confOverlay,
                      OperationType opType,
                      boolean runInBackground) {
    this.parentSession = parentSession;
    this.confOverlay = confOverlay;
    this.runAsync = runInBackground;
    this.opHandle = new OperationHandle(opType, parentSession.getProtocolVersion());
    lastAccessTime = System.currentTimeMillis();
    operationTimeout = (long) parentSession.getSQLConf()
        .getConf(ServiceConf.THRIFTSERVER_IDLE_OPERATION_TIMEOUT());
  }

  public Future<?> getBackgroundHandle() {
    return backgroundHandle;
  }

  protected void setBackgroundHandle(Future<?> backgroundHandle) {
    this.backgroundHandle = backgroundHandle;
  }

  public boolean shouldRunAsync() {
    return runAsync;
  }

  public void setConfiguration(SQLConf configuration) {
    this.configuration = configuration;
  }

  public SQLConf getConfiguration() {
    return configuration;
  }

  public ServiceSession getParentSession() {
    return parentSession;
  }

  public OperationHandle getHandle() {
    return opHandle;
  }

  public TProtocolVersion getProtocolVersion() {
    return opHandle.getProtocolVersion();
  }

  public OperationType getType() {
    return opHandle.getOperationType();
  }

  public OperationStatus getStatus() {
    return new OperationStatus(state, operationException);
  }

  public boolean hasResultSet() {
    return hasResultSet;
  }

  protected void setHasResultSet(boolean hasResultSet) {
    this.hasResultSet = hasResultSet;
    opHandle.setHasResultSet(hasResultSet);
  }

  public OperationLog getOperationLog() {
    return operationLog;
  }

  protected final OperationState setState(OperationState newState) throws ServiceSQLException {
    state.validateTransition(newState);
    this.state = newState;
    this.lastAccessTime = System.currentTimeMillis();
    return this.state;
  }

  public boolean isTimedOut(long current) {
    if (operationTimeout == 0) {
      return false;
    }
    if (operationTimeout > 0) {
      // check only when it's in terminal state
      return state.isTerminal() && lastAccessTime + operationTimeout <= current;
    }
    return lastAccessTime + -operationTimeout <= current;
  }

  public long getLastAccessTime() {
    return lastAccessTime;
  }

  public long getOperationTimeout() {
    return operationTimeout;
  }

  public void setOperationTimeout(long operationTimeout) {
    this.operationTimeout = operationTimeout;
  }

  protected void setOperationException(ServiceSQLException operationException) {
    this.operationException = operationException;
  }

  protected final void assertState(OperationState state) throws ServiceSQLException {
    if (this.state != state) {
      throw new ServiceSQLException("Expected state " + state + ", but found " + this.state);
    }
    this.lastAccessTime = System.currentTimeMillis();
  }

  public boolean isRunning() {
    return OperationState.RUNNING.equals(state);
  }

  public boolean isFinished() {
    return OperationState.FINISHED.equals(state);
  }

  public boolean isCanceled() {
    return OperationState.CANCELED.equals(state);
  }

  public boolean isFailed() {
    return OperationState.ERROR.equals(state);
  }

  protected void createOperationLog() {
    if (parentSession.isOperationLogEnabled()) {
      File operationLogFile = new File(parentSession.getOperationLogSessionDir(),
          opHandle.getHandleIdentifier().toString());
      isOperationLogEnabled = true;

      // create log file
      try {
        if (operationLogFile.exists()) {
          LOG.warn("The operation log file should not exist, but it is already there: " +
              operationLogFile.getAbsolutePath());
          operationLogFile.delete();
        }
        if (!operationLogFile.createNewFile()) {
          // the log file already exists and cannot be deleted.
          // If it can be read/written, keep its contents and use it.
          if (!operationLogFile.canRead() || !operationLogFile.canWrite()) {
            LOG.warn("The already existed operation log file cannot be recreated, " +
                "and it cannot be read or written: " + operationLogFile.getAbsolutePath());
            isOperationLogEnabled = false;
            return;
          }
        }
      } catch (Exception e) {
        LOG.warn("Unable to create operation log file: " + operationLogFile.getAbsolutePath(), e);
        isOperationLogEnabled = false;
        return;
      }

      // create OperationLog object with above log file
      try {
        operationLog =
            new OperationLog(opHandle.toString(), operationLogFile, parentSession.getSQLConf());
      } catch (FileNotFoundException e) {
        LOG.warn("Unable to instantiate OperationLog object for operation: " +
            opHandle, e);
        isOperationLogEnabled = false;
        return;
      }

      // register this operationLog to current thread
      OperationLog.setCurrentOperationLog(operationLog);
    }
  }

  protected void unregisterOperationLog() {
    if (isOperationLogEnabled) {
      OperationLog.removeCurrentOperationLog();
    }
  }

  /**
   * Invoked before runInternal().
   * Set up some preconditions, or configurations.
   */
  protected void beforeRun() {
    createOperationLog();
  }

  /**
   * Invoked after runInternal(), even if an exception is thrown in runInternal().
   * Clean up resources, which was set up in beforeRun().
   */
  protected void afterRun() {
    unregisterOperationLog();
  }

  /**
   * Implemented by subclass of Operation class to execute specific behaviors.
   * @throws ServiceSQLException
   */
  protected abstract void runInternal() throws ServiceSQLException;

  public void run() throws ServiceSQLException {
    beforeRun();
    try {
      runInternal();
    } finally {
      afterRun();
    }
  }

  protected void cleanupOperationLog() {
    if (isOperationLogEnabled) {
      if (operationLog == null) {
        LOG.error("Operation [ " + opHandle.getHandleIdentifier() + " ] "
          + "logging is enabled, but its OperationLog object cannot be found.");
      } else {
        operationLog.close();
      }
    }
  }

  // TODO: make this abstract and implement in subclasses.
  public void cancel() throws ServiceSQLException {
    setState(OperationState.CANCELED);
    throw new UnsupportedOperationException("SQLOperation.cancel()");
  }

  public abstract void close() throws ServiceSQLException;

  public abstract TableSchema getResultSetSchema() throws ServiceSQLException;

  public abstract RowSet getNextRowSet(FetchOrientation orientation, long maxRows)
      throws ServiceSQLException;

  public RowSet getNextRowSet() throws ServiceSQLException {
    return getNextRowSet(FetchOrientation.FETCH_NEXT, DEFAULT_FETCH_MAX_ROWS);
  }

  /**
   * Verify if the given fetch orientation is part of the default orientation types.
   * @param orientation
   * @throws ServiceSQLException
   */
  protected void validateDefaultFetchOrientation(FetchOrientation orientation)
      throws ServiceSQLException {
    validateFetchOrientation(orientation, DEFAULT_FETCH_ORIENTATION_SET);
  }

  /**
   * Verify if the given fetch orientation is part of the supported orientation types.
   * @param orientation
   * @param supportedOrientations
   * @throws ServiceSQLException
   */
  protected void validateFetchOrientation(FetchOrientation orientation,
      EnumSet<FetchOrientation> supportedOrientations) throws ServiceSQLException {
    if (!supportedOrientations.contains(orientation)) {
      throw new ServiceSQLException("The fetch type " + orientation.toString() +
          " is not supported for this resultset", "HY106");
    }
  }

  protected Map<String, String> getConfOverlay() {
    return confOverlay;
  }
}
