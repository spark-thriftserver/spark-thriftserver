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

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.service.auth.SparkAuthFactory;
import org.apache.spark.sql.service.cli.ServiceSQLException;
import org.apache.spark.sql.service.rpc.thrift.TProtocolVersion;

/**
 *
 * ServiceSessionImplwithUGI.
 * ServiceSession with connecting user's UGI and delegation token if required
 */
public class ServiceSessionImplwithUGI extends ServiceSessionImpl {

  private UserGroupInformation sessionUgi = null;
  private String delegationTokenStr = null;
  private ServiceSession proxySession = null;
  static final Logger LOG = LoggerFactory.getLogger(ServiceSessionImplwithUGI.class);

  public ServiceSessionImplwithUGI(TProtocolVersion protocol, String username, String password,
                                   SQLContext sqlContext, String ipAddress, String delegationToken)
      throws ServiceSQLException {
    super(protocol, username, password, sqlContext, ipAddress);
    setSessionUGI(username);
  }

  // setup appropriate UGI for the session
  public void setSessionUGI(String owner) throws ServiceSQLException {
    if (owner == null) {
      throw new ServiceSQLException("No username provided for impersonation");
    }
    if (UserGroupInformation.isSecurityEnabled()) {
      try {
        sessionUgi = UserGroupInformation.createProxyUser(
            owner, UserGroupInformation.getLoginUser());
      } catch (IOException e) {
        throw new ServiceSQLException("Couldn't setup proxy user", e);
      }
    } else {
      sessionUgi = UserGroupInformation.createRemoteUser(owner);
    }
  }

  public UserGroupInformation getSessionUgi() {
    return this.sessionUgi;
  }

  public String getDelegationToken() {
    return this.delegationTokenStr;
  }

  @Override
  protected synchronized void acquire(boolean userAccess) {
    super.acquire(userAccess);
  }

  /**
   * Close the file systems for the session and remove it from the FileSystem cache.
   * Cancel the session's delegation token and close the metastore connection
   */
  @Override
  public void close() throws ServiceSQLException {
    try {
      acquire(true);
    } finally {
      try {
        super.close();
      } finally {
        try {
          FileSystem.closeAllForUGI(sessionUgi);
        } catch (IOException ioe) {
          throw new ServiceSQLException("Could not clean up file-system handles for UGI: "
              + sessionUgi, ioe);
        }
      }
    }
  }

  @Override
  protected ServiceSession getSession() {
    assert proxySession != null;

    return proxySession;
  }

  public void setProxySession(ServiceSession proxySession) {
    this.proxySession = proxySession;
  }

  @Override
  public String getDelegationToken(SparkAuthFactory authFactory, String owner,
                                   String renewer) throws ServiceSQLException {
    return authFactory.getDelegationToken(owner, renewer, getIpAddress());
  }

  @Override
  public void cancelDelegationToken(SparkAuthFactory authFactory, String tokenStr)
      throws ServiceSQLException {
    authFactory.cancelDelegationToken(tokenStr);
  }

  @Override
  public void renewDelegationToken(SparkAuthFactory authFactory, String tokenStr)
      throws ServiceSQLException {
    authFactory.renewDelegationToken(tokenStr);
  }

}
