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

package org.apache.spark.sql.service.cli.session;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.service.auth.SparkAuthFactory;
import org.apache.spark.sql.service.cli.ServiceSQLException;
import org.apache.spark.sql.service.rpc.thrift.TProtocolVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * ServiceSessionImplwithUGI.
 * ServiceSession with connecting user's UGI and delegation token if required
 */
public class ServiceSessionImplwithUGI extends ServiceSessionImpl {
  public static final String HS2TOKEN = "SparkServer2ImpersonationToken";

  private UserGroupInformation sessionUgi = null;
  private String delegationTokenStr = null;
  private Hive sessionHive = null;
  private ServiceSession proxySession = null;
  static final Logger LOG = LoggerFactory.getLogger(ServiceSessionImplwithUGI.class);

  public ServiceSessionImplwithUGI(TProtocolVersion protocol, String username, String password,
      HiveConf hiveConf, String ipAddress, String delegationToken, SQLContext context)
      throws ServiceSQLException {
    super(protocol, username, password, hiveConf, ipAddress, context);
    setSessionUGI(username);
    setDelegationToken(delegationToken);
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
    // if we have a metastore connection with impersonation, then set it first
    if (sessionHive != null) {
      Hive.set(sessionHive);
    }
  }

  /**
   * Close the file systems for the session and remove it from the FileSystem cache.
   * Cancel the session's delegation token and close the metastore connection
   */
  @Override
  public void close() throws ServiceSQLException {
    try {
      acquire(true);
      cancelDelegationToken();
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

  /**
   * Enable delegation token for the session
   * save the token string and set the token.signature in hive conf. The metastore client uses
   * this token.signature to determine where to use kerberos or delegation token
   * @throws HiveException
   * @throws IOException
   */
  private void setDelegationToken(String delegationTokenStr) throws ServiceSQLException {
    this.delegationTokenStr = delegationTokenStr;
    if (delegationTokenStr != null) {
      getHiveConf().set("hive.metastore.token.signature", HS2TOKEN);
      try {
        Utils.setTokenStr(sessionUgi, delegationTokenStr, HS2TOKEN);
      } catch (IOException e) {
        throw new ServiceSQLException("Couldn't setup delegation token in the ugi", e);
      }
    }
  }

  // If the session has a delegation token obtained from the metastore, then cancel it
  private void cancelDelegationToken() throws ServiceSQLException {
    if (delegationTokenStr != null) {
      try {
        Hive.get(getHiveConf()).cancelDelegationToken(delegationTokenStr);
      } catch (HiveException e) {
        throw new ServiceSQLException("Couldn't cancel delegation token", e);
      }
      // close the metastore connection created with this delegation token
      Hive.closeCurrent();
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
