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

package org.apache.spark.sql.service.auth.thrift;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.ReflectionUtils;

import org.apache.spark.sql.service.auth.thrift.HadoopThriftAuthBridge.Server.ServerMode;
import org.apache.spark.sql.service.utils.Utils;

public class SparkDelegationTokenManager {

  public static final String  DELEGATION_TOKEN_GC_INTERVAL =
          "spark.cluster.delegation.token.gc-interval";
  private static long DELEGATION_TOKEN_GC_INTERVAL_DEFAULT = 3600000; // 1 hour
  // Delegation token related keys
  public static final String  DELEGATION_KEY_UPDATE_INTERVAL_KEY =
          "spark.cluster.delegation.key.update-interval";
  public static final long    DELEGATION_KEY_UPDATE_INTERVAL_DEFAULT =
          24*60*60*1000; // 1 day
  public static final String  DELEGATION_TOKEN_RENEW_INTERVAL_KEY =
          "spark.cluster.delegation.token.renew-interval";
  public static final long    DELEGATION_TOKEN_RENEW_INTERVAL_DEFAULT =
          24*60*60*1000;  // 1 day
  public static final String  DELEGATION_TOKEN_MAX_LIFETIME_KEY =
          "spark.cluster.delegation.token.max-lifetime";
  public static final long    DELEGATION_TOKEN_MAX_LIFETIME_DEFAULT =
          7*24*60*60*1000; // 7 days
  public static final String DELEGATION_TOKEN_STORE_CLS =
          "spark.cluster.delegation.token.store.class";
  public static final String DELEGATION_TOKEN_STORE_ZK_CONNECT_STR =
          "spark.cluster.delegation.token.store.zookeeper.connectString";
  // Alternate connect string specification configuration
  public static final String DELEGATION_TOKEN_STORE_ZK_CONNECT_STR_ALTERNATE =
          "spark.zookeeper.quorum";

  public static final String DELEGATION_TOKEN_STORE_ZK_CONNECT_TIMEOUTMILLIS =
          "spark.cluster.delegation.token.store.zookeeper.connectTimeoutMillis";
  public static final String DELEGATION_TOKEN_STORE_ZK_ZNODE =
          "spark.cluster.delegation.token.store.zookeeper.znode";
  public static final String DELEGATION_TOKEN_STORE_ZK_ACL =
          "spark.cluster.delegation.token.store.zookeeper.acl";
  public static final String DELEGATION_TOKEN_STORE_ZK_ZNODE_DEFAULT =
          "/hivedelegation";

  protected DelegationTokenSecretManager secretManager;

  public SparkDelegationTokenManager() {
  }

  public DelegationTokenSecretManager getSecretManager() {
    return secretManager;
  }

  public void startDelegationTokenSecretManager(Configuration conf, ServerMode smode)
          throws IOException {
    long secretKeyInterval =
            conf.getLong(DELEGATION_KEY_UPDATE_INTERVAL_KEY,
                DELEGATION_KEY_UPDATE_INTERVAL_DEFAULT);
    long tokenMaxLifetime =
            conf.getLong(DELEGATION_TOKEN_MAX_LIFETIME_KEY,
                DELEGATION_TOKEN_MAX_LIFETIME_DEFAULT);
    long tokenRenewInterval =
            conf.getLong(DELEGATION_TOKEN_RENEW_INTERVAL_KEY,
                DELEGATION_TOKEN_RENEW_INTERVAL_DEFAULT);
    long tokenGcInterval =
            conf.getLong(DELEGATION_TOKEN_GC_INTERVAL,
                DELEGATION_TOKEN_GC_INTERVAL_DEFAULT);

    DelegationTokenStore dts = getTokenStore(conf);
    dts.setConf(conf);
    dts.init(null, smode);
    secretManager =
            new TokenStoreDelegationTokenSecretManager(secretKeyInterval, tokenMaxLifetime,
                    tokenRenewInterval, tokenGcInterval, dts);
    secretManager.startThreads();
  }

  public String getDelegationToken(final String owner, final String renewer, String remoteAddr)
          throws IOException,
          InterruptedException {
    /**
     * If the user asking the token is same as the 'owner' then don't do
     * any proxy authorization checks. For cases like oozie, where it gets
     * a delegation token for another user, we need to make sure oozie is
     * authorized to get a delegation token.
     */
    // Do all checks on short names
    UserGroupInformation currUser = UserGroupInformation.getCurrentUser();
    UserGroupInformation ownerUgi = UserGroupInformation.createRemoteUser(owner);
    if (!ownerUgi.getShortUserName().equals(currUser.getShortUserName())) {
      // in the case of proxy users, the getCurrentUser will return the
      // real user (for e.g. oozie) due to the doAs that happened just before the
      // server started executing the method getDelegationToken in the MetaStore
      ownerUgi = UserGroupInformation.createProxyUser(owner, UserGroupInformation.getCurrentUser());
      ProxyUsers.authorize(ownerUgi, remoteAddr, null);
    }
    return ownerUgi.doAs(new PrivilegedExceptionAction<String>() {

      @Override
      public String run() throws IOException {
        return secretManager.getDelegationToken(renewer);
      }
    });
  }

  public String getDelegationTokenWithService(String owner, String renewer, String service,
                                              String remoteAddr)
          throws IOException, InterruptedException {
    String token = getDelegationToken(owner, renewer, remoteAddr);
    return Utils.addServiceToToken(token, service);
  }

  public long renewDelegationToken(String tokenStrForm)
          throws IOException {
    return secretManager.renewDelegationToken(tokenStrForm);
  }

  public String getUserFromToken(String tokenStr) throws IOException {
    return secretManager.getUserFromToken(tokenStr);
  }

  public void cancelDelegationToken(String tokenStrForm) throws IOException {
    secretManager.cancelDelegationToken(tokenStrForm);
  }

  /**
   * Verify token string
   * @param tokenStrForm
   * @return user name
   * @throws IOException
   */
  public synchronized String verifyDelegationToken(String tokenStrForm) throws IOException {
    Token<DelegationTokenIdentifier> t = new Token<DelegationTokenIdentifier>();
    t.decodeFromUrlString(tokenStrForm);

    DelegationTokenIdentifier id = getTokenIdentifier(t);
    secretManager.verifyToken(id, t.getPassword());
    return id.getUser().getShortUserName();
  }

  protected DelegationTokenIdentifier getTokenIdentifier(Token<DelegationTokenIdentifier> token)
          throws IOException {
    // turn bytes back into identifier for cache lookup
    ByteArrayInputStream buf = new ByteArrayInputStream(token.getIdentifier());
    DataInputStream in = new DataInputStream(buf);
    DelegationTokenIdentifier id = secretManager.createIdentifier();
    id.readFields(in);
    return id;
  }

  private DelegationTokenStore getTokenStore(Configuration conf) throws IOException {
    String tokenStoreClassName = conf.get(DELEGATION_TOKEN_STORE_CLS, "");
    if (StringUtils.isBlank(tokenStoreClassName)) {
      return new MemoryTokenStore();
    }
    try {
      Class<? extends DelegationTokenStore> storeClass =
              Class.forName(tokenStoreClassName).asSubclass(DelegationTokenStore.class);
      return ReflectionUtils.newInstance(storeClass, conf);
    } catch (ClassNotFoundException e) {
      throw new IOException("Error initializing delegation token store: " + tokenStoreClassName, e);
    }
  }


}
