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

package org.apache.spark.sql.thriftserver.utils;

import java.io.IOException;
import javax.security.auth.login.LoginException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;

import org.apache.spark.sql.thriftserver.auth.thrift.DelegationTokenIdentifier;

public class Utils {

  public static UserGroupInformation getUGI() throws LoginException, IOException {
    String doAs = System.getenv("HADOOP_USER_NAME");
    if(doAs != null && doAs.length() > 0) {
      /*
       * this allows doAs (proxy user) to be passed along across process boundary where
       * delegation tokens are not supported.  For example, a DDL stmt via WebHCat with
       * a doAs parameter, forks to 'hcat' which needs to start a Session that
       * proxies the end user
       */
      return UserGroupInformation.createProxyUser(doAs, UserGroupInformation.getLoginUser());
    }
    return UserGroupInformation.getCurrentUser();
  }

  /**
   * Add a given service to delegation token string.
   */
  public static String addServiceToToken(
      String tokenStr,
      String tokenService) throws IOException {
    Token<DelegationTokenIdentifier> delegationToken = createToken(tokenStr, tokenService);
    return delegationToken.encodeToUrlString();
  }

  /**
   * Create a new token using the given string and service
   */
  private static Token<DelegationTokenIdentifier> createToken(
      String tokenStr,
      String tokenService) throws IOException {
    Token<DelegationTokenIdentifier> delegationToken = new Token<DelegationTokenIdentifier>();
    delegationToken.decodeFromUrlString(tokenStr);
    delegationToken.setService(new Text(tokenService));
    return delegationToken;
  }
}
