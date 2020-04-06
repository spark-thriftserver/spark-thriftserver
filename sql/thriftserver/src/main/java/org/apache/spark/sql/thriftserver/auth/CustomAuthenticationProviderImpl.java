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

package org.apache.spark.sql.thriftserver.auth;

import javax.security.sasl.AuthenticationException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.thriftserver.internal.ServiceConf;

/**
 * This authentication provider implements the {@code CUSTOM} authentication. It allows a {@link
 * PasswdAuthenticationProvider} to be specified at configuration time which may additionally
 * implement {@link org.apache.hadoop.conf.Configurable Configurable} to grab Hive's {@link
 * org.apache.hadoop.conf.Configuration Configuration}.
 */
public class CustomAuthenticationProviderImpl implements PasswdAuthenticationProvider {

  private final PasswdAuthenticationProvider customProvider;

  @SuppressWarnings("unchecked")
  CustomAuthenticationProviderImpl(SparkSession spark) {
    Configuration hadoopConf = spark.sparkContext().hadoopConfiguration();
    hadoopConf.set(ServiceConf.THRIFTSERVER_CUSTOM_AUTHENTICATION_CLASS().key(),
      ServiceConf.customAuthenticationClass(spark.sessionState().conf()));
    Class<? extends PasswdAuthenticationProvider> customHandlerClass =
      (Class<? extends PasswdAuthenticationProvider>) hadoopConf.getClass(
        ServiceConf.THRIFTSERVER_CUSTOM_AUTHENTICATION_CLASS().key(),
        PasswdAuthenticationProvider.class);
    customProvider = ReflectionUtils.newInstance(customHandlerClass, hadoopConf);
  }

  @Override
  public void authenticate(String user, String password) throws AuthenticationException {
    customProvider.authenticate(user, password);
  }

}
