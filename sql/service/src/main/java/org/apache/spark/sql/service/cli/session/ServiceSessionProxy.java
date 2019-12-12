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

/**
 * Proxy wrapper on ServiceSession to execute operations
 * by impersonating given user
 */
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.lang.reflect.UndeclaredThrowableException;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.security.UserGroupInformation;

import org.apache.spark.sql.service.cli.ServiceSQLException;

public class ServiceSessionProxy implements InvocationHandler {
  private final ServiceSession base;
  private final UserGroupInformation ugi;

  public ServiceSessionProxy(ServiceSession serviceSession, UserGroupInformation ugi) {
    this.base = serviceSession;
    this.ugi = ugi;
  }

  public static ServiceSession getProxy(ServiceSession serviceSession, UserGroupInformation ugi)
      throws IllegalArgumentException, ServiceSQLException {
    return (ServiceSession)Proxy.newProxyInstance(ServiceSession.class.getClassLoader(),
        new Class<?>[] {ServiceSession.class},
        new ServiceSessionProxy(serviceSession, ugi));
  }

  @Override
  public Object invoke(Object arg0, final Method method, final Object[] args)
      throws Throwable {
    try {
      if (method.getDeclaringClass() == ServiceSessionBase.class) {
        return invoke(method, args);
      }
      return ugi.doAs(
        new PrivilegedExceptionAction<Object>() {
          @Override
          public Object run() throws ServiceSQLException {
            return invoke(method, args);
          }
        });
    } catch (UndeclaredThrowableException e) {
      Throwable innerException = e.getCause();
      if (innerException instanceof PrivilegedActionException) {
        throw innerException.getCause();
      } else {
        throw e.getCause();
      }
    }
  }

  private Object invoke(final Method method, final Object[] args) throws ServiceSQLException {
    try {
      return method.invoke(base, args);
    } catch (InvocationTargetException e) {
      if (e.getCause() instanceof ServiceSQLException) {
        throw (ServiceSQLException)e.getCause();
      }
      throw new RuntimeException(e.getCause());
    } catch (IllegalArgumentException e) {
      throw new RuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }
}

