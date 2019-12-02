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
package org.apache.spark.sql.service.auth.shims;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.security.authentication.util.KerberosName;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URI;
import java.security.AccessControlException;

/**
 * Implemention of shims against Hadoop 0.23.0.
 */
public class Hadoop23Shims extends HadoopShimsSecure {

  public Hadoop23Shims() {
  }

  class ProxyFileSystem23 extends ProxyFileSystem {
    public ProxyFileSystem23(FileSystem fs) {
      super(fs);
    }

    public ProxyFileSystem23(FileSystem fs, URI uri) {
      super(fs, uri);
    }

    @Override
    public RemoteIterator<LocatedFileStatus> listLocatedStatus(final Path f)
            throws FileNotFoundException, IOException {
      return new RemoteIterator<LocatedFileStatus>() {
        private final RemoteIterator<LocatedFileStatus> stats =
                ProxyFileSystem23.super.listLocatedStatus(
                        ProxyFileSystem23.super.swizzleParamPath(f));

        @Override
        public boolean hasNext() throws IOException {
          return stats.hasNext();
        }

        @Override
        public LocatedFileStatus next() throws IOException {
          LocatedFileStatus result = stats.next();
          return new LocatedFileStatus(
                  ProxyFileSystem23.super.swizzleFileStatus(result, false),
                  result.getBlockLocations());
        }
      };
    }

    /**
     * Proxy file system also needs to override the access() method behavior.
     * Cannot add Override annotation since FileSystem.access() may not exist in
     * the version of hadoop used to build Hive.
     */
    @Override
    public void access(Path path, FsAction action) throws AccessControlException,
            FileNotFoundException, IOException {
      Path underlyingFsPath = swizzleParamPath(path);
      FileStatus underlyingFsStatus = fs.getFileStatus(underlyingFsPath);
      try {
        if (accessMethod != null) {
          accessMethod.invoke(fs, underlyingFsPath, action);
        } else {
          // If the FS has no access() method, we can try DefaultFileAccess ..
          DefaultFileAccess.checkFileAccess(fs, underlyingFsStatus, action);
        }
      } catch (AccessControlException err) {
        throw err;
      } catch (FileNotFoundException err) {
        throw err;
      } catch (IOException err) {
        throw err;
      } catch (Exception err) {
        throw new RuntimeException(err.getMessage(), err);
      }
    }
  }

  @Override
  public FileSystem createProxyFileSystem(FileSystem fs, URI uri) {
    return new ProxyFileSystem23(fs, uri);
  }

  protected static final Method accessMethod;
  protected static final Method getPasswordMethod;

  static {
    Method m = null;
    try {
      m = FileSystem.class.getMethod("access", Path.class, FsAction.class);
    } catch (NoSuchMethodException err) {
      // This version of Hadoop does not support FileSystem.access().
    }
    accessMethod = m;

    try {
      m = Configuration.class.getMethod("getPassword", String.class);
    } catch (NoSuchMethodException err) {
      // This version of Hadoop does not support getPassword(), just retrieve password from conf.
      m = null;
    }
    getPasswordMethod = m;
  }

  @Override
  public String getPassword(Configuration conf, String name) throws IOException {
    if (getPasswordMethod == null) {
      // Just retrieve value from conf
      return conf.get(name);
    } else {
      try {
        char[] pw = (char[]) getPasswordMethod.invoke(conf, name);
        if (pw == null) {
          return null;
        }
        return new String(pw);
      } catch (Exception err) {
        throw new IOException(err.getMessage(), err);
      }
    }
  }

  /**
   * Returns a shim to wrap KerberosName
   */
  @Override
  public KerberosNameShim getKerberosNameShim(String name) throws IOException {
    return new KerberosNameShim(name);
  }

  /**
   * Shim for KerberosName
   */
  public class KerberosNameShim implements HadoopShimsSecure.KerberosNameShim {

    private final KerberosName kerberosName;

    public KerberosNameShim(String name) {
      kerberosName = new KerberosName(name);
    }

    @Override
    public String getDefaultRealm() {
      return kerberosName.getDefaultRealm();
    }

    @Override
    public String getServiceName() {
      return kerberosName.getServiceName();
    }

    @Override
    public String getHostName() {
      return kerberosName.getHostName();
    }

    @Override
    public String getRealm() {
      return kerberosName.getRealm();
    }

    @Override
    public String getShortName() throws IOException {
      return kerberosName.getShortName();
    }
  }

}