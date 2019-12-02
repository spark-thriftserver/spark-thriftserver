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
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.net.URI;

/**
 * In order to be compatible with multiple versions of Hadoop, all parts
 * of the Hadoop interface that are not cross-version compatible are
 * encapsulated in an implementation of this class. Users should use
 * the ShimLoader class as a factory to obtain an implementation of
 * HadoopShims corresponding to the version of Hadoop currently on the
 * classpath.
 */
public interface HadoopShims {

  /**
   * Create a proxy file system that can serve a given scheme/authority using some
   * other file system.
   */
  public FileSystem createProxyFileSystem(FileSystem fs, URI uri);

  /**
   * Use password API (if available) to fetch credentials/password
   * @param conf
   * @param name
   * @return
   */
  public String getPassword(Configuration conf, String name) throws IOException;

  /**
   * Returns a shim to wrap KerberosName
   */
  public KerberosNameShim getKerberosNameShim(String name) throws IOException;

  /**
   * Shim for KerberosName
   */
  public interface KerberosNameShim {
    public String getDefaultRealm();
    public String getServiceName();
    public String getHostName();
    public String getRealm();
    public String getShortName() throws IOException;
  }
}
