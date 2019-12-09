/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.jdbc.miniSS2;

import com.google.common.base.Preconditions;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.service.Service;
import org.apache.spark.sql.service.cli.CLIServiceClient;
import org.apache.spark.sql.service.cli.SessionHandle;
import org.apache.spark.sql.service.cli.thrift.ThriftBinaryCLIService;
import org.apache.spark.sql.service.cli.thrift.ThriftCLIServiceClient;
import org.apache.spark.sql.service.cli.thrift.ThriftHttpCLIService;
import org.apache.spark.sql.service.internal.ServiceConf;
import org.apache.spark.sql.service.server.SparkServer2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public class MiniSS2 extends AbstractSparkService {

  private static final Logger LOG = LoggerFactory.getLogger(MiniSS2.class);

  public static final String HS2_BINARY_MODE = "binary";
  public static final String HS2_HTTP_MODE = "http";
  private static final String driverName = "org.apache.spark.sql.jdbc.SparkDriver";
  private static final FsPermission FULL_PERM = new FsPermission((short) 00777);
  private static final FsPermission WRITE_ALL_PERM = new FsPermission((short) 00733);
  private static final String tmpDir = System.getProperty("test.tmp.dir");
  private SparkServer2 sparkServer2 = null;
  private final File baseDir;
  private final Path baseFsDir;
  private final FileSystem localFS;
  private boolean useMiniKdc = false;
  private final String serverPrincipal;
  private boolean cleanupLocalDirOnStartup = false;

  public static class Builder {
    private SQLContext sqlContext = null;
    private boolean useMiniKdc = false;
    private String serverPrincipal;
    private String serverKeytab;
    private boolean isHTTPTransMode = false;
    private boolean usePortsFromConf = false;
    private String authType = "KERBEROS";
    private boolean isHA = false;
    private boolean cleanupLocalDirOnStartup;

    public Builder() {
    }

    public Builder withMiniMR() {
      return this;
    }

    public Builder withMiniKdc(String serverPrincipal, String serverKeytab) {
      this.useMiniKdc = true;
      this.serverPrincipal = serverPrincipal;
      this.serverKeytab = serverKeytab;
      return this;
    }

    public Builder withAuthenticationType(String authType) {
      this.authType = authType;
      return this;
    }

    public Builder withSQLContext(SQLContext sqlContext) {
      this.sqlContext = sqlContext;
      return this;
    }

    public Builder withHA() {
      this.isHA = true;
      return this;
    }

    /**
     * Start HS2 with HTTP transport mode, default is binary mode
     *
     * @return this Builder
     */
    public Builder withHTTPTransport() {
      this.isHTTPTransMode = true;
      return this;
    }

    public Builder cleanupLocalDirOnStartup(boolean val) {
      this.cleanupLocalDirOnStartup = val;
      return this;
    }

    public MiniSS2 build() throws Exception {
      if (isHTTPTransMode) {
        sqlContext.setConf(ServiceConf.THRIFTSERVER_TRANSPORT_MODE(), HS2_HTTP_MODE);
      } else {
        sqlContext.setConf(ServiceConf.THRIFTSERVER_TRANSPORT_MODE(), HS2_BINARY_MODE);
      }
      return new MiniSS2(sqlContext, useMiniKdc, serverPrincipal, serverKeytab,
          usePortsFromConf, authType, cleanupLocalDirOnStartup);
    }
  }


  public FileSystem getLocalFS() {
    return localFS;
  }

  public boolean isUseMiniKdc() {
    return useMiniKdc;
  }

  private MiniSS2(SQLContext sqlContext, boolean useMiniKdc,
                  String serverPrincipal, String serverKeytab,
                  boolean usePortsFromConf, String authType, boolean cleanupLocalDirOnStartup) throws Exception {
    // Always use localhost for hostname as some tests like SSL CN validation ones
    // are tied to localhost being present in the certificate name
    super(
        sqlContext,
        "localhost",
        (usePortsFromConf ? (int) sqlContext.conf().getConf(ServiceConf.THRIFTSERVER_THRIFT_PORT()) : findFreePort()),
        (usePortsFromConf ? (int) sqlContext.conf().getConf(ServiceConf.THRIFTSERVER_HTTP_PORT()) : findFreePort()));
    this.useMiniKdc = useMiniKdc;
    this.serverPrincipal = serverPrincipal;
    this.cleanupLocalDirOnStartup = cleanupLocalDirOnStartup;
    this.baseDir = getBaseDir();
    localFS = FileSystem.getLocal(sqlContext.sparkContext().hadoopConfiguration());
    FileSystem fs;

    // This is FS only mode, just initialize the dfs root directory.
    fs = FileSystem.getLocal(sqlContext.sparkContext().hadoopConfiguration());
    baseFsDir = new Path("file://" + baseDir.toURI().getPath());

    if (cleanupLocalDirOnStartup) {
      // Cleanup baseFsDir since it can be shared across tests.
      LOG.info("Attempting to cleanup baseFsDir: {} while setting up MiniSS2", baseDir);
      Preconditions.checkState(baseFsDir.depth() >= 3); // Avoid "/tmp", directories closer to "/"
      fs.delete(baseFsDir, true);
    }

    if (useMiniKdc) {
      sqlContext.setConf(ServiceConf.THRIFTSERVER_KERBEROS_PRINCIPAL(), serverPrincipal);
      sqlContext.setConf(ServiceConf.THRIFTSERVER_KERBEROS_KEYTAB(), serverKeytab);
      sqlContext.setConf(ServiceConf.THRIFTSERVER_AUTHENTICATION(), authType);
    }
    String metaStoreURL =
        "jdbc:derby:;databaseName=" + baseDir.getAbsolutePath() + File.separator
            + "test_metastore;create=true";

    fs.mkdirs(baseFsDir);
    Path wareHouseDir = new Path(baseFsDir, "warehouse");
    // Create warehouse with 777, so that user impersonation has no issues.
    FileSystem.mkdirs(fs, wareHouseDir, FULL_PERM);

    fs.mkdirs(wareHouseDir);
    setWareHouseDir(wareHouseDir.toString());
    if (!usePortsFromConf) {
      // reassign a new port, just in case if one of the MR services grabbed the last one
      setBinaryPort(findFreePort());
    }
    sqlContext.setConf(ServiceConf.THRIFTSERVER_THRIFT_BIND_HOST(), getHost());
    sqlContext.setConf(ServiceConf.THRIFTSERVER_THRIFT_PORT(), getBinaryPort());
    sqlContext.setConf(ServiceConf.THRIFTSERVER_HTTP_PORT(), getHttpPort());
  }

  public MiniSS2(SQLContext sqlContext) throws Exception {
    this(sqlContext, false);
  }

  public MiniSS2(SQLContext sqlContext, boolean usePortsFromConf) throws Exception {
    this(sqlContext, false, null, null, usePortsFromConf,
        "KERBEROS", true);
  }

  public void start(Map<String, String> confOverlay) throws Exception {

    sparkServer2 = new SparkServer2(getSqlContext());
    // Set confOverlay parameters
    for (Map.Entry<String, String> entry : confOverlay.entrySet()) {
      setConfProperty(entry.getKey(), entry.getValue());
    }
    sparkServer2.init(getSqlConf());
    sparkServer2.start();
    waitForStartup();
    setStarted(true);
  }

  public void stop() {
    verifyStarted();
    // Currently there is no way to stop the MetaStore service. It will be stopped when the
    // test JVM exits. This is how other tests are also using MetaStore server.

    sparkServer2.stop();
    setStarted(false);
  }

  public void cleanup() {
    FileUtils.deleteQuietly(baseDir);
  }


  public CLIServiceClient getServiceClient() {
    verifyStarted();
    return getServiceClientInternal();
  }

  public SQLConf getServerConf() {
    if (sparkServer2 != null) {
      return sparkServer2.getSqlConf();
    }
    return null;
  }

  public CLIServiceClient getServiceClientInternal() {
    for (Service service : sparkServer2.getServices()) {
      if (service instanceof ThriftBinaryCLIService) {
        return new ThriftCLIServiceClient((ThriftBinaryCLIService) service);
      }
      if (service instanceof ThriftHttpCLIService) {
        return new ThriftCLIServiceClient((ThriftHttpCLIService) service);
      }
    }
    throw new IllegalStateException("SparkServer2 not running Thrift service");
  }

  /**
   * return connection URL for this server instance
   *
   * @return
   * @throws Exception
   */
  public String getJdbcURL() throws Exception {
    return getJdbcURL("default");
  }

  /**
   * return connection URL for this server instance
   *
   * @param dbName - DB name to be included in the URL
   * @return
   * @throws Exception
   */
  public String getJdbcURL(String dbName) throws Exception {
    return getJdbcURL(dbName, "");
  }

  /**
   * return connection URL for this server instance
   *
   * @param dbName         - DB name to be included in the URL
   * @param sessionConfExt - Addional string to be appended to sessionConf part of url
   * @return
   * @throws Exception
   */
  public String getJdbcURL(String dbName, String sessionConfExt) throws Exception {
    return getJdbcURL(dbName, sessionConfExt, "");
  }

  /**
   * return connection URL for this server instance
   *
   * @param dbName         - DB name to be included in the URL
   * @param sessionConfExt - Addional string to be appended to sessionConf part of url
   * @param sparkConfExt    - Additional string to be appended to HiveConf part of url (excluding the ?)
   * @return
   * @throws Exception
   */
  public String getJdbcURL(String dbName, String sessionConfExt, String sparkConfExt)
      throws Exception {
    sessionConfExt = (sessionConfExt == null ? "" : sessionConfExt);
    sparkConfExt = (sparkConfExt == null ? "" : sparkConfExt);
    // Strip the leading ";" if provided
    // (this is the assumption with which we're going to start configuring sessionConfExt)
    if (sessionConfExt.startsWith(";")) {
      sessionConfExt = sessionConfExt.substring(1);
    }
    if (isUseMiniKdc()) {
      sessionConfExt = "principal=" + serverPrincipal + ";" + sessionConfExt;
    }
    if (isHttpTransportMode()) {
      sessionConfExt = "transportMode=http;httpPath=cliservice" + ";" + sessionConfExt;
    }
    String baseJdbcURL;

    baseJdbcURL = getBaseJdbcURL();

    baseJdbcURL = baseJdbcURL + dbName;
    if (!sessionConfExt.isEmpty()) {
      baseJdbcURL = baseJdbcURL + ";" + sessionConfExt;
    }
    if ((sparkConfExt != null) && (!sparkConfExt.trim().isEmpty())) {
      baseJdbcURL = baseJdbcURL + "?" + sparkConfExt;
    }
    return baseJdbcURL;
  }

  /**
   * Build base JDBC URL
   *
   * @return
   */
  public String getBaseJdbcURL() {
    if (isHttpTransportMode()) {
      return "jdbc:spark://" + getHost() + ":" + getHttpPort() + "/";
    } else {
      return "jdbc:spark://" + getHost() + ":" + getBinaryPort() + "/";
    }
  }

  private boolean isHttpTransportMode() {
    String transportMode = getConfProperty(ServiceConf.THRIFTSERVER_TRANSPORT_MODE().key());
    return transportMode != null && (transportMode.equalsIgnoreCase(HS2_HTTP_MODE));
  }


  public static String getJdbcDriverName() {
    return driverName;
  }


  private void waitForStartup() throws Exception {
    int waitTime = 0;
    long startupTimeout = 1000L * 1000L;
    CLIServiceClient hs2Client = getServiceClientInternal();
    SessionHandle sessionHandle = null;
    do {
      Thread.sleep(500L);
      waitTime += 500L;
      if (waitTime > startupTimeout) {
        throw new TimeoutException("Couldn't access new SparkServer2: " + getJdbcURL());
      }
      try {
        Map<String, String> sessionConf = new HashMap<String, String>();
        /**
         if (isUseMiniKdc()) {
         getMiniKdc().loginUser(getMiniKdc().getDefaultUserPrincipal());
         sessionConf.put("principal", serverPrincipal);
         }
         */
        sessionHandle = hs2Client.openSession("foo", "bar", sessionConf);
      } catch (Exception e) {
        // service not started yet
        continue;
      }
      hs2Client.closeSession(sessionHandle);
      break;
    } while (true);
  }

  public Service.STATE getState() {
    return sparkServer2.getServiceState();
  }

  static File getBaseDir() {
    File baseDir = new File(tmpDir + "/local_base");
    return baseDir;
  }

  /**
   * Finds a free port on the machine
   *
   * @return
   * @throws IOException
   */
  public static int findFreePort() throws IOException {
    ServerSocket socket = new ServerSocket(0);
    int port = socket.getLocalPort();
    socket.close();
    return port;
  }
}