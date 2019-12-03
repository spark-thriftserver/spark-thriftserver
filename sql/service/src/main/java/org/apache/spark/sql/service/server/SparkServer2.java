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

package org.apache.spark.sql.service.server;

import java.util.Properties;

import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.service.CompositeService;
import org.apache.spark.sql.service.cli.CLIService;
import org.apache.spark.sql.service.cli.thrift.ThriftBinaryCLIService;
import org.apache.spark.sql.service.cli.thrift.ThriftCLIService;
import org.apache.spark.sql.service.cli.thrift.ThriftHttpCLIService;
import org.apache.spark.sql.service.internal.ServiceConf;
import scala.runtime.AbstractFunction0;
import scala.runtime.BoxedUnit;

import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.util.ShutdownHookManager;

/**
 * SparkServer2.
 *
 */
public class SparkServer2 extends CompositeService {
  private static final Logger LOG = LoggerFactory.getLogger(SparkServer2.class);

  private CLIService cliService;
  private ThriftCLIService thriftCLIService;
  private SQLContext sqlContext;

  public SparkServer2(SQLContext sqlContext) {
    super(SparkServer2.class.getSimpleName());
    this.sqlContext = sqlContext;
  }

  @Override
  public synchronized void init(SQLConf sqlConf) {
    cliService = new CLIService(this, sqlContext);
    addService(cliService);
    if (isHTTPTransportMode(sqlConf)) {
      thriftCLIService = new ThriftHttpCLIService(cliService, sqlContext);
    } else {
      thriftCLIService = new ThriftBinaryCLIService(cliService, sqlContext);
    }
    addService(thriftCLIService);
    super.init(sqlConf);

    // Add a shutdown hook for catching SIGTERM & SIGINT
    // this must be higher than the Hadoop Filesystem priority of 10,
    // which the default priority is.
    // The signature of the callback must match that of a scala () -> Unit
    // function
    ShutdownHookManager.addShutdownHook(
        new AbstractFunction0<BoxedUnit>() {
          public BoxedUnit apply() {
            try {
              LOG.info("Hive Server Shutdown hook invoked");
              stop();
            } catch (Throwable e) {
              LOG.warn("Ignoring Exception while stopping Hive Server from shutdown hook",
                  e);
            }
            return BoxedUnit.UNIT;
          }
        });
  }

  public static boolean isHTTPTransportMode(SQLConf sqlConf) {
    String transportMode = System.getenv("THRIFTSERVER_TRANSPORT_MODE");
    if (transportMode == null) {
      transportMode = sqlConf.getConf(ServiceConf.THRIFTSERVER_TRANSPORT_MODE());
    }
    if (transportMode != null && (transportMode.equalsIgnoreCase("http"))) {
      return true;
    }
    return false;
  }

  @Override
  public synchronized void start() {
    super.start();
  }

  @Override
  public synchronized void stop() {
    LOG.info("Shutting down SparkServer2");
    super.stop();
  }

  public static void main(String[] args) {
    try {
      ServerOptionsProcessor oproc = new ServerOptionsProcessor("sparkserver2");
      ServerOptionsProcessorResponse oprocResponse = oproc.parse(args);

      // Log debug message from "oproc" after log4j initialize properly
      LOG.debug(oproc.getDebugMessage().toString());

      // Call the executor which will execute the appropriate command based on the parsed options
      oprocResponse.getServerOptionsExecutor().execute();
    } catch (Exception e) {
      LOG.error("Error initializing log: " + e.getMessage(), e);
      System.exit(-1);
    }
  }

  /**
   * ServerOptionsProcessor.
   * Process arguments given to SparkServer2 (-sparkconf property=value)
   * Set properties in System properties
   * Create an appropriate response object,
   * which has executor to execute the appropriate command based on the parsed options.
   */
  public static class ServerOptionsProcessor {
    private final Options options = new Options();
    private org.apache.commons.cli.CommandLine commandLine;
    private final String serverName;
    private final StringBuilder debugMessage = new StringBuilder();

    @SuppressWarnings("static-access")
    public ServerOptionsProcessor(String serverName) {
      this.serverName = serverName;
      // -sparkconf x=y
      options.addOption(OptionBuilder
          .withValueSeparator()
          .hasArgs(2)
          .withArgName("property=value")
          .withLongOpt("sparkconf")
          .withDescription("Use value for given property")
          .create());
      options.addOption(new Option("H", "help", false, "Print help information"));
    }

    public ServerOptionsProcessorResponse parse(String[] argv) {
      try {
        commandLine = new GnuParser().parse(options, argv);
        // Process --sparkconf
        // Get sparkconf param values and set the System property values
        Properties confProps = commandLine.getOptionProperties("sparkconf");
        for (String propKey : confProps.stringPropertyNames()) {
          // save logging message for log4j output latter after log4j initialize properly
          debugMessage.append("Setting " + propKey + "=" + confProps.getProperty(propKey) + ";\n");
          System.setProperty(propKey, confProps.getProperty(propKey));
        }

        // Process --help
        if (commandLine.hasOption('H')) {
          return new ServerOptionsProcessorResponse(new HelpOptionExecutor(serverName, options));
        }
      } catch (ParseException e) {
        // Error out & exit - we were not able to parse the args successfully
        System.err.println("Error starting SparkServer2 with given arguments: ");
        System.err.println(e.getMessage());
        System.exit(-1);
      }
      // Default executor, when no option is specified
      return new ServerOptionsProcessorResponse(new StartOptionExecutor());
    }

    StringBuilder getDebugMessage() {
      return debugMessage;
    }
  }

  /**
   * The response sent back from {@link ServerOptionsProcessor#parse(String[])}
   */
  static class ServerOptionsProcessorResponse {
    private final ServerOptionsExecutor serverOptionsExecutor;

    ServerOptionsProcessorResponse(ServerOptionsExecutor serverOptionsExecutor) {
      this.serverOptionsExecutor = serverOptionsExecutor;
    }

    ServerOptionsExecutor getServerOptionsExecutor() {
      return serverOptionsExecutor;
    }
  }

  /**
   * The executor interface for running the appropriate SparkServer2 command based on parsed options
   */
  interface ServerOptionsExecutor {
    void execute();
  }

  /**
   * HelpOptionExecutor: executes the --help option by printing out the usage
   */
  static class HelpOptionExecutor implements ServerOptionsExecutor {
    private final Options options;
    private final String serverName;

    HelpOptionExecutor(String serverName, Options options) {
      this.options = options;
      this.serverName = serverName;
    }

    @Override
    public void execute() {
      new HelpFormatter().printHelp(serverName, options);
      System.exit(0);
    }
  }

  /**
   * StartOptionExecutor: starts SparkServer2.
   * This is the default executor, when no option is specified.
   */
  static class StartOptionExecutor implements ServerOptionsExecutor {
    @Override
    public void execute() {
      try {
        LOG.error("Don't support starting SparkServer2 here");
      } catch (Throwable t) {
        LOG.error("Error starting SparkServer2", t);
        System.exit(-1);
      }
    }
  }
}
