//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package org.apache.spark.sql.cli;

import java.util.Iterator;
import java.util.Properties;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.Level;

public class CommonCliOptions {
  protected final Options OPTIONS = new Options();
  protected CommandLine commandLine;
  protected final String cliname;
  private boolean verbose = false;

  public CommonCliOptions(String cliname, boolean includeHiveConf) {
    this.cliname = cliname;
    this.OPTIONS.addOption(new Option("v", "verbose", false, "Verbose mode"));
    this.OPTIONS.addOption(new Option("h", "help", false, "Print help information"));
    if (includeHiveConf) {
      Options var10000 = this.OPTIONS;
      OptionBuilder.withValueSeparator();
      OptionBuilder.hasArgs(2);
      OptionBuilder.withArgName("property=value");
      OptionBuilder.withLongOpt("hiveconf");
      OptionBuilder.withDescription("Use value for given property");
      var10000.addOption(OptionBuilder.create());
    }

  }

  public static void splitAndSetLogger(final String propKey, final Properties confProps) {
    String propVal = confProps.getProperty(propKey);
    if (propVal.contains(",")) {
      String[] tokens = propVal.split(",");
      for (String token : tokens) {
        if (Level.getLevel(token) == null) {
          System.setProperty("hive.root.logger", token);
        } else {
          System.setProperty("hive.log.level", token);
        }
      }
    } else {
      System.setProperty(propKey, confProps.getProperty(propKey));
    }
  }

  public Properties addHiveconfToSystemProperties() {
    Properties confProps = this.commandLine.getOptionProperties("hiveconf");

    String propKey;
    for(Iterator i$ = confProps.stringPropertyNames().iterator();
        i$.hasNext();
        System.setProperty(propKey, confProps.getProperty(propKey))) {
      propKey = (String)i$.next();
      if (this.verbose) {
        System.err.println("hiveconf: " + propKey + "=" + confProps.getProperty(propKey));
      }
    }

    return confProps;
  }

  public void printUsage() {
    (new HelpFormatter()).printHelp(this.cliname, this.OPTIONS);
  }

  public void parse(String[] args) {
    try {
      this.commandLine = (new GnuParser()).parse(this.OPTIONS, args);
      if (this.commandLine.hasOption('h')) {
        this.printUsage();
        System.exit(1);
      }

      if (this.commandLine.hasOption('v')) {
        this.verbose = true;
      }
    } catch (ParseException var3) {
      System.err.println(var3.getMessage());
      this.printUsage();
      System.exit(1);
    }

  }

  public boolean isVerbose() {
    return this.verbose;
  }
}
