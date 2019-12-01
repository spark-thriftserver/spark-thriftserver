//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package org.apache.spark.sql.service.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.sql.internal.SQLConf;

public class SystemVariables {
  private static final Log l4j = LogFactory.getLog(SystemVariables.class);
  protected static Pattern varPat = Pattern.compile("\\$\\{[^\\}\\$ ]+\\}");
  public static final String ENV_PREFIX = "env:";
  public static final String SYSTEM_PREFIX = "system:";
  public static final String SPARKCONF_PREFIX = "sparkconf:";
  public static final String SPARKVAR_PREFIX = "sparkvar:";
  public static final String SET_COLUMN_NAME = "set";

  public SystemVariables() {
  }

  protected String getSubstitute(SQLConf conf, String var) {
    String val = null;

    try {
      if (var.startsWith("system:")) {
        val = System.getProperty(var.substring("system:".length()));
      }
    } catch (SecurityException var5) {
      l4j.warn("Unexpected SecurityException in Configuration", var5);
    }

    if (val == null && var.startsWith("env:")) {
      val = System.getenv(var.substring("env:".length()));
    }

    if (val == null && conf != null && var.startsWith("sparkconf:")) {
      val = conf.getConfString(var.substring("sparkconf:".length()));
    }

    return val;
  }

  public static boolean containsVar(String expr) {
    return expr != null && varPat.matcher(expr).find();
  }

  protected String substitute(String expr) {
    return expr == null ? null : (new SystemVariables()).substitute((SQLConf)null, expr, 1);
  }

  protected String substitute(SQLConf conf, String expr) {
    return expr == null ? null : (new SystemVariables()).substitute(conf, expr, 1);
  }

  protected String substitute(SQLConf conf, String expr, int depth) {
    Matcher match = varPat.matcher("");
    String eval = expr;
    StringBuilder builder = new StringBuilder();

    int s;
    for(s = 0; s <= depth; ++s) {
      match.reset(eval);
      builder.setLength(0);
      int prev = 0;

      boolean found;
      for(found = false; match.find(prev); prev = match.end()) {
        String group = match.group();
        String var = group.substring(2, group.length() - 1);
        String substitute = this.getSubstitute(conf, var);
        if (substitute == null) {
          substitute = group;
        } else {
          found = true;
        }

        builder.append(eval.substring(prev, match.start())).append(substitute);
      }

      if (!found) {
        return eval;
      }

      builder.append(eval.substring(prev));
      eval = builder.toString();
    }

    if (s > depth) {
      throw new IllegalStateException("Variable substitution depth is deeper than " + depth + " for expression " + expr);
    } else {
      return eval;
    }
  }
}
