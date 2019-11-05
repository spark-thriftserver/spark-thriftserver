/**
 * This class provides helper routines to emit informational and error
 * messages to the user and log4j files while obeying the current session's
 * verbosity levels.
 *
 * NEVER write directly to the SessionStates standard output other than to
 * emit result data DO use printInfo and printError provided by LogHelper to
 * emit non result data strings.
 *
 * It is perfectly acceptable to have global static LogHelper objects (for
 * example - once per module) LogHelper always emits info/error to current
 * session as required.
 */

package org.apache.hive.service.utils;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.slf4j.Logger;

import java.io.PrintStream;

public class LogHelper {
  protected Logger LOG;
  protected boolean isSilent;

  public LogHelper(Logger LOG) {
    this(LOG, false);
  }

  public LogHelper(Logger LOG, boolean isSilent) {
    this.LOG = LOG;
    this.isSilent = isSilent;
  }

  public PrintStream getOutStream() {
    SessionState ss = SessionState.get();
    return ((ss != null) && (ss.out != null)) ? ss.out : System.out;
  }

  public PrintStream getInfoStream() {
    SessionState ss = SessionState.get();
    return ((ss != null) && (ss.info != null)) ? ss.info : getErrStream();
  }

  public PrintStream getErrStream() {
    SessionState ss = SessionState.get();
    return ((ss != null) && (ss.err != null)) ? ss.err : System.err;
  }

  public PrintStream getChildOutStream() {
    SessionState ss = SessionState.get();
    return ((ss != null) && (ss.childOut != null)) ? ss.childOut : System.out;
  }

  public PrintStream getChildErrStream() {
    SessionState ss = SessionState.get();
    return ((ss != null) && (ss.childErr != null)) ? ss.childErr : System.err;
  }

  public boolean getIsSilent() {
    SessionState ss = SessionState.get();
    // use the session or the one supplied in constructor
    return (ss != null) ? ss.getIsSilent() : isSilent;
  }

  public void logInfo(String info) {
    logInfo(info, null);
  }

  public void logInfo(String info, String detail) {
    LOG.info(info + StringUtils.defaultString(detail));
  }

  public void printInfo(String info) {
    printInfo(info, null);
  }

  public void printInfo(String info, String detail) {
    if (!getIsSilent()) {
      getInfoStream().println(info);
    }
    LOG.info(info + StringUtils.defaultString(detail));
  }

  public void printError(String error) {
    printError(error, null);
  }

  public void printError(String error, String detail) {
    getErrStream().println(error);
    LOG.error(error + StringUtils.defaultString(detail));
  }
}
