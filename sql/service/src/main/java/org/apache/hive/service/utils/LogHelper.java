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
