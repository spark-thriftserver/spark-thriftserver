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

package org.apache.spark.sql.service.utils;

import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.service.internal.ServiceConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class VariableSubstitution extends SystemVariables {
  private static final Logger l4j = LoggerFactory.getLogger(VariableSubstitution.class);

  private final Map<String, String> hiveVariableSource;

  public VariableSubstitution(Map<String, String> hiveVariableSource) {
    this.hiveVariableSource = hiveVariableSource;
  }

  /**
   * The super method will handle with the case of substitutions for system variables,
   * hive conf variables and env variables. In this method, it will retrieve the hive
   * variables using hiveVariableSource.
   *
   * @param conf
   * @param var
   * @return
   */
  @Override
  protected String getSubstitute(SQLConf conf, String var) {
    String val = super.getSubstitute(conf, var);
    if (val == null && hiveVariableSource != null) {
      if (var.startsWith(SPARKVAR_PREFIX)) {
        val = hiveVariableSource.get(var.substring(SPARKVAR_PREFIX.length()));
      } else {
        val = hiveVariableSource.get(var);
      }
    }
    return val;
  }

  public String substitute(SQLConf conf, String expr) {
    if (expr == null) {
      return expr;
    }
    if ((boolean) conf.getConf(ServiceConf.THRIFTSERVER_VARIABLE_SUBSTITUTE())) {
      l4j.debug("Substitution is on: " + expr);
    } else {
      return expr;
    }
    int depth = (int) conf.getConf(ServiceConf.THRIFTSERVER_VARIABLE_SUBSTITUTE_DEPTH());
    return substitute(conf, expr, depth);
  }
}
