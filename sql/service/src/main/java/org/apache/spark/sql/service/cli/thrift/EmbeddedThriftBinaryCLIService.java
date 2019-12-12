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

package org.apache.spark.sql.service.cli.thrift;

import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.service.cli.CLIService;
import org.apache.spark.sql.service.cli.ICLIService;


/**
 * EmbeddedThriftBinaryCLIService.
 *
 */
public class EmbeddedThriftBinaryCLIService extends ThriftBinaryCLIService {

  public EmbeddedThriftBinaryCLIService() {
    super(new CLIService(null, null), null);
    isEmbedded = true;
  }

  @Override
  public synchronized void init(SQLConf conf) {
    // Null SQLConf is passed in jdbc driver side code since driver side is supposed to be
    // independent of sqlConf object. Get new SQLConf object here in this case.
    if (conf == null) {
        sqlConf = SQLConf.get();
    }
    cliService.init(sqlConf);
    cliService.start();
    super.init(sqlConf);
  }

  public ICLIService getService() {
    return cliService;
  }
}
