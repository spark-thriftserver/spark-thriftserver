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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A SparkServer2 thread used to construct new server threads.
 * In particular, this thread ensures an orderly cleanup,
 * when killed by its corresponding ExecutorService.
 */
public class ThreadWithGarbageCleanup extends Thread {
  private static final Logger LOG = LoggerFactory.getLogger(ThreadWithGarbageCleanup.class);

  public ThreadWithGarbageCleanup(Runnable runnable) {
    super(runnable);
  }

  /**
   * Add any Thread specific garbage cleanup code here.
   * Currently, it shuts down the RawStore object for this thread if it is not null.
   */
  @Override
  public void finalize() throws Throwable {
    cleanRawStore();
    super.finalize();
  }

  private void cleanRawStore() {
  }

  /**
   * Cache the ThreadLocal RawStore object. Called from the corresponding thread.
   */
  public void cacheThreadLocalRawStore() {
  }
}
