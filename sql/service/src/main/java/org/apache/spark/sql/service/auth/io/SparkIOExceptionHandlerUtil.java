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
package org.apache.spark.sql.service.auth.io;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;

public class SparkIOExceptionHandlerUtil {

  private static final ThreadLocal<SparkIOExceptionHandlerChain> handlerChainInstance =
    new ThreadLocal<SparkIOExceptionHandlerChain>();

  private static synchronized SparkIOExceptionHandlerChain get(JobConf job) {
    SparkIOExceptionHandlerChain cache = SparkIOExceptionHandlerUtil.handlerChainInstance
        .get();
    if (cache == null) {
      SparkIOExceptionHandlerChain toSet = SparkIOExceptionHandlerChain
          .getSparkIOExceptionHandlerChain(job);
      handlerChainInstance.set(toSet);
      cache = SparkIOExceptionHandlerUtil.handlerChainInstance.get();
    }
    return cache;
  }

  /**
   * Handle exception thrown when creating record reader. In case that there is
   * an exception raised when construction the record reader and one handler can
   * handle this exception, it should return an record reader, which is either a
   * dummy empty record reader or a specific record reader that do some magic.
   *
   * @param e
   * @param job
   * @return RecordReader
   * @throws IOException
   */
  public static RecordReader handleRecordReaderCreationException(Exception e,
                                                                 JobConf job) throws IOException {
    SparkIOExceptionHandlerChain ioExpectionHandlerChain = get(job);
    if (ioExpectionHandlerChain != null) {
      return ioExpectionHandlerChain.handleRecordReaderCreationException(e);
    }
    throw new IOException(e);
  }

  /**
   * Handle exception thrown when calling record reader's next. If this
   * exception is handled by one handler, will just return true. Otherwise,
   * either re-throw this exception in one handler or at the end of the handler
   * chain.
   *
   * @param e
   * @param job
   * @return true on success
   * @throws IOException
   */
  public static boolean handleRecordReaderNextException(Exception e, JobConf job)
      throws IOException {
    SparkIOExceptionHandlerChain ioExpectionHandlerChain = get(job);
    if (ioExpectionHandlerChain != null) {
      return ioExpectionHandlerChain.handleRecordReaderNextException(e);
    }
    throw new IOException(e);
  }

}
