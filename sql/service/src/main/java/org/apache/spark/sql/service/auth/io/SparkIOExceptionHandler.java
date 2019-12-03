/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.service.auth.io;

import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;

/**
 * SparkIOExceptionHandler defines an interface that all io exception handler in
 * Hive should implement. Different IO exception handlers can implement
 * different logics based on the exception input into it.
 */
public interface SparkIOExceptionHandler {

  /**
   * process exceptions raised when creating a record reader.
   *
   * @param e
   * @return RecordReader
   */
  RecordReader<?, ?> handleRecordReaderCreationException(Exception e)
      throws IOException;

  /**
   * process exceptions thrown when calling rr's next
   *
   * @param e
   * @param result
   * @throws IOException
   */
  void handleRecorReaderNextException(Exception e,
                                      SparkIOExceptionNextHandleResult result)
      throws IOException;

}
