/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment.incremental;

import io.druid.data.input.InputRow;
import io.druid.java.util.common.logger.Logger;
import oak.WritableOakBuffer;

import java.util.function.Consumer;

/**
 * For sending as input parameter to the Oak's putifAbsentComputeIfPresent method
 */
public class OffheapOakComputeConsumer implements Consumer<WritableOakBuffer>
{
  private static final Logger log = new Logger(OffheapOakComputeConsumer.class);
  OffheapAggsManager aggsManager;
  boolean reportParseExceptions;
  InputRow row;
  ThreadLocal<InputRow> rowContainer;
  int executions; // for figuring out whether a put or a compute was executed

  public OffheapOakComputeConsumer(
          OffheapAggsManager aggsManager,
          boolean reportParseExceptions,
          InputRow row,
          ThreadLocal<InputRow> rowContainer
  )
  {
    this.aggsManager = aggsManager;
    this.reportParseExceptions = reportParseExceptions;
    this.row = row;
    this.rowContainer = rowContainer;
    this.executions = 0;
  }

  @Override
  public void accept(WritableOakBuffer writableOakBuffer)
  {
    aggsManager.aggregate(reportParseExceptions, row, rowContainer, writableOakBuffer.getByteBuffer());
    executions++;

  }

  public int executed()
  {
    return executions;
  }
}
