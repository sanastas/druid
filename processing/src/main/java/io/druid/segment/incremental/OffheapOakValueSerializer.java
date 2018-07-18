/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment.incremental;

import com.google.common.collect.Maps;
import io.druid.data.input.InputRow;
import io.druid.data.input.Row;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import io.druid.data.input.MapBasedRow;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.DimensionHandler;
import io.druid.segment.DimensionIndexer;
import io.druid.segment.incremental.IncrementalIndex.DimensionDesc;
import oak.ValueSerializer;

public class OffheapOakValueSerializer implements ValueSerializer<IncrementalIndexRow, Row>
{

  private List<DimensionDesc> dimensionDescsList;
  private OffheapAggsManager aggsManager;
  private boolean reportParseExceptions;
  private ThreadLocal<InputRow> rowContainer;

  public OffheapOakValueSerializer(
          List<DimensionDesc> dimensionDescsList,
          OffheapAggsManager aggsManager,
          boolean reportParseExceptions,
          ThreadLocal<InputRow> rowContainer
  )
  {
    this.dimensionDescsList = dimensionDescsList;
    this.aggsManager = aggsManager;
    this.reportParseExceptions = reportParseExceptions;
    this.rowContainer = rowContainer;
  }

  @Override
  public void serialize(IncrementalIndexRow key, Row value, ByteBuffer byteBuffer)
  {
    aggsManager.initValue(byteBuffer, reportParseExceptions, (InputRow) value, rowContainer);
  }

  @Override
  public Row deserialize(ByteBuffer serializedKey, ByteBuffer serializedValue)
  {
    long timeStamp = OffheapOakIncrementalIndex.getTimestamp(serializedKey);
    int dimsLength = OffheapOakIncrementalIndex.getDimsLength(serializedKey);
    Map<String, Object> theVals = Maps.newLinkedHashMap();
    for (int i = 0; i < dimsLength; ++i) {
      Object dim = OffheapOakIncrementalIndex.getDimValue(serializedKey, i);
      DimensionDesc dimensionDesc = dimensionDescsList.get(i);
      if (dimensionDesc == null) {
        continue;
      }
      String dimensionName = dimensionDesc.getName();
      DimensionHandler handler = dimensionDesc.getHandler();
      if (dim == null || handler.getLengthOfEncodedKeyComponent(dim) == 0) {
        theVals.put(dimensionName, null);
        continue;
      }
      final DimensionIndexer indexer = dimensionDesc.getIndexer();
      Object rowVals = indexer.convertUnsortedEncodedKeyComponentToActualArrayOrList(dim, DimensionIndexer.LIST);
      theVals.put(dimensionName, rowVals);
    }

    BufferAggregator[] aggs = aggsManager.getAggs();
    for (int i = 0; i < aggs.length; ++i) {
      theVals.put(aggsManager.metrics[i].getName(), aggs[i].get(serializedValue, aggsManager.aggOffsetInBuffer[i]));
    }

    return new MapBasedRow(timeStamp, theVals);
  }

}
