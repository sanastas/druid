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

import com.google.common.base.Supplier;
import com.google.common.collect.Maps;
import io.druid.data.input.InputRow;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.ColumnSelectorFactory;

import java.util.Map;
import java.util.function.Function;

import io.druid.segment.column.ColumnCapabilitiesImpl;
import io.druid.segment.incremental.IncrementalIndex.TimeAndDims;
import java.nio.ByteBuffer;

public class OffheapAggsManager extends AggsManager<BufferAggregator>
{
  private static final Logger log = new Logger(OffheapAggsManager.class);

  public volatile Map<String, ColumnSelectorFactory> selectors;
  public volatile int[] aggOffsetInBuffer;
  public volatile int aggsTotalSize;
  private Function<TimeAndDims, AggBufferInfo> getAggsBuffer;

  /* basic constractor */
  OffheapAggsManager(
      final IncrementalIndexSchema incrementalIndexSchema,
      final boolean deserializeComplexMetrics,
      final boolean reportParseExceptions,
      final boolean concurrentEventAdd,
      Supplier<InputRow> rowSupplier,
      Function<TimeAndDims, AggBufferInfo> getAggsBuffer,
      Map<String, ColumnCapabilitiesImpl> columnCapabilities,
      IncrementalIndex incrementalIndex
  )
  {
    super(incrementalIndexSchema, deserializeComplexMetrics, reportParseExceptions,
            concurrentEventAdd, rowSupplier, columnCapabilities, incrementalIndex);
    this.getAggsBuffer = getAggsBuffer;
  }

  @Override
  protected BufferAggregator[] initAggs(
      AggregatorFactory[] metrics,
      Supplier<InputRow> rowSupplier,
      boolean deserializeComplexMetrics,
      boolean concurrentEventAdd
  )
  {
    selectors = Maps.newHashMap();
    aggOffsetInBuffer = new int[metrics.length];

    for (int i = 0; i < metrics.length; i++) {
      AggregatorFactory agg = metrics[i];

      ColumnSelectorFactory columnSelectorFactory = makeColumnSelectorFactory(
              agg,
              rowSupplier,
              deserializeComplexMetrics
      );

      selectors.put(
              agg.getName(),
              new OnheapIncrementalIndex.ObjectCachingColumnSelectorFactory(columnSelectorFactory, concurrentEventAdd)
      );

      if (i == 0) {
        aggOffsetInBuffer[i] = metrics[i].getMaxIntermediateSize();
      } else {
        aggOffsetInBuffer[i] = aggOffsetInBuffer[i - 1] + metrics[i - 1].getMaxIntermediateSize();
      }
    }
    aggsTotalSize += aggOffsetInBuffer[metrics.length - 1] + metrics[metrics.length - 1].getMaxIntermediateSize();

    return new BufferAggregator[metrics.length];
  }

  @Override
  public BufferAggregator[] getAggsForRow(TimeAndDims timeAndDims)
  {
    return getAggs();
  }

  @Override
  public Object getAggVal(TimeAndDims timeAndDims, int aggIndex)
  {
    BufferAggregator agg = getAggs()[aggIndex];
    AggBufferInfo aggBufferInfo = getAggsBuffer.apply(timeAndDims);
    return agg.get(aggBufferInfo.aggBuffer, aggBufferInfo.position + aggOffsetInBuffer[aggIndex]);
  }

  @Override
  public float getMetricFloatValue(TimeAndDims timeAndDims, int aggIndex)
  {
    BufferAggregator agg = getAggs()[aggIndex];
    AggBufferInfo aggBufferInfo = getAggsBuffer.apply(timeAndDims);
    return agg.getFloat(aggBufferInfo.aggBuffer, aggBufferInfo.position + aggOffsetInBuffer[aggIndex]);
  }

  @Override
  public long getMetricLongValue(TimeAndDims timeAndDims, int aggIndex)
  {
    BufferAggregator agg = getAggs()[aggIndex];
    AggBufferInfo aggBufferInfo = getAggsBuffer.apply(timeAndDims);
    return agg.getLong(aggBufferInfo.aggBuffer, aggBufferInfo.position + aggOffsetInBuffer[aggIndex]);
  }

  @Override
  public Object getMetricObjectValue(TimeAndDims timeAndDims, int aggIndex)
  {
    BufferAggregator agg = getAggs()[aggIndex];
    AggBufferInfo aggBufferInfo = getAggsBuffer.apply(timeAndDims);
    return agg.get(aggBufferInfo.aggBuffer, aggBufferInfo.position + aggOffsetInBuffer[aggIndex]);
  }

  @Override
  public double getMetricDoubleValue(TimeAndDims timeAndDims, int aggIndex)
  {
    BufferAggregator agg = getAggs()[aggIndex];
    AggBufferInfo aggBufferInfo = getAggsBuffer.apply(timeAndDims);
    return agg.getDouble(aggBufferInfo.aggBuffer, aggBufferInfo.position + aggOffsetInBuffer[aggIndex]);
  }

  @Override
  public boolean isNull(TimeAndDims timeAndDims, int aggIndex)
  {
    BufferAggregator agg = getAggs()[aggIndex];
    AggBufferInfo aggBufferInfo = getAggsBuffer.apply(timeAndDims);
    return agg.isNull(aggBufferInfo.aggBuffer, aggBufferInfo.position + aggOffsetInBuffer[aggIndex]);
  }

  public void clearSelectors()
  {
    if (selectors != null) {
      selectors.clear();
    }
  }

  public static class AggBufferInfo
  {
    ByteBuffer aggBuffer;
    Integer position;

    public AggBufferInfo(ByteBuffer aggBuffer, Integer position)
    {
      this.aggBuffer = aggBuffer;
      this.position = position;
    }
  }
}
