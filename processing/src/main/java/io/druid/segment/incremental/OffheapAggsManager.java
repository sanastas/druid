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
import io.druid.collections.NonBlockingPool;
import io.druid.data.input.InputRow;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.ColumnSelectorFactory;

import java.util.Map;

import io.druid.segment.column.ColumnCapabilitiesImpl;

import java.nio.ByteBuffer;

public class OffheapAggsManager extends AggsManager<BufferAggregator>
{
  private static final Logger log = new Logger(OffheapAggsManager.class);

  public volatile Map<String, ColumnSelectorFactory> selectors;
  public volatile int[] aggOffsetInBuffer;
  public volatile int aggsTotalSize;
  NonBlockingPool<ByteBuffer> bufferPool;

  /* basic constractor */
  OffheapAggsManager(
      final IncrementalIndexSchema incrementalIndexSchema,
      final boolean deserializeComplexMetrics,
      final boolean reportParseExceptions,
      final boolean concurrentEventAdd,
      Supplier<InputRow> rowSupplier,
      Map<String, ColumnCapabilitiesImpl> columnCapabilities,
      NonBlockingPool<ByteBuffer> bufferPool,
      IncrementalIndex incrementalIndex
  )
  {
    super(incrementalIndexSchema, deserializeComplexMetrics, reportParseExceptions,
            concurrentEventAdd, rowSupplier, columnCapabilities, incrementalIndex);
    this.bufferPool = bufferPool;
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

  public void clearSelectors()
  {
    if (selectors != null) {
      selectors.clear();
    }
  }
}
