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
import io.druid.data.input.InputRow;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.dimension.DimensionSpec;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.ColumnValueSelector;
import io.druid.segment.DimensionSelector;
import io.druid.segment.column.ColumnCapabilities;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 */
public class OnheapIncrementalIndex extends ExternalDataIncrementalIndex<Aggregator>
{
  private static final Logger log = new Logger(OnheapIncrementalIndex.class);

  protected final FactsHolder facts;
  protected final AtomicInteger indexIncrement = new AtomicInteger(0);
  protected final int maxRowCount;
  protected OnheapAggsManager aggsManager;

  private String outOfRowsReason = null;

  OnheapIncrementalIndex(
      IncrementalIndexSchema incrementalIndexSchema,
      boolean deserializeComplexMetrics,
      boolean reportParseExceptions,
      boolean concurrentEventAdd,
      boolean sortFacts,
      int maxRowCount
  )
  {
    super(incrementalIndexSchema, deserializeComplexMetrics, reportParseExceptions, concurrentEventAdd);
    this.aggsManager = new OnheapAggsManager(incrementalIndexSchema, deserializeComplexMetrics,
            reportParseExceptions, concurrentEventAdd, rowSupplier, columnCapabilities, this);
    this.maxRowCount = maxRowCount;

    this.facts = incrementalIndexSchema.isRollup() ? new RollupFactsHolder(sortFacts, dimsComparator(), getDimensions())
                                                   : new PlainFactsHolder(sortFacts);
  }

  @Override
  protected FactsHolder getFacts()
  {
    return facts;
  }

  @Override
  public Aggregator[] getAggs()
  {
    return aggsManager.getAggs();
  }

  @Override
  public AggregatorFactory[] getMetricAggs()
  {
    return aggsManager.getMetricAggs();
  }

  @Override
  protected Integer addToFacts(
      boolean reportParseExceptions,
      InputRow row,
      AtomicInteger numEntries,
      TimeAndDims key,
      ThreadLocal<InputRow> rowContainer,
      Supplier<InputRow> rowSupplier,
      boolean skipMaxRowsInMemoryCheck
  ) throws IndexSizeExceededException
  {
    final int priorIndex = facts.getPriorIndex(key);

    Aggregator[] aggs;

    if (TimeAndDims.EMPTY_ROW_INDEX != priorIndex) {
      aggs = aggsManager.concurrentGet(priorIndex);
      aggsManager.doAggregate(aggsManager.metrics, aggs, rowContainer, row, reportParseExceptions);
    } else {
      aggs = new Aggregator[aggsManager.metrics.length];
      aggsManager.factorizeAggs(aggsManager.metrics, aggs, rowContainer, row);
      aggsManager.doAggregate(aggsManager.metrics, aggs, rowContainer, row, reportParseExceptions);

      final int rowIndex = indexIncrement.getAndIncrement();
      aggsManager.concurrentSet(rowIndex, aggs);

      // Last ditch sanity checks
      if (numEntries.get() >= maxRowCount
          && facts.getPriorIndex(key) == TimeAndDims.EMPTY_ROW_INDEX
          && !skipMaxRowsInMemoryCheck) {
        throw new IndexSizeExceededException("Maximum number of rows [%d] reached", maxRowCount);
      }
      final int prev = facts.putIfAbsent(key, rowIndex);
      if (TimeAndDims.EMPTY_ROW_INDEX == prev) {
        numEntries.incrementAndGet();
      } else {
        // We lost a race
        aggs = aggsManager.concurrentGet(prev);
        aggsManager.doAggregate(aggsManager.metrics, aggs, rowContainer, row, reportParseExceptions);
        // Free up the misfire
        aggsManager.concurrentRemove(rowIndex);
        // This is expected to occur ~80% of the time in the worst scenarios
      }
    }

    return numEntries.get();
  }

  @Override
  public int getLastRowIndex()
  {
    return indexIncrement.get() - 1;
  }

  @Override
  public boolean canAppendRow()
  {
    final boolean canAdd = size() < maxRowCount;
    if (!canAdd) {
      outOfRowsReason = StringUtils.format("Maximum number of rows [%d] reached", maxRowCount);
    }
    return canAdd;
  }

  @Override
  public String getOutOfRowsReason()
  {
    return outOfRowsReason;
  }

  @Override
  protected Aggregator[] getAggsForRow(TimeAndDims timeAndDims)
  {
    int rowIndex = timeAndDims.getRowIndex();
    return aggsManager.concurrentGet(rowIndex);
  }

  @Override
  protected Object getAggVal(TimeAndDims timeAndDims, int aggIndex)
  {
    int rowIndex = timeAndDims.getRowIndex();
    return aggsManager.concurrentGet(rowIndex)[aggIndex].get();
  }

  @Override
  public float getMetricFloatValue(TimeAndDims timeAndDims, int aggIndex)
  {
    int rowIndex = timeAndDims.getRowIndex();
    return aggsManager.concurrentGet(rowIndex)[aggIndex].getFloat();
  }

  @Override
  public long getMetricLongValue(TimeAndDims timeAndDims, int aggIndex)
  {
    int rowIndex = timeAndDims.getRowIndex();
    return aggsManager.concurrentGet(rowIndex)[aggIndex].getLong();
  }

  @Override
  public Object getMetricObjectValue(TimeAndDims timeAndDims, int aggIndex)
  {
    int rowIndex = timeAndDims.getRowIndex();
    return aggsManager.concurrentGet(rowIndex)[aggIndex].get();
  }

  @Override
  public double getMetricDoubleValue(TimeAndDims timeAndDims, int aggIndex)
  {
    int rowIndex = timeAndDims.getRowIndex();
    return aggsManager.concurrentGet(rowIndex)[aggIndex].getDouble();
  }

  @Override
  public boolean isNull(TimeAndDims timeAndDims, int aggIndex)
  {
    int rowIndex = timeAndDims.getRowIndex();
    return aggsManager.concurrentGet(rowIndex)[aggIndex].isNull();
  }

  /**
   * Clear out maps to allow GC
   * NOTE: This is NOT thread-safe with add... so make sure all the adding is DONE before closing
   */
  @Override
  public void close()
  {
    super.close();
    aggsManager.closeAggregators();
    aggsManager.clearAggregators();
    facts.clear();
    aggsManager.clearSelectors();
  }

  // Caches references to selector objects for each column instead of creating a new object each time in order to save heap space.
  // In general the selectorFactory need not to thread-safe.
  // If required, set concurrentEventAdd to true to use concurrent hash map instead of vanilla hash map for thread-safe
  // operations.
  static class ObjectCachingColumnSelectorFactory implements ColumnSelectorFactory
  {
    private final Map<String, ColumnValueSelector<?>> columnSelectorMap;
    private final ColumnSelectorFactory delegate;

    public ObjectCachingColumnSelectorFactory(ColumnSelectorFactory delegate, boolean concurrentEventAdd)
    {
      this.delegate = delegate;

      if (concurrentEventAdd) {
        columnSelectorMap = new ConcurrentHashMap<>();
      } else {
        columnSelectorMap = new HashMap<>();
      }
    }

    @Override
    public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
    {
      return delegate.makeDimensionSelector(dimensionSpec);
    }

    @Override
    public ColumnValueSelector<?> makeColumnValueSelector(String columnName)
    {
      final ColumnValueSelector existing = columnSelectorMap.get(columnName);
      if (existing != null) {
        return existing;
      }
      return columnSelectorMap.computeIfAbsent(columnName, delegate::makeColumnValueSelector);
    }

    @Nullable
    @Override
    public ColumnCapabilities getColumnCapabilities(String columnName)
    {
      return delegate.getColumnCapabilities(columnName);
    }
  }

  @Nullable
  @Override
  public String getMetricType(String metric)
  {
    return aggsManager.getMetricType(metric);
  }

  @Override
  public ColumnValueSelector<?> makeMetricColumnValueSelector(String metric, TimeAndDimsHolder currEntry)
  {
    return aggsManager.makeMetricColumnValueSelector(metric, currEntry);
  }

  @Override
  public List<String> getMetricNames()
  {
    return aggsManager.getMetricNames();
  }

  @Override
  protected String getMetricName(int metricIndex)
  {
    return aggsManager.metrics[metricIndex].getName();
  }

}
