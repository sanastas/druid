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

import com.google.common.collect.Iterators;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.logger.Logger;
import io.druid.java.util.common.parsers.ParseException;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.aggregation.PostAggregator;
import io.druid.segment.ColumnValueSelector;
import io.druid.segment.DimensionIndexer;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.List;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicInteger;

import io.druid.segment.column.ColumnCapabilitiesImpl;
import io.druid.segment.column.ValueType;
import oak.OakMapBuilder;
import oak.OakMapOffHeapImpl;
import oak.OakMap;
import oak.CloseableIterator;

import javax.annotation.Nullable;


/**
 */
public class OffheapOakIncrementalIndex extends InternalDataIncrementalIndex<BufferAggregator>
{

  private static final Logger log = new Logger(OffheapOakIncrementalIndex.class);

  // When serializing an object from IncrementalIndexRow.dims, we use:
  // 1. 4 bytes for representing its type (Double, Float, Long or String)
  // 2. 8 bytes for saving its value or the array position and length (in the case of String)
  static final Integer ALLOC_PER_DIM = 12;
  static final Integer NO_DIM = -1;
  static final Integer TIME_STAMP_INDEX = 0;
  static final Integer DIMS_LENGTH_INDEX = TIME_STAMP_INDEX + Long.BYTES;
  static final Integer DIMS_INDEX = DIMS_LENGTH_INDEX + Integer.BYTES;
  // Serialization and deserialization offsets
  static final Integer VALUE_TYPE_OFFSET = 0;
  static final Integer DATA_OFFSET = VALUE_TYPE_OFFSET + Integer.BYTES;
  static final Integer ARRAY_INDEX_OFFSET = VALUE_TYPE_OFFSET + Integer.BYTES;
  static final Integer ARRAY_LENGTH_OFFSET = ARRAY_INDEX_OFFSET + Integer.BYTES;

  OakMapOffHeapImpl<IncrementalIndexRow, InputRow> oak;
  private final int maxRowCount;
  private String outOfRowsReason = null;
  private OffheapAggsManager aggsManager;

  OffheapOakIncrementalIndex(
          IncrementalIndexSchema incrementalIndexSchema,
          boolean deserializeComplexMetrics,
          boolean reportParseExceptions,
          boolean concurrentEventAdd,
          int maxRowCount,
          int chunkMaxItems,
          int chunkBytesPerItem
  )
  {
    super(incrementalIndexSchema, deserializeComplexMetrics, reportParseExceptions,
            concurrentEventAdd);
    this.maxRowCount = maxRowCount;

    this.aggsManager = new OffheapAggsManager(incrementalIndexSchema, deserializeComplexMetrics,
            reportParseExceptions, concurrentEventAdd, rowSupplier,
            columnCapabilities, null, this);

    IncrementalIndexRow minIncrementalIndexRow = getMinIncrementalIndexRow();

    OakMapBuilder builder = new OakMapBuilder()
            .setChunkMaxItems(chunkMaxItems)
            .setChunkBytesPerItem(chunkBytesPerItem)
            .setKeySerializer(new OffheapOakKeySerializer(dimensionDescsList))
            .setKeySizeCalculator(new OffheapOakKeySizeCalculator(dimensionDescsList))
            .setValueSerializer(new OffheapOakValueSerializer(dimensionDescsList, aggsManager, reportParseExceptions, in))
            .setValueSizeCalculator(new OffheapOakValueSizeCalculator(aggsManager.aggsTotalSize))
            .setMinKey(minIncrementalIndexRow)
            .setKeysComparator(new OffheapOakKeysComparator(dimensionDescsList))
            .setSerializationsComparator(new OffheapOakSerializationsComparator(dimensionDescsList))
            .setSerializationAndKeyComparator(new OffheapOakSerializationAndKeyComparator(dimensionDescsList));

    oak = builder.buildOffHeapOakMap();
  }

  @Override
  public Iterable<Row> iterableWithPostAggregations(List<PostAggregator> postAggs, boolean descending)
  {
    OakMap oakMap = descending ? oak.descendingMap() : oak;
    CloseableIterator<Row> valuesIterator = oakMap.valuesIterator();
    return new Iterable<Row>()
    {
      @Override
      public Iterator<Row> iterator()
      {
        return Iterators.transform(
            valuesIterator,
            new com.google.common.base.Function<Row, Row>() {
              @Nullable
              @Override
              public Row apply(@Nullable Row row)
              {
                Map<String, Object> event = ((MapBasedRow) row).getEvent();
                long timestamp = ((MapBasedRow) row).getTimestamp().getMillis();
                if (postAggs != null) {
                  for (PostAggregator postAgg : postAggs) {
                    event.put(postAgg.getName(), postAgg.compute(event));
                  }
                }
                return new MapBasedRow(timestamp, event);
              }
            }
        );
      }
    };
  }

  @Override
  public void close()
  {
    oak.close();
  }

  @Override
  protected long getMinTimeMillis()
  {
    return oak.getMinKey().getTimestamp();
  }

  @Override
  protected long getMaxTimeMillis()
  {
    return oak.getMaxKey().getTimestamp();
  }

  @Override
  public int getLastRowIndex()
  {
    return 0; // Oak doesn't use the row indexes
  }

  @Override
  protected BufferAggregator[] getAggsForRow(IncrementalIndexRow incrementalIndexRow)
  {
    return getAggs();
  }

  @Override
  protected Object getAggVal(IncrementalIndexRow incrementalIndexRow, int aggIndex)
  {
    BufferAggregator agg = getAggs()[aggIndex];
    return oak.getTransformation(incrementalIndexRow, buff -> agg.get(buff,
            buff.position() + aggsManager.aggOffsetInBuffer[aggIndex]));
  }

  @Override
  protected float getMetricFloatValue(IncrementalIndexRow incrementalIndexRow, int aggIndex)
  {
    BufferAggregator agg = getAggs()[aggIndex];
    return oak.getTransformation(incrementalIndexRow, buff -> agg.getFloat(buff,
            buff.position() + aggsManager.aggOffsetInBuffer[aggIndex]));
  }

  @Override
  protected long getMetricLongValue(IncrementalIndexRow incrementalIndexRow, int aggIndex)
  {
    BufferAggregator agg = getAggs()[aggIndex];
    return oak.getTransformation(incrementalIndexRow, buff -> agg.getLong(buff,
            buff.position() + aggsManager.aggOffsetInBuffer[aggIndex]));
  }

  @Override
  protected Object getMetricObjectValue(IncrementalIndexRow incrementalIndexRow, int aggIndex)
  {
    BufferAggregator agg = getAggs()[aggIndex];
    return oak.getTransformation(incrementalIndexRow, buff -> agg.get(buff,
            buff.position() + aggsManager.aggOffsetInBuffer[aggIndex]));
  }

  @Override
  protected double getMetricDoubleValue(IncrementalIndexRow incrementalIndexRow, int aggIndex)
  {
    BufferAggregator agg = getAggs()[aggIndex];
    return oak.getTransformation(incrementalIndexRow, buff -> agg.getDouble(buff,
            buff.position() + aggsManager.aggOffsetInBuffer[aggIndex]));
  }

  @Override
  protected boolean isNull(IncrementalIndexRow incrementalIndexRow, int aggIndex)
  {
    BufferAggregator agg = getAggs()[aggIndex];
    return oak.getTransformation(incrementalIndexRow, buff -> agg.isNull(buff,
            buff.position() + aggsManager.aggOffsetInBuffer[aggIndex]));
  }

  @Override
  public Iterable<IncrementalIndexRow> timeRangeIterable(
          boolean descending, long timeStart, long timeEnd)
  {
    if (timeStart > timeEnd) {
      return null;
    }

    IncrementalIndexRow from = new IncrementalIndexRow(timeStart, null, dimensionDescsList);
    IncrementalIndexRow to = new IncrementalIndexRow(timeEnd + 1, null, dimensionDescsList);
    OakMap subMap = oak.subMap(from, true, to, false);
    if (descending == true) {
      subMap = subMap.descendingMap();
    }
    CloseableIterator<IncrementalIndexRow> keysIterator = subMap.keysIterator();
    return new Iterable<IncrementalIndexRow>() {
      @Override
      public Iterator<IncrementalIndexRow> iterator()
      {
        return Iterators.transform(
          keysIterator,
          key -> key
        );
      }
    };
  }

  @Override
  public Iterable<IncrementalIndexRow> keySet()
  {
    CloseableIterator<IncrementalIndexRow> keysIterator = oak.keysIterator();

    return new Iterable<IncrementalIndexRow>() {
      @Override
      public Iterator<IncrementalIndexRow> iterator()
      {
        return Iterators.transform(
            keysIterator,
            key -> key
        );
      }
    };
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

  private Integer addToOak(
          InputRow row,
          AtomicInteger numEntries,
          IncrementalIndexRow incrementalIndexRow,
          ThreadLocal<InputRow> rowContainer,
          boolean skipMaxRowsInMemoryCheck
  ) throws IndexSizeExceededException
  {
    OffheapOakComputer computer = new OffheapOakComputer(aggsManager, reportParseExceptions,
            row, rowContainer);
    if (numEntries.get() < maxRowCount || skipMaxRowsInMemoryCheck) {
      oak.putIfAbsentComputeIfPresent(incrementalIndexRow, row, computer);

      int currSize = oak.entries();
      int prev = numEntries.get();
      while (currSize > prev) {
        if (numEntries.compareAndSet(prev, currSize)) {
          break;
        }
        prev = numEntries.get();
      }

    } else {
      if (!oak.computeIfPresent(incrementalIndexRow, computer)) { // the key wasn't in oak
        throw new IndexSizeExceededException("Maximum number of rows [%d] reached", maxRowCount);
      }
    }
    return numEntries.get();
  }

  @Override
  public BufferAggregator[] getAggs()
  {
    return aggsManager.getAggs();
  }

  @Override
  public AggregatorFactory[] getMetricAggs()
  {
    return aggsManager.getMetricAggs();
  }

  public static void aggregate(
          AggregatorFactory[] metrics,
          boolean reportParseExceptions,
          InputRow row,
          ThreadLocal<InputRow> rowContainer,
          ByteBuffer aggBuffer,
          int[] aggOffsetInBuffer,
          BufferAggregator[] aggs
  )
  {
    rowContainer.set(row);

    for (int i = 0; i < metrics.length; i++) {
      final BufferAggregator agg = aggs[i];

      synchronized (agg) {
        try {
          agg.aggregate(aggBuffer, aggBuffer.position() + aggOffsetInBuffer[i]);
        }
        catch (ParseException e) {
          // "aggregate" can throw ParseExceptions if a selector expects something but gets something else.
          if (reportParseExceptions) {
            throw new ParseException(e, "Encountered parse error for aggregator[%s]", metrics[i].getName());
          } else {
            log.debug(e, "Encountered parse error, skipping aggregator[%s].", metrics[i].getName());
          }
        }
      }
    }
    rowContainer.set(null);
  }

  @Override
  public IncrementalIndexAddResult add(InputRow row, boolean skipMaxRowsInMemoryCheck) throws IndexSizeExceededException
  {
    IncrementalIndexRowResult incrementalIndexRowResult = toIncrementalIndexRow(row);
    final int rv = addToOak(
            row,
            numEntries,
            incrementalIndexRowResult.getIncrementalIndexRow(),
            in,
            skipMaxRowsInMemoryCheck
    );
    updateMaxIngestedTime(row.getTimestamp());
    return new IncrementalIndexAddResult(rv, 0, null);
  }

  private IncrementalIndexRow getMinIncrementalIndexRow()
  {
    return new IncrementalIndexRow(this.minTimestamp, null, dimensionDescsList);
  }

  public final Comparator<Object> dimsByteBufferComparator()
  {
    return new IncrementalIndexRowByteBuffersComp(dimensionDescsList);
  }

  static final class IncrementalIndexRowByteBuffersComp implements Comparator<Object>
  {
    private List<DimensionDesc> dimensionDescsList;

    public IncrementalIndexRowByteBuffersComp(List<DimensionDesc> dimensionDescsList)
    {
      this.dimensionDescsList = dimensionDescsList;
    }

    @Override
    public int compare(Object lhs, Object rhs)
    {
      if (lhs instanceof ByteBuffer) {
        if (rhs instanceof ByteBuffer) {
          return compareByteBuffers((ByteBuffer) lhs, (ByteBuffer) rhs);
        } else {
          return compareByteBufferIncrementalIndexRow((ByteBuffer) lhs, (IncrementalIndexRow) rhs);
        }
      } else {
        if (rhs instanceof ByteBuffer) {
          return compareIncrementalIndexRowByteBuffer((IncrementalIndexRow) lhs, (ByteBuffer) rhs);
        } else {
          return compareIncrementalIndexRows((IncrementalIndexRow) lhs, (IncrementalIndexRow) rhs);
        }
      }
    }

    private int compareByteBuffers(ByteBuffer lhs, ByteBuffer rhs)
    {
      int retVal = Longs.compare(getTimestamp(lhs), getTimestamp(rhs));
      int numComparisons = Math.min(getDimsLength(lhs), getDimsLength(rhs));

      int dimIndex = 0;
      while (retVal == 0 && dimIndex < numComparisons) {
        int lhsType = lhs.getInt(getDimIndexInBuffer(lhs, dimIndex));
        int rhsType = rhs.getInt(getDimIndexInBuffer(rhs, dimIndex));

        if (lhsType == NO_DIM) {
          if (rhsType == NO_DIM) {
            ++dimIndex;
            continue;
          }
          return -1;
        }

        if (rhsType == NO_DIM) {
          return 1;
        }

        final DimensionIndexer indexer = dimensionDescsList.get(dimIndex).getIndexer();
        Object lhsObject = getDimValue(lhs, dimIndex);
        Object rhsObject = getDimValue(rhs, dimIndex);
        retVal = indexer.compareUnsortedEncodedKeyComponents(lhsObject, rhsObject);
        ++dimIndex;
      }

      if (retVal == 0) {
        int lengthDiff = Ints.compare(getDimsLength(lhs), getDimsLength(rhs));
        if (lengthDiff == 0) {
          return 0;
        }
        ByteBuffer largerDims = lengthDiff > 0 ? lhs : rhs;
        return checkDimsAllNull(largerDims, numComparisons) ? 0 : lengthDiff;
      }
      return retVal;
    }

    private int compareIncrementalIndexRows(IncrementalIndexRow lhs, IncrementalIndexRow rhs)
    {
      int retVal = Longs.compare(lhs.getTimestamp(), rhs.getTimestamp());
      int lhsDimsLength = lhs.getDims() == null ? 0 : lhs.getDims().length;
      int rhsDimsLength = rhs.getDims() == null ? 0 : rhs.getDims().length;
      int numComparisons = Math.min(lhsDimsLength, rhsDimsLength);

      int index = 0;
      while (retVal == 0 && index < numComparisons) {
        final Object lhsIdxs = lhs.getDims()[index];
        final Object rhsIdxs = rhs.getDims()[index];

        if (lhsIdxs == null) {
          if (rhsIdxs == null) {
            ++index;
            continue;
          }
          return -1;
        }

        if (rhsIdxs == null) {
          return 1;
        }

        final DimensionIndexer indexer = dimensionDescsList.get(index).getIndexer();
        retVal = indexer.compareUnsortedEncodedKeyComponents(lhsIdxs, rhsIdxs);
        ++index;
      }

      if (retVal == 0) {
        int lengthDiff = Ints.compare(lhsDimsLength, rhsDimsLength);
        if (lengthDiff == 0) {
          return 0;
        }
        Object[] largerDims = lengthDiff > 0 ? lhs.getDims() : rhs.getDims();
        return allNull(largerDims, numComparisons) ? 0 : lengthDiff;
      }

      return retVal;
    }

    private int compareIncrementalIndexRowByteBuffer(IncrementalIndexRow lhs, ByteBuffer rhs)
    {
      int retVal = Longs.compare(lhs.getTimestamp(), getTimestamp(rhs));
      int lhsDimsLength = lhs.getDims() == null ? 0 : lhs.getDims().length;
      int rhsDimsLength = getDimsLength(rhs);
      int numComparisons = Math.min(lhsDimsLength, rhsDimsLength);

      int index = 0;
      while (retVal == 0 && index < numComparisons) {
        final Object lhsIdxs = lhs.getDims()[index];
        final Object rhsIdxs = getDimValue(rhs, index);

        if (lhsIdxs == null) {
          if (rhsIdxs == null) {
            ++index;
            continue;
          }
          return -1;
        }

        if (rhsIdxs == null) {
          return 1;
        }

        final DimensionIndexer indexer = dimensionDescsList.get(index).getIndexer();
        retVal = indexer.compareUnsortedEncodedKeyComponents(lhsIdxs, rhsIdxs);
        ++index;
      }

      if (retVal == 0) {
        int lengthDiff = Ints.compare(lhsDimsLength, rhsDimsLength);
        if (lengthDiff == 0) {
          return 0;
        }

        if (lengthDiff > 0) {
          return allNull(lhs.getDims(), numComparisons) ? 0 : lengthDiff;
        } else {
          return checkDimsAllNull(rhs, numComparisons) ? 0 : lengthDiff;
        }
      }

      return retVal;
    }

    private int compareByteBufferIncrementalIndexRow(ByteBuffer lhs, IncrementalIndexRow rhs)
    {
      return compare(rhs, lhs) * (-1);
    }
  }

  @Nullable
  @Override
  public String getMetricType(String metric)
  {
    return aggsManager.getMetricType(metric);
  }

  @Override
  public ColumnValueSelector<?> makeMetricColumnValueSelector(String metric, IncrementalIndexRowHolder currEntry)
  {
    return aggsManager.makeMetricColumnValueSelector(metric, currEntry);
  }

  @Override
  public List<String> getMetricNames()
  {
    return aggsManager.getMetricNames();
  }

  // Serialization utils
  static long getTimestamp(ByteBuffer buff)
  {
    return buff.getLong(buff.position() + OffheapOakIncrementalIndex.TIME_STAMP_INDEX);
  }

  static int getDimsLength(ByteBuffer buff)
  {
    return buff.getInt(buff.position() + OffheapOakIncrementalIndex.DIMS_LENGTH_INDEX);
  }

  static int getDimIndexInBuffer(ByteBuffer buff, int dimIndex)
  {
    int dimsLength = getDimsLength(buff);
    if (dimIndex >= dimsLength) {
      return OffheapOakIncrementalIndex.NO_DIM;
    }
    return buff.position() + OffheapOakIncrementalIndex.DIMS_INDEX +
            dimIndex * OffheapOakIncrementalIndex.ALLOC_PER_DIM;
  }

  static Object getDimValue(ByteBuffer buff, int dimIndex)
  {
    Object dimObject = null;
    int dimsLength = getDimsLength(buff);
    if (dimIndex >= dimsLength) {
      return null;
    }
    int dimIndexInBuffer = getDimIndexInBuffer(buff, dimIndex);
    int dimType = buff.getInt(getDimIndexInBuffer(buff, dimIndex));
    if (dimType == OffheapOakIncrementalIndex.NO_DIM) {
      return null;
    } else if (dimType == ValueType.DOUBLE.ordinal()) {
      dimObject = buff.getDouble(dimIndexInBuffer + OffheapOakIncrementalIndex.DATA_OFFSET);
    } else if (dimType == ValueType.FLOAT.ordinal()) {
      dimObject = buff.getFloat(dimIndexInBuffer + OffheapOakIncrementalIndex.DATA_OFFSET);
    } else if (dimType == ValueType.LONG.ordinal()) {
      dimObject = buff.getLong(dimIndexInBuffer + OffheapOakIncrementalIndex.DATA_OFFSET);
    } else if (dimType == ValueType.STRING.ordinal()) {
      int arrayIndexOffset = buff.getInt(dimIndexInBuffer + OffheapOakIncrementalIndex.ARRAY_INDEX_OFFSET);
      int arrayIndex = buff.position() + arrayIndexOffset;
      int arraySize = buff.getInt(dimIndexInBuffer + OffheapOakIncrementalIndex.ARRAY_LENGTH_OFFSET);
      int[] array = new int[arraySize];
      for (int i = 0; i < arraySize; i++) {
        array[i] = buff.getInt(arrayIndex);
        arrayIndex += Integer.BYTES;
      }
      dimObject = array;
    }

    return dimObject;
  }

  static boolean checkDimsAllNull(ByteBuffer buff, int numComparisons)
  {
    int dimsLength = getDimsLength(buff);
    for (int index = 0; index < Math.min(dimsLength, numComparisons); index++) {
      if (buff.getInt(getDimIndexInBuffer(buff, index)) != OffheapOakIncrementalIndex.NO_DIM) {
        return false;
      }
    }
    return true;
  }

  static ValueType getDimValueType(int dimIndex, List<DimensionDesc> dimensionDescsList)
  {
    DimensionDesc dimensionDesc = dimensionDescsList.get(dimIndex);
    if (dimensionDesc == null) {
      return null;
    }
    ColumnCapabilitiesImpl capabilities = dimensionDesc.getCapabilities();
    if (capabilities == null) {
      return null;
    }
    return capabilities.getType();
  }

}
