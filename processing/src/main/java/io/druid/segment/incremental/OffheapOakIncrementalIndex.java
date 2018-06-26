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

import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
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
import io.druid.segment.column.ColumnCapabilitiesImpl;
import io.druid.segment.column.ValueType;
import io.druid.segment.ColumnValueSelector;
import io.druid.segment.DimensionHandler;
import io.druid.segment.DimensionIndexer;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.List;
import java.util.Comparator;
import java.util.function.Function;
import java.util.concurrent.atomic.AtomicInteger;
import oak.OakMapOffHeapImpl;
import oak.OakMap;
import oak.CloseableIterator;

import javax.annotation.Nullable;


/**
 */
public class OffheapOakIncrementalIndex extends
        io.druid.segment.incremental.InternalDataIncrementalIndex<BufferAggregator>
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

  OakMapOffHeapImpl oak;
  private final int maxRowCount;
  private String outOfRowsReason = null;
  private OffheapAggsManager aggsManager;

  OffheapOakIncrementalIndex(
          io.druid.segment.incremental.IncrementalIndexSchema incrementalIndexSchema,
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

    IncrementalIndexRow minIncrementalIndexRow = getMinIncrementalIndexRow();
    oak = new OakMapOffHeapImpl(new IncrementalIndexRowByteBuffersComp(dimensionDescsList),
            minIncrementalIndexRow,
            new OffheapOakCreateKeyConsumer(dimensionDescsList),
            new OffheapOakKeyCapacityCalculator(dimensionDescsList),
            chunkMaxItems,
            chunkBytesPerItem);
    this.maxRowCount = maxRowCount;

    this.aggsManager = new OffheapAggsManager(incrementalIndexSchema, deserializeComplexMetrics,
            reportParseExceptions, concurrentEventAdd, rowSupplier,
            columnCapabilities, null, this);
  }

  @Override
  public Iterable<Row> iterableWithPostAggregations(List<PostAggregator> postAggs, boolean descending)
  {
    return new Iterable<Row>()
    {
      @Override
      public Iterator<Row> iterator()
      {
        OakMap oakMap = descending ? oak.descendingMap() : oak;
        Function<Map.Entry<ByteBuffer, ByteBuffer>, Row> transformer = new EntryTransformer(postAggs);
        return oakMap.entriesTransformIterator(transformer);
      }
    };
  }

  // for oak's transform iterator
  private class EntryTransformer implements Function<Map.Entry<ByteBuffer, ByteBuffer>, Row>
  {
    List<PostAggregator> postAggs;
    final List<DimensionDesc> dimensions;

    public EntryTransformer(List<PostAggregator> postAggs)
    {
      this.postAggs = postAggs;
      this.dimensions = getDimensions();
    }

    @Nullable
    @Override
    public Row apply(@Nullable Map.Entry<ByteBuffer, ByteBuffer> entry)
    {
      IncrementalIndexRow key = incrementalIndexRowDeserialization(entry.getKey());
      ByteBuffer value = entry.getValue();
      Object[] dims = key.getDims();

      Map<String, Object> theVals = Maps.newLinkedHashMap();
      for (int i = 0; i < dims.length; ++i) {
        Object dim = dims[i];
        DimensionDesc dimensionDesc = dimensions.get(i);
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

      BufferAggregator[] aggs = getAggs();
      for (int i = 0; i < aggs.length; ++i) {
        theVals.put(aggsManager.metrics[i].getName(), aggs[i].get(value, aggsManager.aggOffsetInBuffer[i]));
      }

      if (postAggs != null) {
        for (PostAggregator postAgg : postAggs) {
          theVals.put(postAgg.getName(), postAgg.compute(theVals));
        }
      }

      return new MapBasedRow(key.getTimestamp(), theVals);
    }
  };

  @Override
  public void close()
  {
    oak.close();
  }

  @Override
  protected long getMinTimeMillis()
  {
    return oak.getMinKey(buff -> buff.getLong(buff.position() + TIME_STAMP_INDEX));
  }

  @Override
  protected long getMaxTimeMillis()
  {
    return oak.getMaxKey(buff -> buff.getLong(buff.position() + TIME_STAMP_INDEX));
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
    CloseableIterator<ByteBuffer> keysIterator = subMap.keysIterator();
    return new Iterable<IncrementalIndexRow>() {
      @Override
      public Iterator<IncrementalIndexRow> iterator()
      {
        return Iterators.transform(
          keysIterator,
          byteBuffer -> incrementalIndexRowDeserialization(byteBuffer)
        );
      }
    };
  }

  @Override
  public Iterable<IncrementalIndexRow> keySet()
  {
    CloseableIterator<ByteBuffer> keysIterator = oak.keysIterator();

    return new Iterable<IncrementalIndexRow>() {
      @Override
      public Iterator<IncrementalIndexRow> iterator()
      {
        return Iterators.transform(
            keysIterator,
            byteBuffer -> incrementalIndexRowDeserialization(byteBuffer)
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
    //log.info("addToOak. current number of entries = " + numEntries.get());
    OffheapOakCreateKeyConsumer keyCreator = new OffheapOakCreateKeyConsumer(dimensionDescsList);
    OffheapOakKeyCapacityCalculator keyCapacityCalculator = new OffheapOakKeyCapacityCalculator(dimensionDescsList);
    OffheapOakCreateValueConsumer valueCreator = new OffheapOakCreateValueConsumer(aggsManager, reportParseExceptions,
            row, rowContainer);
    OffheapOakComputeConsumer func = new OffheapOakComputeConsumer(aggsManager, reportParseExceptions,
            row, rowContainer);
    if (numEntries.get() < maxRowCount || skipMaxRowsInMemoryCheck) {
      oak.putIfAbsentComputeIfPresent(incrementalIndexRow, keyCreator, keyCapacityCalculator, valueCreator,
              aggsManager.aggsTotalSize, func);

      int currSize = oak.entries();
      int prev = numEntries.get();
      while (currSize > prev) {
        if (numEntries.compareAndSet(prev, currSize)) {
          break;
        }
        prev = numEntries.get();
      }

    } else {
      if (!oak.computeIfPresent(incrementalIndexRow, func)) { // the key wasn't in oak
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

  void incrementalIndexRowSerialization(IncrementalIndexRow incrementalIndexRow, ByteBuffer buff)
  {
    int allocSize = incrementalIndexRowAllocSize(incrementalIndexRow);
    if (buff == null || buff.remaining() < allocSize) {
      return;
    }

    buff.putLong(incrementalIndexRow.getTimestamp());
    Object[] dims = incrementalIndexRow.getDims();
    int dimsLength = (dims == null ? 0 : dims.length);
    buff.putInt(dimsLength);

    int currDimsIndex = DIMS_INDEX;
    int currArrayIndex = DIMS_INDEX + ALLOC_PER_DIM * dimsLength;
    for (int dimIndex = 0; dimIndex < dimsLength; dimIndex++) {
      ValueType valueType = getDimValueType(dimIndex);
      if (valueType == null || dims[dimIndex] == null) {
        buff.putInt(currDimsIndex, NO_DIM);
        currDimsIndex += ALLOC_PER_DIM;
        continue;
      }
      switch (valueType) {
        case LONG:
          buff.putInt(currDimsIndex, valueType.ordinal());
          currDimsIndex += Integer.BYTES;
          buff.putLong(currDimsIndex, (Long) dims[dimIndex]);
          currDimsIndex += Long.BYTES;
          break;
        case FLOAT:
          buff.putInt(currDimsIndex, valueType.ordinal());
          currDimsIndex += Integer.BYTES;
          buff.putFloat(currDimsIndex, (Float) dims[dimIndex]);
          currDimsIndex += Long.BYTES;
          break;
        case DOUBLE:
          buff.putInt(currDimsIndex, valueType.ordinal());
          currDimsIndex += Integer.BYTES;
          buff.putDouble(currDimsIndex, (Double) dims[dimIndex]);
          currDimsIndex += Long.BYTES;
          break;
        case STRING:
          buff.putInt(currDimsIndex, valueType.ordinal()); // writing the value type
          currDimsIndex += Integer.BYTES;
          buff.putInt(currDimsIndex, currArrayIndex); // writing the array position
          currDimsIndex += Integer.BYTES;
          if (dims[dimIndex] == null) {
            buff.putInt(currDimsIndex, 0);
            currDimsIndex += Integer.BYTES;
            break;
          }
          int[] array = (int[]) dims[dimIndex];
          buff.putInt(currDimsIndex, array.length); // writing the array length
          currDimsIndex += Integer.BYTES;
          for (int j = 0; j < array.length; j++) {
            buff.putInt(currArrayIndex, array[j]);
            currArrayIndex += Integer.BYTES;
          }
          break;
        default:
          buff.putInt(currDimsIndex, NO_DIM);
          currDimsIndex += ALLOC_PER_DIM;
      }
    }
    buff.position(0);
  }

  public int incrementalIndexRowAllocSize(IncrementalIndexRow incrementalIndexRow)
  {
    if (incrementalIndexRow == null) {
      return 0;
    }

    Object[] dims = incrementalIndexRow.getDims();
    if (dims == null) {
      return Long.BYTES + Integer.BYTES;
    }

    // When the dimensionDesc's capabilities are of type ValueType.STRING,
    // the object in timeAndDims.dims is of type int[].
    // In this case, we need to know the array size before allocating the ByteBuffer.
    int sumOfArrayLengths = 0;
    for (int i = 0; i < dims.length; i++) {
      Object dim = dims[i];
      if (dim == null) {
        continue;
      }
      if (getDimValueType(i) == ValueType.STRING) {
        sumOfArrayLengths += ((int[]) dim).length;
      }
    }

    // The ByteBuffer will contain:
    // 1. the timeStamp
    // 2. dims.length
    // 3. the serialization of each dim
    // 4. the array (for dims with capabilities of a String ValueType)
    return Long.BYTES + Integer.BYTES + ALLOC_PER_DIM * dims.length + Integer.BYTES * sumOfArrayLengths;
  }

  IncrementalIndexRow incrementalIndexRowDeserialization(ByteBuffer buff)
  {
    long timeStamp = getTimestamp(buff);
    int dimsLength = getDimsLength(buff);
    Object[] dims = new Object[dimsLength];
    for (int dimIndex = 0; dimIndex < dimsLength; dimIndex++) {
      Object dim = getDimValue(buff, dimIndex);
      dims[dimIndex] = dim;
    }
    return new IncrementalIndexRow(timeStamp, dims, dimensionDescsList, IncrementalIndexRow.EMPTY_ROW_INDEX);
  }

  private ValueType getDimValueType(int dimIndex)
  {
    DimensionDesc dimensionDesc = getDimensions().get(dimIndex);
    if (dimensionDesc == null) {
      return null;
    }
    ColumnCapabilitiesImpl capabilities = dimensionDesc.getCapabilities();
    if (capabilities == null) {
      return null;
    }
    return capabilities.getType();
  }

  static long getTimestamp(ByteBuffer buff)
  {
    return buff.getLong(buff.position() + TIME_STAMP_INDEX);
  }

  static int getDimsLength(ByteBuffer buff)
  {
    return buff.getInt(buff.position() + DIMS_LENGTH_INDEX);
  }

  static Object getDimValue(ByteBuffer buff, int dimIndex)
  {
    Object dimObject = null;
    int dimsLength = getDimsLength(buff);
    if (dimIndex >= dimsLength) {
      return null;
    }
    int dimType = buff.getInt(getDimIndexInBuffer(buff, dimIndex));
    if (dimType == NO_DIM) {
      return null;
    } else if (dimType == ValueType.DOUBLE.ordinal()) {
      dimObject = buff.getDouble(getDimIndexInBuffer(buff, dimIndex) + Integer.BYTES);
    } else if (dimType == ValueType.FLOAT.ordinal()) {
      dimObject = buff.getFloat(getDimIndexInBuffer(buff, dimIndex) + Integer.BYTES);
    } else if (dimType == ValueType.LONG.ordinal()) {
      dimObject = buff.getLong(getDimIndexInBuffer(buff, dimIndex) + Integer.BYTES);
    } else if (dimType == ValueType.STRING.ordinal()) {
      int arrayIndex = buff.position() + buff.getInt(getDimIndexInBuffer(buff, dimIndex) + Integer.BYTES);
      int arraySize = buff.getInt(getDimIndexInBuffer(buff, dimIndex) + 2 * Integer.BYTES);
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
      if (buff.getInt(getDimIndexInBuffer(buff, index)) != NO_DIM) {
        return false;
      }
    }
    return true;
  }

  static int getDimIndexInBuffer(ByteBuffer buff, int dimIndex)
  {
    int dimsLength = getDimsLength(buff);
    if (dimIndex >= dimsLength) {
      return NO_DIM;
    }
    return buff.position() + DIMS_INDEX + dimIndex * ALLOC_PER_DIM;
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

}
