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

import com.oath.oak.OakRBuffer;
import io.druid.segment.column.ValueType;
import io.druid.segment.incremental.IncrementalIndex.DimensionDesc;
import java.util.List;

public class OakIncrementalIndexRow extends IncrementalIndexRow
{

  OakRBuffer oakRBuffer;

  public OakIncrementalIndexRow(OakRBuffer oakRBuffer, List<DimensionDesc> dimensionDescsList)
  {
    this.oakRBuffer = oakRBuffer;
    this.dimensionDescsList = dimensionDescsList;
  }

  @Override
  public long getTimestamp()
  {
    return oakRBuffer.getLong(oakRBuffer.position() + OakIncrementalIndex.TIME_STAMP_INDEX);
  }

  @Override
  public int getDimsLength()
  {
    return oakRBuffer.getInt(oakRBuffer.position() + OakIncrementalIndex.DIMS_LENGTH_INDEX);
  }

  @Override
  public Object getDim(int dimIndex)
  {
    return getDim(getDimsLength(), dimIndex);
  }

  public Object getDim(int dimsLength, int dimIndex)
  {
    if (dimIndex >= dimsLength) {
      return null;
    }
    return getDimValue(dimsLength, dimIndex);
  }

  @Override
  public boolean isNull(int dimIndex)
  {
    int dimsLength = getDimsLength();
    return dimIndex >= dimsLength ||
            getDimType(dimsLength, dimIndex) == OakIncrementalIndex.NO_DIM;
  }

  @Override
  public Object[] getDims()
  {
    int dimsLength = getDimsLength();
    if (dimsLength == 0) {
      return null;
    }
    Object[] dims = new Object[dimsLength];
    for (int dimIndex = 0; dimIndex < dimsLength; dimIndex++) {
      Object dim = getDimValue(dimsLength, dimIndex);
      dims[dimIndex] = dim;
    }
    return dims;
  }

  @Override
  public int getRowIndex()
  {
    return oakRBuffer.getInt(oakRBuffer.position() + OakIncrementalIndex.ROW_INDEX_INDEX);
  }

  @Override
  void setRowIndex(int rowIndex)
  {
    throw new UnsupportedOperationException();
  }

  /**
   * bytesInMemory estimates the size of the serialized IncrementalIndexRow key.
   * Each serialized IncrementalRoeIndex contains:
   * 1. a timeStamp
   * 2. the dims array length
   * 3. the rowIndex
   * 4. the serialization of each dim
   * 5. the array (for dims with capabilities of a String ValueType)
   *
   * @return long estimated bytesInMemory
   */
  @Override
  public long estimateBytesInMemory()
  {

    long sizeInBytes = Long.BYTES + 2 * Integer.BYTES;
    int dimsLength = getDimsLength();
    for (int dimIndex = 0; dimIndex < dimsLength; dimIndex++) {
      sizeInBytes += OakIncrementalIndex.ALLOC_PER_DIM;
      int dimType = getDimType(dimsLength, dimIndex);
      if (dimType == ValueType.STRING.ordinal()) {
        int dimIndexInBuffer = getDimIndexInBuffer(dimsLength, dimIndex);
        int arraySize = oakRBuffer.getInt(dimIndexInBuffer + OakIncrementalIndex.ARRAY_LENGTH_OFFSET);
        sizeInBytes += (arraySize * Integer.BYTES);
      }
    }
    return sizeInBytes;
  }




  /* ---------------- OakRBuffer utils -------------- */

  private int getDimIndexInBuffer(int dimsLength, int dimIndex)
  {
    if (dimIndex >= dimsLength) {
      return OakIncrementalIndex.NO_DIM;
    }
    return oakRBuffer.position() + OakIncrementalIndex.DIMS_INDEX + dimIndex * OakIncrementalIndex.ALLOC_PER_DIM;
  }

  private Object getDimValue(int dimsLength, int dimIndex)
  {
    Object dimObject = null;
    if (dimIndex >= dimsLength) {
      return null;
    }
    int dimIndexInBuffer = getDimIndexInBuffer(dimsLength, dimIndex);
    int dimType = oakRBuffer.getInt(dimIndexInBuffer);
    if (dimType == OakIncrementalIndex.NO_DIM) {
      return null;
    } else if (dimType == ValueType.DOUBLE.ordinal()) {
      dimObject = oakRBuffer.getDouble(dimIndexInBuffer + OakIncrementalIndex.DATA_OFFSET);
    } else if (dimType == ValueType.FLOAT.ordinal()) {
      dimObject = oakRBuffer.getFloat(dimIndexInBuffer + OakIncrementalIndex.DATA_OFFSET);
    } else if (dimType == ValueType.LONG.ordinal()) {
      dimObject = oakRBuffer.getLong(dimIndexInBuffer + OakIncrementalIndex.DATA_OFFSET);
    } else if (dimType == ValueType.STRING.ordinal()) {
      int arrayIndexOffset = oakRBuffer.getInt(dimIndexInBuffer + OakIncrementalIndex.ARRAY_INDEX_OFFSET);
      int arrayIndex = oakRBuffer.position() + arrayIndexOffset;
      int arraySize = oakRBuffer.getInt(dimIndexInBuffer + OakIncrementalIndex.ARRAY_LENGTH_OFFSET);
      int[] array = new int[arraySize];
      for (int i = 0; i < arraySize; i++) {
        array[i] = oakRBuffer.getInt(arrayIndex);
        arrayIndex += Integer.BYTES;
      }
      dimObject = array;
    }

    return dimObject;
  }

  private int getDimType(int dimsLength, int dimIndex)
  {
    if (dimIndex >= dimsLength) {
      return OakIncrementalIndex.NO_DIM;
    }
    int dimIndexInBuffer = getDimIndexInBuffer(dimsLength, dimIndex);
    return oakRBuffer.getInt(dimIndexInBuffer);
  }
}
