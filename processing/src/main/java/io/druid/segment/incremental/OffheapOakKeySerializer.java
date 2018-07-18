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

import java.util.List;
import java.nio.ByteBuffer;

import io.druid.segment.column.ValueType;
import io.druid.segment.incremental.IncrementalIndex.DimensionDesc;
import oak.KeySerializer;

public class OffheapOakKeySerializer implements KeySerializer<IncrementalIndexRow>
{
  private List<DimensionDesc> dimensionDescsList;

  public OffheapOakKeySerializer(List<DimensionDesc> dimensionDescsList)
  {
    this.dimensionDescsList = dimensionDescsList;
  }

  @Override
  public void serialize(IncrementalIndexRow incrementalIndexRow, ByteBuffer buff)
  {
    long timestamp = incrementalIndexRow.getTimestamp();
    Object[] dims = incrementalIndexRow.getDims();
    int dimsLength = (dims == null ? 0 : dims.length);

    // calculating buffer indexes for writing the key data
    int buffIndex = buff.position();  // the first byte for writing the key
    int timeStampIndex = buffIndex + OffheapOakIncrementalIndex.TIME_STAMP_INDEX;   // the timestamp index
    int dimsLengthIndex = buffIndex + OffheapOakIncrementalIndex.DIMS_LENGTH_INDEX; // the dims array length index
    int dimsIndex = buffIndex + OffheapOakIncrementalIndex.DIMS_INDEX;              // the dims array index
    int dimCapacity = OffheapOakIncrementalIndex.ALLOC_PER_DIM;                     // the number of bytes required
                                                                                    // per dim
    int noDim = OffheapOakIncrementalIndex.NO_DIM;                                  // for mentioning that
                                                                                    // a certain dim is null
    int dimsArraysIndex = dimsIndex + dimCapacity * dimsLength;                     // the index for
                                                                                    // writing the int arrays
    // of dims with a STRING type
    int dimsArrayOffset = dimsArraysIndex - buffIndex;                              // for saving the array position
                                                                                    // in the buffer
    int valueTypeOffset = OffheapOakIncrementalIndex.VALUE_TYPE_OFFSET;             // offset from the dimIndex
    int dataOffset = OffheapOakIncrementalIndex.DATA_OFFSET;                        // for non-STRING dims
    int arrayIndexOffset = OffheapOakIncrementalIndex.ARRAY_INDEX_OFFSET;           // for STRING dims
    int arrayLengthOffset = OffheapOakIncrementalIndex.ARRAY_LENGTH_OFFSET;         // for STRING dims

    buff.putLong(timeStampIndex, timestamp);
    buff.putInt(dimsLengthIndex, dimsLength);
    for (int i = 0; i < dimsLength; i++) {
      ValueType valueType = OffheapOakIncrementalIndex.getDimValueType(i, dimensionDescsList);
      if (valueType == null || dims[i] == null) {
        buff.putInt(dimsIndex, noDim);
      } else {
        buff.putInt(dimsIndex + valueTypeOffset, valueType.ordinal());
        switch (valueType) {
          case FLOAT:
            buff.putFloat(dimsIndex + dataOffset, (Float) dims[i]);
            break;
          case DOUBLE:
            buff.putDouble(dimsIndex + dataOffset, (Double) dims[i]);
            break;
          case LONG:
            buff.putLong(dimsIndex + dataOffset, (Long) dims[i]);
            break;
          case STRING:
            int[] arr = (int[]) dims[i];
            buff.putInt(dimsIndex + arrayIndexOffset, dimsArrayOffset);
            buff.putInt(dimsIndex + arrayLengthOffset, arr.length);
            for (int arrIndex = 0; arrIndex < arr.length; arrIndex++) {
              buff.putInt(dimsArraysIndex + arrIndex * Integer.BYTES, arr[arrIndex]);
            }
            dimsArraysIndex += (arr.length * Integer.BYTES);
            dimsArrayOffset += (arr.length * Integer.BYTES);
            break;
          default:
            buff.putInt(dimsIndex, noDim);
        }
      }

      dimsIndex += dimCapacity;
    }

  }

  @Override
  public IncrementalIndexRow deserialize(ByteBuffer serializedKey)
  {
    long timeStamp = OffheapOakIncrementalIndex.getTimestamp(serializedKey);
    int dimsLength = OffheapOakIncrementalIndex.getDimsLength(serializedKey);
    Object[] dims = new Object[dimsLength];
    for (int dimIndex = 0; dimIndex < dimsLength; dimIndex++) {
      Object dim = OffheapOakIncrementalIndex.getDimValue(serializedKey, dimIndex);
      dims[dimIndex] = dim;
    }
    return new IncrementalIndexRow(timeStamp, dims, dimensionDescsList, IncrementalIndexRow.EMPTY_ROW_INDEX);
  }
}
