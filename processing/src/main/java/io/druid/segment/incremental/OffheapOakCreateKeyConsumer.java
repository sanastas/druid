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

import java.util.List;
import java.util.function.Consumer;
import java.nio.ByteBuffer;

import io.druid.java.util.common.logger.Logger;
import io.druid.segment.column.ColumnCapabilitiesImpl;
import io.druid.segment.column.ValueType;
import io.druid.segment.incremental.IncrementalIndex.TimeAndDims;
import io.druid.segment.incremental.IncrementalIndex.DimensionDesc;

import oak.OakMap.KeyInfo;

public class OffheapOakCreateKeyConsumer implements Consumer<KeyInfo>
{
  private static final Logger log = new Logger(OffheapOakCreateKeyConsumer.class);
  private List<DimensionDesc> dimensionDescsList;

  public OffheapOakCreateKeyConsumer(List<DimensionDesc> dimensionDescsList)
  {
    this.dimensionDescsList = dimensionDescsList;
  }

  @Override
  public void accept(KeyInfo keyInfo)
  {
    TimeAndDims timeAndDims = (TimeAndDims) keyInfo.key;
    long timestamp = timeAndDims.getTimestamp();
    Object[] dims = timeAndDims.getDims();
    int dimsLength = (dims == null ? 0 : dims.length);

    ByteBuffer buff = keyInfo.buffer;
    int ki = keyInfo.index;

    // calculating buffer indexes for writing the key data
    //log.info("OffheapOakCreateKeyConsumer: buff.position() = " + buff.position() + ", ki = " + ki);
    int buffIndex = buff.position() + ki;  // the first byte for writing the key
    int timeStampIndex = buffIndex;                               // the timestamp index
    int dimsLengthIndex = timeStampIndex + Long.BYTES;            // the dims array length index
    int dimsIndex = dimsLengthIndex + Integer.BYTES;              // the dims array index
    int dimCapacity = OffheapOakIncrementalIndex.ALLOC_PER_DIM;   // the number of bytes required per dim
    int noDim = OffheapOakIncrementalIndex.NO_DIM;                // for mentioning that a certain dim is null
    int dimsArraysIndex = dimsIndex + dimCapacity * dimsLength;   // the index for writing the int arrays
                                                                  // of dims with a STRING type
    int dimsArrayOffset = dimsArraysIndex - buffIndex;            // for saving the array position in the buffer
    int valueTypeOffset = 0;                                      // offset from the dimIndex
    int dataOffset = Integer.BYTES;                               // for non-STRING dims
    int arrayIndexOffset = Integer.BYTES;                         // for STRING dims
    int arrayLengthOffset = arrayIndexOffset + Integer.BYTES;     // for STRING dims

    buff.putLong(timeStampIndex, timestamp);
    buff.putInt(dimsLengthIndex, dimsLength);
    for (int i = 0; i < dimsLength; i++) {
      ValueType valueType = getDimValueType(i);
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

  private ValueType getDimValueType(int dimIndex)
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
