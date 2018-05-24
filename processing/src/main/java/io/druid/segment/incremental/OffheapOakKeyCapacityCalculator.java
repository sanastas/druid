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
import java.util.function.Function;

import io.druid.segment.column.ColumnCapabilitiesImpl;
import io.druid.segment.column.ValueType;
import io.druid.segment.incremental.IncrementalIndex.TimeAndDims;
import io.druid.segment.incremental.IncrementalIndex.DimensionDesc;

public class OffheapOakKeyCapacityCalculator implements Function<Object, Integer>
{
  private List<DimensionDesc> dimensionDescsList;

  public OffheapOakKeyCapacityCalculator(List<DimensionDesc> dimensionDescsList)
  {
    this.dimensionDescsList = dimensionDescsList;
  }

  @Override
  public Integer apply(Object object)
  {
    if (object == null || !(object instanceof TimeAndDims)) {
      return 0;
    }

    TimeAndDims timeAndDims = (TimeAndDims) object;

    Object[] dims = timeAndDims.getDims();
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
    int dimCapacity = OffheapOakIncrementalIndex.ALLOC_PER_DIM;
    return Long.BYTES + Integer.BYTES + dimCapacity * dims.length + Integer.BYTES * sumOfArrayLengths;

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
