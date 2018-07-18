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

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import io.druid.segment.DimensionIndexer;
import oak.OakComparator;
import java.nio.ByteBuffer;
import java.util.List;

public class OffheapOakSerializationsComparator implements OakComparator<ByteBuffer, ByteBuffer>
{

  private List<IncrementalIndex.DimensionDesc> dimensionDescsList;

  public OffheapOakSerializationsComparator(List<IncrementalIndex.DimensionDesc> dimensionDescsList)
  {
    this.dimensionDescsList = dimensionDescsList;
  }


  @Override
  public int compare(ByteBuffer lhs, ByteBuffer rhs)
  {
    int retVal = Longs.compare(OffheapOakIncrementalIndex.getTimestamp(lhs), OffheapOakIncrementalIndex.getTimestamp(rhs));
    int numComparisons = Math.min(OffheapOakIncrementalIndex.getDimsLength(lhs), OffheapOakIncrementalIndex.getDimsLength(rhs));

    int dimIndex = 0;
    while (retVal == 0 && dimIndex < numComparisons) {
      int lhsType = lhs.getInt(OffheapOakIncrementalIndex.getDimIndexInBuffer(lhs, dimIndex));
      int rhsType = rhs.getInt(OffheapOakIncrementalIndex.getDimIndexInBuffer(rhs, dimIndex));

      if (lhsType == OffheapOakIncrementalIndex.NO_DIM) {
        if (rhsType == OffheapOakIncrementalIndex.NO_DIM) {
          ++dimIndex;
          continue;
        }
        return -1;
      }

      if (rhsType == OffheapOakIncrementalIndex.NO_DIM) {
        return 1;
      }

      final DimensionIndexer indexer = dimensionDescsList.get(dimIndex).getIndexer();
      Object lhsObject = OffheapOakIncrementalIndex.getDimValue(lhs, dimIndex);
      Object rhsObject = OffheapOakIncrementalIndex.getDimValue(rhs, dimIndex);
      retVal = indexer.compareUnsortedEncodedKeyComponents(lhsObject, rhsObject);
      ++dimIndex;
    }

    if (retVal == 0) {
      int lengthDiff = Ints.compare(OffheapOakIncrementalIndex.getDimsLength(lhs), OffheapOakIncrementalIndex.getDimsLength(rhs));
      if (lengthDiff == 0) {
        return 0;
      }
      ByteBuffer largerDims = lengthDiff > 0 ? lhs : rhs;
      return OffheapOakIncrementalIndex.checkDimsAllNull(largerDims, numComparisons) ? 0 : lengthDiff;
    }
    return retVal;
  }
}
