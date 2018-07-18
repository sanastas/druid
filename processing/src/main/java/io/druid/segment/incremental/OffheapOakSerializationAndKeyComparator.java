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

public class OffheapOakSerializationAndKeyComparator implements OakComparator<ByteBuffer, IncrementalIndexRow>
{
  private List<IncrementalIndex.DimensionDesc> dimensionDescsList;

  public OffheapOakSerializationAndKeyComparator(List<IncrementalIndex.DimensionDesc> dimensionDescsList)
  {
    this.dimensionDescsList = dimensionDescsList;
  }

  @Override
  public int compare(ByteBuffer lhs, IncrementalIndexRow rhs)
  {
    int retVal = Longs.compare(OffheapOakIncrementalIndex.getTimestamp(lhs), rhs.getTimestamp());
    int lhsDimsLength = OffheapOakIncrementalIndex.getDimsLength(lhs);
    int rhsDimsLength = rhs.getDims() == null ? 0 : rhs.getDims().length;
    int numComparisons = Math.min(lhsDimsLength, rhsDimsLength);

    int index = 0;
    while (retVal == 0 && index < numComparisons) {
      final Object lhsIdxs = OffheapOakIncrementalIndex.getDimValue(lhs, index);
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

      if (lengthDiff > 0) {
        return OffheapOakIncrementalIndex.checkDimsAllNull(lhs, numComparisons) ? 0 : lengthDiff;
      } else {
        return OffheapOakIncrementalIndex.allNull(rhs.getDims(), numComparisons) ? 0 : lengthDiff;
      }
    }

    return retVal;
  }
}
