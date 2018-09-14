/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.carbondata.core.datastore.chunk.store.impl.unsafe;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.memory.CarbonUnsafe;
import org.apache.carbondata.core.scan.result.vector.CarbonColumnVector;

public class UnsafeLVDimChunkStore extends UnsafeAbstractDimensionDataChunkStore {

  private int[] offset;

  private byte[] value;

  /**
   * Constructor
   *
   * @param totalSize      total size of the data to be kept
   * @param isInvertedIdex is inverted index present
   * @param numberOfRows   total number of rows
   */
  public UnsafeLVDimChunkStore(long totalSize, boolean isInvertedIdex, int numberOfRows,
      int[] offset) {
    super(totalSize, isInvertedIdex, numberOfRows);
    this.value = new byte[20];
    this.offset = offset;
  }

  /**
   * Below method will be used to put the rows and its metadata in offheap
   *
   * @param invertedIndex        inverted index to be stored
   * @param invertedIndexReverse inverted index reverse to be stored
   * @param data                 data to be stored
   */
  @Override public void putArray(final int[] invertedIndex, final int[] invertedIndexReverse,
      byte[] data) {
    super.putArray(invertedIndex, invertedIndexReverse, data);
  }

  @Override public byte[] getRow(int rowId) {
    // get the actual row id
    rowId = getRowId(rowId);
    byte[] data = new byte[offset[rowId + 1] - offset[rowId - 1]];
    fillRowInternal(data.length, data, offset[rowId]);
    return data;
  }

  /**
   * Returns the actual row id for data
   * if inverted index is present then get the row id based on reverse inverted index
   * otherwise return the same row id
   *
   * @param rowId row id
   * @return actual row id
   */
  private int getRowId(int rowId) {
    // if column was explicitly sorted we need to get the rowid based inverted index reverse
    if (isExplicitSorted) {
      rowId = CarbonUnsafe.getUnsafe().getInt(dataPageMemoryBlock.getBaseObject(),
          dataPageMemoryBlock.getBaseOffset() + this.invertedIndexReverseOffset + ((long) rowId
              * CarbonCommonConstants.INT_SIZE_IN_BYTE));
    }
    return rowId;
  }

  /**
   * Return the row from unsafe
   *
   * @param length            length of the data
   * @param data              data array
   * @param currentDataOffset current data offset
   */
  private void fillRowInternal(int length, byte[] data, int currentDataOffset) {
    CarbonUnsafe.getUnsafe().copyMemory(dataPageMemoryBlock.getBaseObject(),
        dataPageMemoryBlock.getBaseOffset() + currentDataOffset, data,
        CarbonUnsafe.BYTE_ARRAY_OFFSET, length);
  }

  @Override public void fillRow(int rowId, CarbonColumnVector vector, int vectorRow) {
    // get the row id from reverse inverted index based on row id
    rowId = getRowId(rowId);
    // get the current row offset
    int currentDataOffset = offset[rowId];
    // get the row data length
    int length = offset[rowId + 1] - offset[rowId];
    // check if value length is less the current data length
    // then create a new array else use the same
    if (length > value.length) {
      value = new byte[length];
    }
    // get the row from unsafe
    fillRowInternal(length, value, currentDataOffset);
    vector.putBytes(vectorRow, 0, length, value);
  }

  @Override public int compareTo(int rowId, byte[] compareValue) {
    // as this class handles this variable length data, so filter value can be
    // smaller or bigger than than actual data, so we need to take the smaller length
    int length = offset[rowId + 1] - offset[rowId - 1];
    int currentDataOffset = offset[rowId];
    int compareResult;
    int compareLength = Math.min(length, compareValue.length);
    for (int i = 0; i < compareLength; i++) {
      compareResult = (CarbonUnsafe.getUnsafe().getByte(dataPageMemoryBlock.getBaseObject(),
          dataPageMemoryBlock.getBaseOffset() + offset[rowId]) & 0xff) - (compareValue[i] & 0xff);
      // if compare result is not equal we can break
      if (compareResult != 0) {
        return compareResult;
      }
      // increment the offset by one as comparison is done byte by byte
      currentDataOffset++;
    }
    return length - compareValue.length;
  }
}
