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

package org.apache.carbondata.core.datastore.chunk.impl;

import java.util.BitSet;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.chunk.DimensionColumnPage;
import org.apache.carbondata.core.datastore.chunk.store.impl.safe.AbstractNonDictionaryVectorFiller;
import org.apache.carbondata.core.datastore.chunk.store.impl.safe.NonDictionaryVectorFillerFactory;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.scan.executor.util.QueryUtil;
import org.apache.carbondata.core.scan.result.vector.CarbonColumnVector;
import org.apache.carbondata.core.scan.result.vector.ColumnVectorInfo;
import org.apache.carbondata.core.scan.result.vector.impl.directread.ColumnarVectorWrapperDirectFactory;
import org.apache.carbondata.core.scan.result.vector.impl.directread.ConvertableVector;
import org.apache.carbondata.core.util.ByteUtil;

/**
 * Below class will be used store/fill data for direct string/varchar/ binary type
 */
public class BinaryTypeDimColumnPage implements DimensionColumnPage {

  /**
   * if data was sorted vertically
   */
  protected boolean isExplictSorted;

  /**
   * actual postion of data
   */
  protected int[] invertedIndex;

  /**
   * current position of data after vertical sorting
   */
  protected int[] invertedIndexReverse;

  /**
   * data
   */
  protected byte[] data;

  /**
   * after applying adaptive data type used to store length part
   */
  private DataType lengthStoredType;

  /**
   * data pointer
   */
  private int[] dataOffsets;

  /**
   * total number of rows
   */
  protected int numberOfRows;

  /**
   * to check whether row is null or not
   */
  protected BitSet nullBitSet;

  /**
   * current running offset, will be used during Sequential Filling
   */
  protected int runningOffset;

  /**
   * size of length part in lv format
   */
  private byte lvSize;

  public BinaryTypeDimColumnPage() {

  }
  public BinaryTypeDimColumnPage(byte[] data, int[] invertedIndex, int[] invertedIndexReverse,
      DataType lengthStoredType, int numberOfRows, BitSet nullBitSet,
      ColumnVectorInfo vectorInfo, int actualDataLen) {
    // if vector info is not null it means its for direct fill
    if (vectorInfo == null) {
      putData(data, invertedIndex, invertedIndexReverse, lengthStoredType, numberOfRows,
          nullBitSet);
    } else {
      fillVector(vectorInfo, data, invertedIndex, lengthStoredType, numberOfRows, nullBitSet,
          actualDataLen);
    }
  }

  private void fillVector(ColumnVectorInfo vectorInfo, byte[] data, int[] invertedIndex,
      DataType lengthStoredType, int numberOfRows, BitSet nullBitSet, int actualDataLen) {
    CarbonColumnVector vector = vectorInfo.vector;
    vector.setDictionary(null);
    DataType dt = vector.getType();
    AbstractNonDictionaryVectorFiller vectorFiller = NonDictionaryVectorFillerFactory
        .getVectorFiller(0, dt, numberOfRows, actualDataLen, true, false, lengthStoredType, 0);
    vector = ColumnarVectorWrapperDirectFactory
        .getDirectVectorWrapperFactory(vector, invertedIndex, nullBitSet,
            vectorInfo.deletedRows, true, false);
    vectorFiller.fillVector(data, vector);
    if (vector instanceof ConvertableVector) {
      ((ConvertableVector) vector).convert();
    }
  }

  protected void putData(byte[] data, int[] invertedIndex, int[] invertedIndexReverse,
      DataType lengthStoredType, int numberOfRows, BitSet nullBitSet) {
    this.lengthStoredType = lengthStoredType;
    this.numberOfRows = numberOfRows;
    this.data = data;
    this.invertedIndex = invertedIndex;
    this.invertedIndexReverse = invertedIndexReverse;
    this.isExplictSorted = null != invertedIndex && invertedIndex.length > 0;
    this.nullBitSet = nullBitSet;
    lvSize = (byte) lengthStoredType.getSizeInBytes();
  }

  @Override public int fillRawData(int rowId, int offset, byte[] data) {
    // no required in this case because this column chunk is not the part if
    // mdkey
    return 0;
  }

  @Override public int fillSurrogateKey(int rowId, int chunkIndex, int[] outputSurrogateKey) {
    return chunkIndex + 1;
  }

  @Override public int fillVector(ColumnVectorInfo[] vectorInfo, int chunkIndex) {
    ColumnVectorInfo columnVectorInfo = vectorInfo[chunkIndex];
    CarbonColumnVector vector = columnVectorInfo.vector;
    int offset = columnVectorInfo.offset;
    int vectorOffset = columnVectorInfo.vectorOffset;
    int len = offset + columnVectorInfo.size;
    // for full scan query without inverted index
    if (!isExplictSorted) {
      int localOffset = runningOffset;
      if (lengthStoredType == DataTypes.BYTE) {
        if (nullBitSet.isEmpty()) {
          for (int i = offset; i < len; i++) {
            byte length = data[localOffset];
            localOffset += 1;
            vector.putByteArray(vectorOffset++, localOffset, length, data);
            localOffset += length;
          }
        } else if (!nullBitSet.isEmpty()) {
          for (int i = offset; i < len; i++) {
            if (nullBitSet.get(i)) {
              vector.putNull(vectorOffset);
              localOffset += 1;
            } else {
              byte length = data[localOffset];
              localOffset += 1;
              vector.putByteArray(vectorOffset, localOffset, length, data);
              localOffset += length;
            }
            vectorOffset++;
          }
        } else {
          for (int i = offset; i < len; i++) {
            vector.putNull(vectorOffset);
          }
        }
      } else if (lengthStoredType == DataTypes.SHORT) {
        if (nullBitSet.isEmpty()) {
          for (int i = offset; i < len; i++) {
            int length = (((data[localOffset] & 0xFF) << 8) | (data[localOffset + 1] & 0xFF));
            localOffset += 2;
            vector.putByteArray(vectorOffset++, localOffset, length, data);
            localOffset += length;
          }
        } else if (!nullBitSet.isEmpty()) {
          for (int i = offset; i < len; i++) {
            if (nullBitSet.get(i)) {
              vector.putNull(vectorOffset);
              localOffset += 2;
            } else {
              int length = (((data[localOffset] & 0xFF) << 8) | (data[localOffset + 1] & 0xFF));
              localOffset += 2;
              vector.putByteArray(vectorOffset, localOffset, length, data);
              localOffset += length;
            }
            vectorOffset++;
          }
        } else {
          for (int i = offset; i < len; i++) {
            vector.putNull(vectorOffset);
          }
        }
      } else if (lengthStoredType == DataTypes.SHORT_INT) {
        if (nullBitSet.isEmpty()) {
          for (int i = offset; i < len; i++) {
            int length =
                (((data[localOffset] & 0xFF) << 16) | ((data[localOffset + 1] & 0xFF) << 8) | (
                    data[localOffset + 2] & 0xFF));
            localOffset += 3;
            vector.putByteArray(vectorOffset++, localOffset, length, data);
            localOffset += length;
          }
        } else if (!nullBitSet.isEmpty()) {
          for (int i = offset; i < len; i++) {
            if (nullBitSet.get(i)) {
              vector.putNull(vectorOffset);
              localOffset += 3;
            } else {
              int length =
                  (((data[localOffset] & 0xFF) << 16) | ((data[localOffset + 1] & 0xFF) << 8) | (
                      data[localOffset + 2] & 0xFF));
              localOffset += 3;
              vector.putByteArray(vectorOffset, localOffset, length, data);
              localOffset += length;
            }
            vectorOffset++;
          }
        } else {
          for (int i = offset; i < len; i++) {
            vector.putNull(vectorOffset);
          }
        }
      } else {
        if (nullBitSet.isEmpty()) {
          for (int i = offset; i < len; i++) {
            int length =
                (((data[localOffset] & 0xFF) << 24) | ((data[localOffset + 1] & 0xFF) << 16) | (
                    (data[localOffset + 2] & 0xFF) << 8) | (data[localOffset + 3] & 0xFF));
            localOffset += 4;
            vector.putByteArray(vectorOffset++, localOffset, length, data);
            localOffset += length;
          }
        } else if (!nullBitSet.isEmpty()) {
          for (int i = offset; i < len; i++) {
            if (nullBitSet.get(i)) {
              vector.putNull(vectorOffset);
              localOffset += 4;
            } else {
              int length =
                  (((data[localOffset] & 0xFF) << 24) | ((data[localOffset + 1] & 0xFF) << 16) | (
                      (data[localOffset + 2] & 0xFF) << 8) | (data[localOffset + 3] & 0xFF));
              localOffset += 4;
              vector.putByteArray(vectorOffset, localOffset, length, data);
              localOffset += length;
            }
            vectorOffset++;
          }
        } else {
          for (int i = offset; i < len; i++) {
            vector.putNull(vectorOffset);
          }
        }
      }
      runningOffset = localOffset;
    } else {
      // for inverted index generated data first calculate the offsets and then fill the vector
      if (null == dataOffsets) {
        dataOffsets = QueryUtil.generateOffsetForData(data, numberOfRows, lengthStoredType);
      }
      if (nullBitSet.isEmpty()) {
        for (int i = offset; i < len; i++) {
          int actualIndex = getInvertedReverseIndex(i);
          int currentDataOffset = dataOffsets[actualIndex];
          int nextDataOffset = dataOffsets[actualIndex + 1];
          vector.putByteArray(vectorOffset++, currentDataOffset,
              nextDataOffset - currentDataOffset - lvSize, data);
        }
      } else if (!nullBitSet.isEmpty()) {
        for (int i = offset; i < len; i++) {
          if (nullBitSet.get(i)) {
            vector.putNull(vectorOffset);
          } else {
            int actualIndex = getInvertedReverseIndex(i);
            int currentDataOffset = dataOffsets[actualIndex];
            int nextDataOffset = dataOffsets[actualIndex + 1];
            vector.putByteArray(vectorOffset, currentDataOffset,
                nextDataOffset - currentDataOffset - lvSize, data);
          }
          vectorOffset++;
        }
      } else {
        for (int i = offset; i < len; i++) {
          vector.putNull(vectorOffset);
        }
      }
    }
    return chunkIndex + 1;
  }

  @Override
  public int fillVector(int[] filteredRowId, ColumnVectorInfo[] vectorInfo, int chunkIndex) {
    if (filteredRowId.length == numberOfRows) {
      fillVector(vectorInfo, chunkIndex);
    } else {
      ColumnVectorInfo columnVectorInfo = vectorInfo[chunkIndex];
      CarbonColumnVector vector = columnVectorInfo.vector;
      int offset = columnVectorInfo.offset;
      int vectorOffset = columnVectorInfo.vectorOffset;
      int len = offset + columnVectorInfo.size;
      if (null == dataOffsets) {
        dataOffsets = QueryUtil.generateOffsetForData(data, numberOfRows, lengthStoredType);
      }
      if (!isExplictSorted) {
        if (nullBitSet.isEmpty()) {
          for (int i = offset; i < len; i++) {
            int filteredIndex = filteredRowId[i];
            int currentDataOffset = dataOffsets[filteredIndex];
            int nextDataOffset = dataOffsets[filteredIndex + 1];
            vector.putByteArray(vectorOffset++, currentDataOffset,
                nextDataOffset - currentDataOffset - lvSize, data);
          }
        } else if (!nullBitSet.isEmpty()) {
          for (int i = offset; i < len; i++) {
            int filteredIndex = filteredRowId[i];
            if (nullBitSet.get(filteredIndex)) {
              vector.putNull(vectorOffset);
            } else {
              int currentDataOffset = dataOffsets[filteredIndex];
              int nextDataOffset = dataOffsets[filteredIndex + 1];
              vector.putByteArray(vectorOffset, currentDataOffset,
                  nextDataOffset - currentDataOffset - lvSize, data);
            }
            vectorOffset++;
          }
        } else {
          for (int i = offset; i < len; i++) {
            vector.putNull(vectorOffset);
          }
        }
      } else {
        if (nullBitSet.isEmpty()) {
          for (int i = offset; i < len; i++) {
            int filteredIndex = getInvertedReverseIndex(filteredRowId[i]);
            int currentDataOffset = dataOffsets[filteredIndex];
            int nextDataOffset = dataOffsets[filteredIndex + 1];
            vector.putByteArray(vectorOffset++, currentDataOffset,
                nextDataOffset - currentDataOffset - lvSize, data);
          }
        } else if (!nullBitSet.isEmpty()) {
          for (int i = offset; i < len; i++) {
            int filteredIndex = filteredRowId[i];
            if (nullBitSet.get(filteredIndex)) {
              vector.putNull(vectorOffset);
            } else {
              int actualIndex = getInvertedReverseIndex(filteredIndex);
              int currentDataOffset = dataOffsets[actualIndex];
              int nextDataOffset = dataOffsets[actualIndex + 1];
              vector.putByteArray(vectorOffset, currentDataOffset,
                  nextDataOffset - currentDataOffset - lvSize, data);
            }
            vectorOffset++;
          }
        } else {
          for (int i = offset; i < len; i++) {
            vector.putNull(vectorOffset);
          }
        }
      }
    }
    return chunkIndex + 1;
  }

  @Override public byte[] getChunkData(int rowId) {
    if (nullBitSet.get(rowId)) {
      return CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY;
    }
    if (null == dataOffsets) {
      dataOffsets = QueryUtil.generateOffsetForData(data, numberOfRows, lengthStoredType);
    }
    if (isExplictSorted) {
      rowId = invertedIndexReverse[rowId];
    }
    int currentOffset = dataOffsets[rowId];
    int offsetOfNext = dataOffsets[rowId + 1];
    byte[] currentData = new byte[offsetOfNext - currentOffset - lvSize];
    System.arraycopy(data, currentOffset, currentData, 0, currentData.length);
    return currentData;

  }

  @Override public int getInvertedIndex(int rowId) {
    return invertedIndex[rowId];
  }

  @Override public int getInvertedReverseIndex(int rowId) {
    return invertedIndexReverse[rowId];
  }

  @Override public boolean isNoDicitionaryColumn() {
    return true;
  }

  @Override public boolean isExplicitSorted() {
    return isExplictSorted;
  }

  @Override public int compareTo(int rowId, byte[] compareValue) {
    if (null == dataOffsets) {
      this.dataOffsets = QueryUtil.generateOffsetForData(data, numberOfRows, lengthStoredType);
    }
    int currentOffset = dataOffsets[rowId];
    int nextOffset = dataOffsets[rowId + 1];
    byte[] filterValue = (byte[]) compareValue;
    return ByteUtil.UnsafeComparer.INSTANCE
        .compareTo(data, currentOffset, nextOffset - currentOffset - lvSize, filterValue, 0,
            filterValue.length);
  }

  @Override public void freeMemory() {
    if (null != data) {
      data = null;
      invertedIndex = null;
      invertedIndexReverse = null;
      dataOffsets = null;
    }
  }

  @Override public boolean isAdaptiveEncoded() {
    return false;
  }

  @Override public BitSet getNullBits() {
    return nullBitSet;
  }
}
