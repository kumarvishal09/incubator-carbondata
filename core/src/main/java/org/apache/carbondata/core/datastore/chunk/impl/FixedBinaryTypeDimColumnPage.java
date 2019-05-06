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

import org.apache.carbondata.core.datastore.chunk.store.impl.safe.AbstractNonDictionaryVectorFiller;
import org.apache.carbondata.core.datastore.chunk.store.impl.safe.NonDictionaryVectorFillerFactory;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.scan.result.vector.CarbonColumnVector;
import org.apache.carbondata.core.scan.result.vector.ColumnVectorInfo;
import org.apache.carbondata.core.scan.result.vector.impl.directread.ColumnarVectorWrapperDirectFactory;
import org.apache.carbondata.core.scan.result.vector.impl.directread.ConvertableVector;
import org.apache.carbondata.core.util.ByteUtil;

public class FixedBinaryTypeDimColumnPage extends BinaryTypeDimColumnPage {

  private int eachRowLength;

  public FixedBinaryTypeDimColumnPage(byte[] data, int[] invertedIndex, int[] invertedIndexReverse,
      int numberOfRows, BitSet nullBitSet, int eachRowLength, ColumnVectorInfo vectorInfo,
      int actualDataLen) {
    this.eachRowLength = eachRowLength;
    if (null != vectorInfo) {
      fillVector(vectorInfo, data, invertedIndex, numberOfRows, nullBitSet, actualDataLen,
          eachRowLength);
    } else {
      putData(data, invertedIndex, invertedIndexReverse, DataTypes.INT, numberOfRows, nullBitSet);
    }
  }

  private void fillVector(ColumnVectorInfo vectorInfo, byte[] data, int[] invertedIndex,
      int numberOfRows, BitSet nullBitSet, int actualDataLen, int eachRowLength) {
    CarbonColumnVector vector = vectorInfo.vector;
    vector.setDictionary(null);
    DataType dt = vector.getType();
    AbstractNonDictionaryVectorFiller vectorFiller = NonDictionaryVectorFillerFactory
        .getVectorFiller(0, dt, numberOfRows, actualDataLen, false, true, DataTypes.BYTE,
            eachRowLength);
    vector = ColumnarVectorWrapperDirectFactory
        .getDirectVectorWrapperFactory(vector, invertedIndex, nullBitSet,
            vectorInfo.deletedRows, true, false);
    vectorFiller.fillVector(data, vector);
    if (vector instanceof ConvertableVector) {
      ((ConvertableVector) vector).convert();
    }
  }

  @Override public byte[] getChunkData(int rowId) {
    if (isExplictSorted) {
      rowId = invertedIndexReverse[rowId];
    }
    byte[] currentData = new byte[eachRowLength];
    System.arraycopy(data, rowId * eachRowLength, currentData, 0, currentData.length);
    return currentData;
  }

  @Override public int compareTo(int rowId, byte[] compareValue) {
    byte[] filterValue = (byte[]) compareValue;
    return ByteUtil.UnsafeComparer.INSTANCE
        .compareTo(data, rowId * eachRowLength, eachRowLength, filterValue, 0, filterValue.length);
  }

  @Override public int fillVector(ColumnVectorInfo[] vectorInfo, int chunkIndex) {
    ColumnVectorInfo columnVectorInfo = vectorInfo[chunkIndex];
    CarbonColumnVector vector = columnVectorInfo.vector;
    int offset = columnVectorInfo.offset;
    int vectorOffset = columnVectorInfo.vectorOffset;
    int len = offset + columnVectorInfo.size;
    if (!isExplictSorted) {
      for (int i = offset; i < len; i++) {
        vector.putByteArray(vectorOffset++, runningOffset, eachRowLength, data);
        runningOffset += eachRowLength;
      }
    } else {
      for (int i = offset; i < len; i++) {
        int actualIndex = getInvertedReverseIndex(i);
        vector.putByteArray(vectorOffset++, actualIndex * eachRowLength, eachRowLength, data);
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
      if (!isExplictSorted) {
        for (int i = offset; i < len; i++) {
          int filteredIndex = filteredRowId[i];
          vector.putByteArray(vectorOffset++, filteredIndex * eachRowLength, eachRowLength, data);
        }
      } else {
        for (int i = offset; i < len; i++) {
          int filteredIndex = getInvertedReverseIndex(filteredRowId[i]);
          vector.putByteArray(vectorOffset++, filteredIndex * eachRowLength, eachRowLength, data);
        }
      }
    }
    return chunkIndex + 1;
  }
}