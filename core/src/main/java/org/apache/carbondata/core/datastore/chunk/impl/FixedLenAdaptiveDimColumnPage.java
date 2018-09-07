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

import org.apache.carbondata.core.datastore.chunk.DimensionColumnPage;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.keygenerator.KeyGenerator;
import org.apache.carbondata.core.keygenerator.factory.KeyGeneratorFactory;
import org.apache.carbondata.core.metadata.blocklet.PresenceMeta;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.scan.result.vector.CarbonColumnVector;
import org.apache.carbondata.core.scan.result.vector.ColumnVectorInfo;
import org.apache.carbondata.core.util.comparator.Comparator;
import org.apache.carbondata.core.util.comparator.SerializableComparator;

public class FixedLenAdaptiveDimColumnPage implements DimensionColumnPage {

  private final PresenceMeta presenceMeta;
  /**
   * data page
   */
  private ColumnPage columnPage;
  /**
   * inverted index
   */
  private int[] invertedIndex;
  /**
   * inverted reverse index
   */
  private int[] invertedIndexReverse;

  /**
   * size of each row
   */
  private int eachRowSize;

  /**
   * keygenerator
   */
  private KeyGenerator keyGenerator;
  /**
   * whether data was already sorted
   */
  private boolean isExplictSorted;

  /**
   * reused array for generating byte array for column
   */
  private final int[] dummy = new int[1];

  private SerializableComparator serializableComparator;

  public FixedLenAdaptiveDimColumnPage(ColumnPage columnPage, int[] invertedIndex,
      int[] invertedIndexReverse, int eachRowSize, PresenceMeta presenceMeta) {
    this.keyGenerator = KeyGeneratorFactory.getKeyGenerator(new int[] { eachRowSize });
    this.isExplictSorted = null != invertedIndex;
    this.presenceMeta = presenceMeta;
    this.eachRowSize = eachRowSize;
    this.columnPage = columnPage;
    this.invertedIndexReverse = invertedIndexReverse;
    this.serializableComparator = Comparator.getComparator(DataTypes.INT);
  }

  @Override public int fillRawData(int rowId, int offset, byte[] data) {
    dummy[0] = getDataBasedOnActualRowId(rowId);
    byte[] bytes = keyGenerator.generateKey(dummy);
    System.arraycopy(bytes, 0, data, offset, eachRowSize);
    return eachRowSize;
  }

  @Override public int fillSurrogateKey(int rowId, int chunkIndex, int[] outputSurrogateKey) {
    if (isExplictSorted) {
      rowId = getInvertedReverseIndex(rowId);
    }
    outputSurrogateKey[chunkIndex] = getDataBasedOnActualRowId(rowId);
    return chunkIndex + 1;
  }

  private int getDataBasedOnActualRowId(int rowId) {
    if (isExplictSorted) {
      return columnPage.getInt(getInvertedReverseIndex(rowId));
    }
    return columnPage.getInt(rowId);
  }

  @Override public int fillVector(ColumnVectorInfo[] vectorInfo, int chunkIndex) {
    ColumnVectorInfo columnVectorInfo = vectorInfo[chunkIndex];
    int cutoffdate =
        columnVectorInfo.directDictionaryGenerator == null ? 0 : Integer.MAX_VALUE >> 1;
    int offset = columnVectorInfo.offset;
    int vectorOffset = columnVectorInfo.vectorOffset;
    int len = columnVectorInfo.size + offset;
    CarbonColumnVector vector = columnVectorInfo.vector;
    if (presenceMeta.isNullBitset() && presenceMeta.getBitSet().isEmpty()) {
      for (int j = offset; j < len; j++) {
        vector.putInt(vectorOffset++, getDataBasedOnActualRowId(j) - cutoffdate);
      }
    } else if (presenceMeta.isNullBitset() && !presenceMeta.getBitSet().isEmpty()) {
      for (int j = offset; j < len; j++) {
        if (presenceMeta.getBitSet().get(j)) {
          vector.putNull(vectorOffset++);
        } else {
          vector.putInt(vectorOffset++, getDataBasedOnActualRowId(j) - cutoffdate);
        }
      }
    } else if (!presenceMeta.isNullBitset() && presenceMeta.getBitSet().isEmpty()) {
      for (int j = offset; j < len; j++) {
        vector.putNull(vectorOffset++);
      }
    } else {
      for (int j = offset; j < len; j++) {
        if (presenceMeta.getBitSet().get(j)) {
          vector.putInt(vectorOffset++, getDataBasedOnActualRowId(j) - cutoffdate);
        } else {
          vector.putNull(vectorOffset++);
        }
      }
    }
    return chunkIndex + 1;
  }

  @Override
  public int fillVector(int[] filteredRowId, ColumnVectorInfo[] vectorInfo, int chunkIndex) {
    ColumnVectorInfo columnVectorInfo = vectorInfo[chunkIndex];
    int cutoffdate =
        columnVectorInfo.directDictionaryGenerator == null ? 0 : Integer.MAX_VALUE >> 1;
    int offset = columnVectorInfo.offset;
    int vectorOffset = columnVectorInfo.vectorOffset;
    int len = columnVectorInfo.size + offset;
    CarbonColumnVector vector = columnVectorInfo.vector;
    if (presenceMeta.isNullBitset() && presenceMeta.getBitSet().isEmpty()) {
      for (int j = offset; j < len; j++) {
        vector.putInt(vectorOffset++, getDataBasedOnActualRowId(filteredRowId[j]) - cutoffdate);
      }
    } else if (presenceMeta.isNullBitset() && !presenceMeta.getBitSet().isEmpty()) {
      for (int j = offset; j < len; j++) {
        int rowId = filteredRowId[getActualRowId(j)];
        if (presenceMeta.getBitSet().get(rowId)) {
          vector.putNull(vectorOffset++);
        } else {
          vector.putInt(vectorOffset++, getDataBasedOnActualRowId(filteredRowId[j]) - cutoffdate);
        }
      }
    } else if (!presenceMeta.isNullBitset() && presenceMeta.getBitSet().isEmpty()) {
      for (int j = offset; j < len; j++) {
        vector.putNull(vectorOffset++);
      }
    } else {
      for (int j = offset; j < len; j++) {
        int rowId = filteredRowId[getActualRowId(j)];
        if (presenceMeta.getBitSet().get(rowId)) {
          vector.putInt(vectorOffset++, getDataBasedOnActualRowId(filteredRowId[j]) - cutoffdate);
        } else {
          vector.putNull(vectorOffset++);
        }
      }
    }
    return chunkIndex + 1;
  }

  @Override public byte[] getChunkData(int rowId) {
    return new byte[0];
  }

  @Override public int getInvertedIndex(int rowId) {
    return invertedIndex[rowId];
  }

  @Override public int getInvertedReverseIndex(int rowId) {
    return invertedIndexReverse[rowId];
  }

  @Override public boolean isNoDicitionaryColumn() {
    return false;
  }

  @Override public boolean isExplicitSorted() {
    return null != invertedIndex;
  }

  @Override public int compareTo(int rowId, Object compareValue) {
    return this.serializableComparator.compare(columnPage.getInt(rowId), compareValue);
  }

  @Override public void freeMemory() {
    this.columnPage.freeMemory();
    this.invertedIndexReverse = null;
    this.invertedIndex = null;
  }

  @Override public boolean isAdaptiveEncoded() {
    return true;
  }

  @Override public PresenceMeta getPresentMeta() {
    return presenceMeta;
  }

  private int getActualRowId(int rowId) {
    if (isExplictSorted) {
      return getInvertedReverseIndex(rowId);
    }
    return rowId;
  }
}
