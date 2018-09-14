package org.apache.carbondata.core.datastore.chunk.impl;

import java.util.BitSet;

import org.apache.carbondata.core.datastore.chunk.store.DimensionChunkStoreFactory;
import org.apache.carbondata.core.scan.result.vector.CarbonColumnVector;
import org.apache.carbondata.core.scan.result.vector.ColumnVectorInfo;

public class BinaryDimColumnPage extends VariableLengthDimensionColumnPage {

  private BitSet bitSet;

  private boolean isNullBitset;

  /**
   * Constructor for this class
   *
   * @param dataChunks
   * @param invertedIndex
   * @param invertedIndexReverse
   */

  public BinaryDimColumnPage(byte[] dataChunks, int[] invertedIndex, int[] invertedIndexReverse,
      int numberOfRows, int[] offset, BitSet bitSet, boolean isNullBitset) {
    super(dataChunks, invertedIndex, invertedIndexReverse, numberOfRows,
        DimensionChunkStoreFactory.DimensionStoreType.LV_STORE, null, offset);
    this.bitSet = bitSet;
    this.isNullBitset = isNullBitset;
  }

  /**
   * Fill the data to vector
   *
   * @param vectorInfo
   * @param chunkIndex
   * @return next column index
   */
  @Override public int fillVector(ColumnVectorInfo[] vectorInfo, int chunkIndex) {
    ColumnVectorInfo columnVectorInfo = vectorInfo[chunkIndex];
    CarbonColumnVector vector = columnVectorInfo.vector;
    int offset = columnVectorInfo.offset;
    int vectorOffset = columnVectorInfo.vectorOffset;
    int len = offset + columnVectorInfo.size;
    if (isNullBitset && bitSet.isEmpty()) {
      for (int i = offset; i < len; i++) {
        // Considering only String case now as we support only
        // string in no dictionary case at present.
        dataChunkStore.fillRow(i, vector, vectorOffset++);
      }
    } else if (isNullBitset && !bitSet.isEmpty()) {
      for (int i = offset; i < len; i++) {
        if (bitSet.get(i)) {
          vector.putNull(vectorOffset);
        } else {
          dataChunkStore.fillRow(i, vector, vectorOffset);
        }
        vectorOffset++;
      }
    } else if (!isNullBitset && bitSet.isEmpty()) {
      for (int i = offset; i < len; i++) {
        vector.putNull(vectorOffset);
      }
    } else {
      for (int i = offset; i < len; i++) {
        if (bitSet.get(i)) {
          dataChunkStore.fillRow(i, vector, vectorOffset);
        } else {
          vector.putNull(vectorOffset);
        }
        vectorOffset++;
      }
    }
    return chunkIndex + 1;
  }

  /**
   * Fill the data to vector
   *
   * @param filteredRowId
   * @param vectorInfo
   * @param chunkIndex
   * @return next column index
   */
  @Override public int fillVector(int[] filteredRowId, ColumnVectorInfo[] vectorInfo,
      int chunkIndex) {
    ColumnVectorInfo columnVectorInfo = vectorInfo[chunkIndex];
    CarbonColumnVector vector = columnVectorInfo.vector;
    int offset = columnVectorInfo.offset;
    int vectorOffset = columnVectorInfo.vectorOffset;
    int len = offset + columnVectorInfo.size;
    if (isNullBitset && bitSet.isEmpty()) {
      for (int i = offset; i < len; i++) {
        // Considering only String case now as we support only
        // string in no dictionary case at present.
        dataChunkStore.fillRow(filteredRowId[i], vector, vectorOffset++);
      }
    } else if (isNullBitset && !bitSet.isEmpty()) {
      for (int i = offset; i < len; i++) {
        int actualRowId = dataChunkStore.getInvertedReverseIndex(filteredRowId[i]);
        if (bitSet.get(actualRowId)) {
          vector.putNull(vectorOffset);
        } else {
          dataChunkStore.fillRow(filteredRowId[i], vector, vectorOffset);
        }
        vectorOffset++;
      }
    } else if (!isNullBitset && bitSet.isEmpty()) {
      for (int i = offset; i < len; i++) {
        vector.putNull(vectorOffset);
      }
    } else {
      for (int i = offset; i < len; i++) {
        int actualRowId = dataChunkStore.getInvertedReverseIndex(filteredRowId[i]);
        if (bitSet.get(actualRowId)) {
          dataChunkStore.fillRow(filteredRowId[i], vector, vectorOffset);
        } else {
          vector.putNull(vectorOffset);
        }
        vectorOffset++;
      }
    }
    return chunkIndex + 1;
  }

}
