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

package org.apache.carbondata.core.datastore.chunk.store.impl;

import org.apache.carbondata.core.datastore.chunk.store.DimensionDataChunkStore;
import org.apache.carbondata.core.scan.result.vector.CarbonColumnVector;
import org.apache.carbondata.core.scan.result.vector.impl.CarbonDictionaryImpl;

/**
 * Dimension chunk store for local dictionary encoded data.
 * It's a decorator over dimension chunk store
 */
public class LocalDictDimensionDataChunkStore implements DimensionDataChunkStore {

  private DimensionDataChunkStore dimensionDataChunkStore;

  private byte[][] dictionary;

  public LocalDictDimensionDataChunkStore(DimensionDataChunkStore dimensionDataChunkStore,
      byte[][] dictionary) {
    this.dimensionDataChunkStore = dimensionDataChunkStore;
    this.dictionary = dictionary;
  }

  /**
   * Below method will be used to put the rows and its metadata in offheap
   *
   * @param invertedIndex        inverted index to be stored
   * @param invertedIndexReverse inverted index reverse to be stored
   * @param data                 data to be stored
   */
  public void putArray(int[] invertedIndex, int[] invertedIndexReverse, byte[] data) {
    this.dimensionDataChunkStore.putArray(invertedIndex, invertedIndexReverse, data);
  }

  @Override public byte[] getRow(int rowId) {
    return dictionary[dimensionDataChunkStore.getSurrogate(rowId)];
  }

  @Override public void fillRow(int rowId, CarbonColumnVector vector, int vectorRow) {
    if (vector.hasDictionary()) {
      vector.setDictionary(new CarbonDictionaryImpl(dictionary));
    }
    vector.getDictionaryVector().putInt(vectorRow, dimensionDataChunkStore.getSurrogate(rowId));
  }

  @Override public void fillRow(int rowId, byte[] buffer, int offset) {
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override public int getInvertedIndex(int rowId) {
    return this.dimensionDataChunkStore.getInvertedIndex(rowId);
  }

  @Override public int getInvertedReverseIndex(int rowId) {
    return this.dimensionDataChunkStore.getInvertedReverseIndex(rowId);
  }

  @Override public int getSurrogate(int rowId) {
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override public int getColumnValueSize() {
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override public boolean isExplicitSorted() {
    return this.dimensionDataChunkStore.isExplicitSorted();
  }

  @Override public int compareTo(int rowId, byte[] compareValue) {
    return dimensionDataChunkStore.compareTo(rowId, compareValue);
  }

  /**
   * Below method will be used to free the memory occupied by the column chunk
   */
  @Override public void freeMemory() {
    if (null != dimensionDataChunkStore) {
      this.dimensionDataChunkStore.freeMemory();
      this.dictionary = null;
      this.dimensionDataChunkStore = null;
    }
  }
}
