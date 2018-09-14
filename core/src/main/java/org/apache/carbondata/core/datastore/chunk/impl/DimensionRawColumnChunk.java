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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.List;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.FileReader;
import org.apache.carbondata.core.datastore.chunk.AbstractRawColumnChunk;
import org.apache.carbondata.core.datastore.chunk.DimensionColumnPage;
import org.apache.carbondata.core.datastore.chunk.reader.DimensionColumnChunkReader;
import org.apache.carbondata.core.datastore.compression.CompressorFactory;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageDecoder;
import org.apache.carbondata.core.datastore.page.encoding.DefaultEncodingFactory;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.scan.executor.util.QueryUtil;
import org.apache.carbondata.core.scan.result.vector.CarbonDictionary;
import org.apache.carbondata.core.scan.result.vector.impl.CarbonDictionaryImpl;
import org.apache.carbondata.format.Encoding;
import org.apache.carbondata.format.LocalDictionaryChunk;

/**
 * Contains raw dimension data,
 * 1. The read uncompressed raw data of column chunk with all pages is stored in this instance.
 * 2. The raw data can be converted to processed chunk using decodeColumnPage method
 *  by specifying page number.
 */
public class DimensionRawColumnChunk extends AbstractRawColumnChunk {

  private DimensionColumnPage[] dataChunks;

  private DimensionColumnChunkReader chunkReader;

  private FileReader fileReader;

  private CarbonDictionary localDictionary;

  public DimensionRawColumnChunk(int columnIndex, ByteBuffer rawData, long offSet, int length,
      DimensionColumnChunkReader columnChunkReader) {
    super(columnIndex, rawData, offSet, length);
    this.chunkReader = columnChunkReader;
  }

  /**
   * Convert all raw data with all pages to processed DimensionColumnPage's
   * @return
   */
  public DimensionColumnPage[] decodeAllColumnPages() {
    if (dataChunks == null) {
      dataChunks = new DimensionColumnPage[pagesCount];
    }
    for (int i = 0; i < pagesCount; i++) {
      try {
        if (dataChunks[i] == null) {
          dataChunks[i] = chunkReader.decodeColumnPage(this, i);
        }
      } catch (IOException | MemoryException e) {
        throw new RuntimeException(e);
      }
    }
    return dataChunks;
  }

  /**
   * Convert raw data with specified page number processed to DimensionColumnPage
   * @param pageNumber
   * @return
   */
  public DimensionColumnPage decodeColumnPage(int pageNumber) {
    assert pageNumber < pagesCount;
    if (dataChunks == null) {
      dataChunks = new DimensionColumnPage[pagesCount];
    }
    if (dataChunks[pageNumber] == null) {
      try {
        dataChunks[pageNumber] = chunkReader.decodeColumnPage(this, pageNumber);
      } catch (IOException | MemoryException e) {
        throw new RuntimeException(e);
      }
    }

    return dataChunks[pageNumber];
  }

  /**
   * Convert raw data with specified page number processed to DimensionColumnDataChunk
   *
   * @param index
   * @return
   */
  public DimensionColumnPage convertToDimColDataChunkWithOutCache(int index) {
    assert index < pagesCount;
    // in case of filter query filter column if filter column is decoded and stored.
    // then return the same
    if (dataChunks != null && null != dataChunks[index]) {
      return dataChunks[index];
    }
    try {
      return chunkReader.decodeColumnPage(this, index);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override public void freeMemory() {
    super.freeMemory();
    if (null != dataChunks) {
      for (int i = 0; i < dataChunks.length; i++) {
        if (dataChunks[i] != null) {
          dataChunks[i].freeMemory();
          dataChunks[i] = null;
        }
      }
    }
    rawData = null;
  }

  public void setFileReader(FileReader fileReader) {
    this.fileReader = fileReader;
  }

  public FileReader getFileReader() {
    return fileReader;
  }

  public CarbonDictionary getLocalDictionary() {
    if (null != getDataChunkV3() && null != getDataChunkV3().local_dictionary
        && null == localDictionary) {
      try {
        localDictionary = getDictionary(getDataChunkV3().local_dictionary);
      } catch (IOException | MemoryException e) {
        throw new RuntimeException(e);
      }
    }
    return localDictionary;
  }

  /**
   * Below method will be used to get the local dictionary for a blocklet
   * @param localDictionaryChunk
   * local dictionary chunk thrift object
   * @return local dictionary
   * @throws IOException
   * @throws MemoryException
   */
  private CarbonDictionary getDictionary(LocalDictionaryChunk localDictionaryChunk)
      throws IOException, MemoryException {
    if (null != localDictionaryChunk) {
      byte[][] dictionaryPage = null;
      BitSet usedDictionary = BitSet.valueOf(CompressorFactory.getInstance().getCompressor()
          .unCompressByte(localDictionaryChunk.getDictionary_values()));
      int length = usedDictionary.length();
      byte[][] dictionary = null;
      if (localDictionaryChunk.getDictionary_meta().getEncoders().size() <= 1) {
        dictionaryPage = getDictionaryPage(localDictionaryChunk);
        int index = 0;
        dictionary = new byte[length][];
        for (int i = 0; i < length; i++) {
          if (usedDictionary.get(i)) {
            dictionary[i] = dictionaryPage[index++];
          } else {
            dictionary[i] = null;
          }
        }
      } else {
        int offset = 0;
        int lv_lengthSize = localDictionaryChunk.dictionary_data.getInt(offset);
        offset += CarbonCommonConstants.INT_SIZE_IN_BYTE;
        int[] rowLength = QueryUtil.getDataLengthArray(localDictionaryChunk.dictionary_data, offset,
            localDictionaryChunk.getDictionary_meta().getEncoders(), lv_lengthSize,
            localDictionaryChunk.getDictionary_meta().getEncoder_meta());
        offset += lv_lengthSize;
        byte[] bytes = CompressorFactory.getInstance().getCompressor()
            .unCompressByte(localDictionaryChunk.getDictionary_data(), offset,
                localDictionaryChunk.getDictionary_data().length - offset);
        dictionary = new byte[length][];
        int dicOffset = 0;
        int dataOffset = 0;
        for (int i = 0; i < length; i++) {
          if (usedDictionary.get(i)) {
            dictionary[i] = new byte[rowLength[dicOffset]];
            System.arraycopy(bytes, dataOffset, dictionary[i], 0, rowLength[dicOffset]);
            dataOffset+=rowLength[dicOffset];
            dicOffset++;
          } else {
            dictionary[i] = null;
          }
        }
      }
      // as dictionary values starts from 1 setting null default value
      dictionary[1] = CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY;
      return new CarbonDictionaryImpl(dictionary, usedDictionary.cardinality());
    }
    return null;
  }

  private byte[][] getDictionaryPage(LocalDictionaryChunk localDictionaryChunk)
      throws IOException, MemoryException {
    List<Encoding> encodings = localDictionaryChunk.getDictionary_meta().getEncoders();
    List<ByteBuffer> encoderMetas = localDictionaryChunk.getDictionary_meta().getEncoder_meta();
    ColumnPageDecoder decoder =
        DefaultEncodingFactory.getInstance().createDecoder(encodings, encoderMetas);
    ColumnPage decode = decoder.decode(localDictionaryChunk.getDictionary_data(), 0,
        localDictionaryChunk.getDictionary_data().length);
    byte[][] byteArrayPage = decode.getByteArrayPage();
    decode.freeMemory();
    return byteArrayPage;
  }

  private byte[][] getDictionaryPage1(LocalDictionaryChunk localDictionaryChunk)
      throws IOException, MemoryException {
    List<Encoding> encodings = localDictionaryChunk.getDictionary_meta().getEncoders();
    List<ByteBuffer> encoderMetas = localDictionaryChunk.getDictionary_meta().getEncoder_meta();
    ColumnPageDecoder decoder =
        DefaultEncodingFactory.getInstance().createDecoder(encodings, encoderMetas);
    ColumnPage decode = decoder.decode(localDictionaryChunk.getDictionary_data(), 0,
        localDictionaryChunk.getDictionary_data().length);
    byte[][] byteArrayPage = decode.getByteArrayPage();
    decode.freeMemory();
    return byteArrayPage;
  }

}
