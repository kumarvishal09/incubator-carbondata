/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.carbondata.core.carbon.datastore.chunk.reader.dimension;

import java.io.IOException;
import java.util.List;

import org.apache.carbondata.core.carbon.datastore.chunk.DimensionChunkAttributes;
import org.apache.carbondata.core.carbon.datastore.chunk.DimensionColumnDataChunk;
import org.apache.carbondata.core.carbon.datastore.chunk.impl.ColumnGroupDimensionDataChunk;
import org.apache.carbondata.core.carbon.datastore.chunk.impl.FixedLengthDimensionDataChunk;
import org.apache.carbondata.core.carbon.datastore.chunk.impl.VariableLengthDimensionDataChunk;
import org.apache.carbondata.core.datastorage.store.FileHolder;
import org.apache.carbondata.core.datastorage.store.columnar.UnBlockIndexer;
import org.apache.carbondata.core.reader.CarbonDataChunkReader;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.format.DataChunk;
import org.apache.carbondata.format.Encoding;

/**
 * Compressed dimension chunk reader class
 */
public class CompressedDimensionChunkFileBasedReader extends AbstractChunkReader {

  /**
   * Constructor to get minimum parameter to create instance of this class
   *
   * @param dimensionColumnChunk dimension chunk metadata
   * @param eachColumnValueSize  size of the each column value
   * @param filePath             file from which data will be read
   */
  public CompressedDimensionChunkFileBasedReader(List<Long> dimensionColumnChunkOffsets,
      int[] eachColumnValueSize, String filePath) {
    super(dimensionColumnChunkOffsets, eachColumnValueSize, filePath);
  }

  /**
   * Below method will be used to read the chunk based on block indexes
   *
   * @param fileReader   file reader to read the blocks from file
   * @param blockIndexes blocks to be read
   * @return dimension column chunks
   */
  @Override public DimensionColumnDataChunk[] readDimensionChunks(FileHolder fileReader,
      int... blockIndexes) {
    // read the column chunk based on block index and add
    DimensionColumnDataChunk[] dataChunks =
        new DimensionColumnDataChunk[dimensionColumnChunkOffsets.size()];
    for (int i = 0; i < blockIndexes.length; i++) {
      dataChunks[blockIndexes[i]] = readDimensionChunk(fileReader, blockIndexes[i]);
    }
    return dataChunks;
  }

  /**
   * Below method will be used to read the chunk based on block index
   *
   * @param fileReader file reader to read the blocks from file
   * @param blockIndex block to be read
   * @return dimension column chunk
   */
  @Override public DimensionColumnDataChunk readDimensionChunk(FileHolder fileReader,
      int blockIndex) {
    byte[] dataPage = null;
    int[] invertedIndexes = null;
    int[] invertedIndexesReverse = null;
    int[] rlePage = null;
    DataChunk dimensionColumnChunk = null;
    try {
      dimensionColumnChunk =
          CarbonDataChunkReader.readFooter(filePath, dimensionColumnChunkOffsets.get(blockIndex));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    byte[] data = fileReader.readByteArray(filePath, dimensionColumnChunk.data_page_offset,
        calculateTotalDataLength(dimensionColumnChunk));

    byte[] compressedDataPage = new byte[dimensionColumnChunk.data_page_length];
    System.arraycopy(data, 0, compressedDataPage, 0, dimensionColumnChunk.data_page_length);
    // first read the data and uncompressed it
    dataPage = COMPRESSOR.unCompress(compressedDataPage);
    // if row id block is present then read the row id chunk and uncompress it
    if (hasEncoding(dimensionColumnChunk.encoders, Encoding.INVERTED_INDEX)) {
      byte[] compressedIndexPage = new byte[dimensionColumnChunk.rowid_page_length];
      System.arraycopy(data, dimensionColumnChunk.data_page_length, compressedIndexPage, 0,
          dimensionColumnChunk.rowid_page_length);
      invertedIndexes = CarbonUtil
          .getUnCompressColumnIndex(dimensionColumnChunk.rowid_page_length, compressedIndexPage,
              numberComressor);
      // get the reverse index
      invertedIndexesReverse = getInvertedReverseIndex(invertedIndexes);
    }
    // if rle is applied then read the rle block chunk and then uncompress
    //then actual data based on rle block
    if (hasEncoding(dimensionColumnChunk.encoders, Encoding.RLE)) {
      // read and uncompress the rle block
      byte[] compressedRLEPage = new byte[dimensionColumnChunk.rle_page_length];
      System.arraycopy(data, compressedDataPage.length + dimensionColumnChunk.rowid_page_length,
          compressedRLEPage, 0, dimensionColumnChunk.rle_page_length);
      rlePage = numberComressor.unCompress(compressedRLEPage);
      // uncompress the data with rle indexes
      dataPage = UnBlockIndexer.uncompressData(dataPage, rlePage, eachColumnValueSize[blockIndex]);
      rlePage = null;
    }
    // fill chunk attributes
    DimensionChunkAttributes chunkAttributes = new DimensionChunkAttributes();
    chunkAttributes.setEachRowSize(eachColumnValueSize[blockIndex]);
    chunkAttributes.setInvertedIndexes(invertedIndexes);
    chunkAttributes.setInvertedIndexesReverse(invertedIndexesReverse);
    DimensionColumnDataChunk columnDataChunk = null;

    if (dimensionColumnChunk.isRowMajor()) {
      // to store fixed length column chunk values
      columnDataChunk = new ColumnGroupDimensionDataChunk(dataPage, chunkAttributes);
    }
    // if no dictionary column then first create a no dictionary column chunk
    // and set to data chunk instance
    else if (!hasEncoding(dimensionColumnChunk.encoders, Encoding.DICTIONARY)) {
      columnDataChunk =
          new VariableLengthDimensionDataChunk(getNoDictionaryDataChunk(dataPage), chunkAttributes);
      chunkAttributes.setNoDictionary(true);
    } else {
      // to store fixed length column chunk values
      columnDataChunk = new FixedLengthDimensionDataChunk(dataPage, chunkAttributes);
    }
    return columnDataChunk;
  }

  private int calculateTotalDataLength(DataChunk dimensionColumnChunk) {
    int totalDataLength = 0;
    if ((hasEncoding(dimensionColumnChunk.encoders, Encoding.RLE) && hasEncoding(
        dimensionColumnChunk.encoders, Encoding.INVERTED_INDEX)) || hasEncoding(
        dimensionColumnChunk.encoders, Encoding.RLE)) {
      totalDataLength = dimensionColumnChunk.data_page_length + dimensionColumnChunk.rle_page_length
          + dimensionColumnChunk.rowid_page_length;
    } else if (hasEncoding(dimensionColumnChunk.encoders, Encoding.INVERTED_INDEX)) {
      totalDataLength =
          dimensionColumnChunk.data_page_length + dimensionColumnChunk.rowid_page_length;
    } else {
      totalDataLength = dimensionColumnChunk.data_page_length;
    }
    return totalDataLength;
  }

}
