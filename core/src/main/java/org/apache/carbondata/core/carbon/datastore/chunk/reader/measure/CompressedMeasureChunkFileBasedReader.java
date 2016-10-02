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
package org.apache.carbondata.core.carbon.datastore.chunk.reader.measure;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.core.carbon.datastore.chunk.MeasureColumnDataChunk;
import org.apache.carbondata.core.datastorage.store.FileHolder;
import org.apache.carbondata.core.datastorage.store.compression.ValueCompressionModel;
import org.apache.carbondata.core.datastorage.store.compression.ValueCompressonHolder.UnCompressValue;
import org.apache.carbondata.core.datastorage.store.dataholder.CarbonReadDataHolder;
import org.apache.carbondata.core.metadata.ValueEncoderMeta;
import org.apache.carbondata.core.reader.CarbonDataChunkReader;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.format.DataChunk;

/**
 * Compressed measure chunk reader
 */
public class CompressedMeasureChunkFileBasedReader extends AbstractMeasureChunkReader {

  /**
   * Constructor to get minimum parameter to create instance of this class
   *
   * @param measureColumnChunk measure chunk metadata
   * @param compression        model metadata which was to used to compress and uncompress
   *                           the measure value
   * @param filePath           file from which data will be read
   */
  public CompressedMeasureChunkFileBasedReader(List<Long> measureColumnChunkOffsets,
      String filePath) {
    super(measureColumnChunkOffsets, filePath, false);
  }

  /**
   * Method to read the blocks data based on block indexes
   *
   * @param fileReader   file reader to read the blocks
   * @param blockIndexes blocks to be read
   * @return measure data chunks
   */
  @Override public MeasureColumnDataChunk[] readMeasureChunks(FileHolder fileReader,
      int... blockIndexes) {
    MeasureColumnDataChunk[] datChunk =
        new MeasureColumnDataChunk[measureColumnChunkOffsets.size()];
    for (int i = 0; i < blockIndexes.length; i++) {
      datChunk[blockIndexes[i]] = readMeasureChunk(fileReader, blockIndexes[i]);
    }
    return datChunk;
  }

  /**
   * Method to read the blocks data based on block index
   *
   * @param fileReader file reader to read the blocks
   * @param blockIndex block to be read
   * @return measure data chunk
   */
  @Override public MeasureColumnDataChunk readMeasureChunk(FileHolder fileReader, int blockIndex) {
    MeasureColumnDataChunk datChunk = new MeasureColumnDataChunk();
    DataChunk measureColumnChunk = null;
    try {
      measureColumnChunk =
          CarbonDataChunkReader.readFooter(filePath, measureColumnChunkOffsets.get(blockIndex));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    List<ValueEncoderMeta> valueEncodeMeta = new ArrayList<>();
    for (int i = 0; i < measureColumnChunk.getEncoder_meta().size(); i++) {
      valueEncodeMeta
          .add(deserializeEncoderMeta(measureColumnChunk.getEncoder_meta().get(i).array()));
    }
    ValueCompressionModel compressionModel = CarbonUtil.getValueCompressionModel(valueEncodeMeta);
    UnCompressValue values =
        compressionModel.getUnCompressValues()[0].getNew().getCompressorObject();
    // create a new uncompressor
    // read data from file and set to uncompressor
    values.setValue(fileReader.readByteArray(filePath, measureColumnChunk.data_page_offset,
        measureColumnChunk.data_page_length));
    // get the data holder after uncompressing
    CarbonReadDataHolder measureDataHolder =
        values.uncompress(compressionModel.getChangedDataType()[0])
            .getValues(compressionModel.getDecimal()[0], compressionModel.getMaxValue()[0]);
    // set the data chunk
    datChunk.setMeasureDataHolder(measureDataHolder);
    // set the enun value indexes
    datChunk.setNullValueIndexHolder(getPresenceMeta(measureColumnChunk.presence));
    return datChunk;
  }

}
