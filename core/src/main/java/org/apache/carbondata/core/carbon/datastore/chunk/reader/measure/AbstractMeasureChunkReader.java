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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.BitSet;
import java.util.List;

import org.apache.carbondata.core.carbon.datastore.chunk.reader.MeasureColumnChunkReader;
import org.apache.carbondata.core.carbon.metadata.blocklet.datachunk.PresenceMeta;
import org.apache.carbondata.core.datastorage.store.compression.SnappyCompression.SnappyByteCompression;
import org.apache.carbondata.core.metadata.ValueEncoderMeta;
import org.apache.carbondata.core.util.CarbonUtil;

/**
 * Measure block reader abstract class
 */
public abstract class AbstractMeasureChunkReader implements MeasureColumnChunkReader {

  /**
   * file path from which blocks will be read
   */
  protected String filePath;

  /**
   * measure chunk have the information about the metadata present in the file
   */
  protected List<Long> measureColumnChunkOffsets;

  /**
   * Constructor to get minimum parameter to create instance of this class
   *
   * @param measureColumnChunk measure chunk metadata
   * @param compression        model metadata which was to used to compress and uncompress
   *                           the measure value
   * @param filePath           file from which data will be read
   * @param isInMemory         in case of in memory it will read and holds the data and when
   *                           query request will come it will uncompress and the data
   */
  public AbstractMeasureChunkReader(List<Long> measureColumnChunkOffsets, String filePath,
      boolean isInMemory) {
    this.measureColumnChunkOffsets = measureColumnChunkOffsets;
    this.filePath = filePath;
  }

  /**
   * Below method will be used to convert the thrift presence meta to wrapper
   * presence meta
   *
   * @param presentMetadataThrift
   * @return wrapper presence meta
   */
  protected PresenceMeta getPresenceMeta(
      org.apache.carbondata.format.PresenceMeta presentMetadataThrift) {
    PresenceMeta presenceMeta = new PresenceMeta();
    presenceMeta.setRepresentNullValues(presentMetadataThrift.isRepresents_presence());
    presenceMeta.setBitSet(BitSet.valueOf(
        SnappyByteCompression.INSTANCE.unCompress(presentMetadataThrift.getPresent_bit_stream())));
    return presenceMeta;
  }

  /**
   * Below method will be used to convert the encode metadata to
   * ValueEncoderMeta object
   *
   * @param encoderMeta
   * @return ValueEncoderMeta object
   */
  protected ValueEncoderMeta deserializeEncoderMeta(byte[] encoderMeta) {
    // TODO : should remove the unnecessary fields.
    ByteArrayInputStream aos = null;
    ObjectInputStream objStream = null;
    ValueEncoderMeta meta = null;
    try {
      aos = new ByteArrayInputStream(encoderMeta);
      objStream = new ObjectInputStream(aos);
      meta = (ValueEncoderMeta) objStream.readObject();
    } catch (ClassNotFoundException e) {
      CarbonUtil.closeStreams(objStream);
    } catch (IOException e) {
      CarbonUtil.closeStreams(objStream);
    }
    return meta;
  }

}
