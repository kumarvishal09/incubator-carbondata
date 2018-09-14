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

package org.apache.carbondata.core.datastore.page.encoding.dimension.legacy;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.TableSpec;
import org.apache.carbondata.core.datastore.columnar.PageIndexGenerator;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageEncoder;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageEncoderMeta;
import org.apache.carbondata.core.datastore.page.encoding.DefaultEncodingFactory;
import org.apache.carbondata.core.datastore.page.encoding.EncodedColumnPage;
import org.apache.carbondata.core.datastore.page.statistics.PrimitivePageStatsCollector;
import org.apache.carbondata.core.keygenerator.KeyGenException;
import org.apache.carbondata.core.keygenerator.KeyGenerator;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.format.BlockletMinMaxIndex;
import org.apache.carbondata.format.DataChunk2;
import org.apache.carbondata.format.Encoding;
import org.apache.carbondata.format.SortState;

public abstract class IndexStorageEncoder extends ColumnPageEncoder {
  PageIndexGenerator pageIndexGenerator;
  byte[] compressedDataPage;
  protected EncodedColumnPage encodedColumnPage;
  private boolean storeOffset;
  private KeyGenerator keyGenerator;

  IndexStorageEncoder(boolean storeOffset, KeyGenerator keyGenerator) {
    this.storeOffset = storeOffset;
    this.keyGenerator = keyGenerator;
  }

  abstract void encodeIndexStorage(ColumnPage inputPage);

  @Override
  protected byte[] encodeData(ColumnPage input) throws MemoryException, IOException {
    encodeIndexStorage(input);
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(stream);
    if(this.storeOffset) {
      ColumnPage lengthPage = getLengthPage(pageIndexGenerator.getLength(),
          (TableSpec.MeasureSpec) input.getRowOffsetPage().getColumnSpec());
      ColumnPageEncoder encoder = DefaultEncodingFactory.getInstance()
          .createEncoder(lengthPage.getColumnSpec(), lengthPage, null);
      this.encodedColumnPage = encoder.encode(lengthPage);
      out.writeInt(this.encodedColumnPage.getEncodedData().array().length);
      out.write(this.encodedColumnPage.getEncodedData().array());
      lengthPage.freeMemory();
    }
    out.write(compressedDataPage);
    if (pageIndexGenerator.getRowIdPageLengthInBytes() > 0) {
      out.writeInt(pageIndexGenerator.getRowIdPageLengthInBytes());
      short[] rowIdPage = pageIndexGenerator.getRowIdPage();
      for (short rowId : rowIdPage) {
        out.writeShort(rowId);
      }
      if (pageIndexGenerator.getRowIdRlePageLengthInBytes() > 0) {
        short[] rowIdRlePage = pageIndexGenerator.getRowIdRlePage();
        for (short rowIdRle : rowIdRlePage) {
          out.writeShort(rowIdRle);
        }
      }
    }
    if (pageIndexGenerator.getDataRlePageLengthInBytes() > 0) {
      short[] dataRlePage = pageIndexGenerator.getDataRlePage();
      for (short dataRle : dataRlePage) {
        out.writeShort(dataRle);
      }
    }
    return stream.toByteArray();
  }

  @Override
  protected ColumnPageEncoderMeta getEncoderMeta(ColumnPage inputPage) {
    return null;
  }

  @Override
  protected List<ByteBuffer> buildEncoderMeta(ColumnPage inputPage) throws IOException {
    return encodedColumnPage.getPageMetadata().encoder_meta;
  }

  @Override
  protected void fillLegacyFields(DataChunk2 dataChunk)
      throws IOException {
    SortState sort = (pageIndexGenerator.getRowIdPageLengthInBytes() > 0) ?
        SortState.SORT_EXPLICIT : SortState.SORT_NATIVE;
    dataChunk.setSort_state(sort);
    if (pageIndexGenerator.getRowIdPageLengthInBytes() > 0) {
      int rowIdPageLength = CarbonCommonConstants.INT_SIZE_IN_BYTE +
          pageIndexGenerator.getRowIdPageLengthInBytes() +
          pageIndexGenerator.getRowIdRlePageLengthInBytes();
      dataChunk.setRowid_page_length(rowIdPageLength);
    }
    if (pageIndexGenerator.getDataRlePageLengthInBytes() > 0) {
      dataChunk.setRle_page_length(pageIndexGenerator.getDataRlePageLengthInBytes());
    }
    dataChunk.setData_page_length(compressedDataPage.length);
  }
  @Override
  protected List<Encoding> getEncodingList() {
    List<Encoding> encodings = new ArrayList<>();
    if (pageIndexGenerator.getRowIdPageLengthInBytes() > 0) {
      encodings.add(Encoding.INVERTED_INDEX);
    }
    if(storeOffset) {
      encodings.add(Encoding.DIRECT_COMPRESS);
    }
    encodings.addAll(encodedColumnPage.getPageMetadata().getEncoders());
    return encodings;
  }
  
  private ColumnPage getLengthPage(int[] length, TableSpec.MeasureSpec columnSpec) throws MemoryException {
    ColumnPage lengthPage =
        ColumnPage.newPage(columnSpec, DataTypes.INT, length.length);
    lengthPage
        .setStatsCollector(PrimitivePageStatsCollector.newInstance(lengthPage.getDataType()));
    for (int i = 0; i < length.length ; i++) {
      lengthPage.putData(i, length[i]);
    }
    return lengthPage;
  }

  protected BlockletMinMaxIndex buildMinMaxIndex(ColumnPage inputPage) {
    if (null != keyGenerator) {
      BlockletMinMaxIndex index = new BlockletMinMaxIndex();
      try {
        int min = (int) (inputPage.getStatistics().getMin());
        int max = (int) (inputPage.getStatistics().getMax());
        index.addToMax_values(ByteBuffer.wrap(keyGenerator.generateKey(new int[] { max })));
        index.addToMin_values(ByteBuffer.wrap(keyGenerator.generateKey(new int[] { min })));
      } catch (KeyGenException e) {
        throw new RuntimeException(e);
      }
      return index;
    } else {
      return super.buildMinMaxIndex(inputPage);
    }
  }
}