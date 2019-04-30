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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.datastore.columnar.BinaryPageIndexGenerator;
import org.apache.carbondata.core.datastore.columnar.PageIndexGenerator;
import org.apache.carbondata.core.datastore.compression.Compressor;
import org.apache.carbondata.core.datastore.compression.CompressorFactory;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageEncoder;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.format.Encoding;

public class BinaryDimColumnCodec extends IndexStorageCodec {

  private final List<Encoding> encodingList;

  public BinaryDimColumnCodec(boolean isSort) {
    super(isSort);
    encodingList = new ArrayList<>();
    encodingList.add(Encoding.DIRECT_STRING);
  }

  @Override
  public String getName() {
    return "BinaryDimColumnCodec";
  }

  @Override public ColumnPageEncoder createEncoder(Map<String, Object> parameter) {
    return new IndexStorageEncoder(true, null, encodingList) {
      private final int THREE_BYTES_MAX = (int) Math.pow(2, 23) - 1;
      private final int THREE_BYTES_MIN = - THREE_BYTES_MAX - 1;
      @Override
      protected void encodeIndexStorage(ColumnPage input) throws MemoryException, IOException {
        PageIndexGenerator<byte[][]> pageIndexGenerator;
        // get actual data
        byte[][] data = input.getByteArrayPage();
        // fill length array
        int[] lengthArray = new int[data.length];
        int max = Integer.MIN_VALUE;
        int min = Integer.MAX_VALUE;
        int currentDataLength = 0;
        int size = 0;
        boolean isEqualLengthData = true;
        int prvDataLen = data[0].length;
        for (int i = 0; i < lengthArray.length; i++) {
          currentDataLength = data[i].length;
          if (prvDataLen != currentDataLength) {
            isEqualLengthData = false;
          }
          lengthArray[i] = currentDataLength;
          size += currentDataLength;
          if (max < currentDataLength) {
            max = currentDataLength;
          }
          if (min > currentDataLength) {
            min = currentDataLength;
          }
        }
        if (isEqualLengthData) {
          this.isEqualLength = true;
          this.eachDataLength = currentDataLength;
        }
        Compressor compressor = CompressorFactory.getInstance().getCompressor(
            input.getColumnCompressorName());
        pageIndexGenerator =
            new BinaryPageIndexGenerator(data, isSort, lengthArray);
        byte[] flattened = null;
        byte[][] dataPage = pageIndexGenerator.getDataPage();
        if (!isEqualLengthData) {
          // free memory
          selectedDataType = fitLongMinMax(max, min);
          ByteBuffer byteBuffer;
          if (DataTypes.BYTE == selectedDataType) {
            byteBuffer = ByteBuffer.allocate(lengthArray.length + size);
            for (int i = 0; i < lengthArray.length; i++) {
              byteBuffer.put((byte) lengthArray[i]);
              byteBuffer.put(dataPage[i]);
            }
          } else if (DataTypes.SHORT == selectedDataType) {
            byteBuffer = ByteBuffer.allocate((lengthArray.length * 2) + size);
            for (int i = 0; i < lengthArray.length; i++) {
              byteBuffer.putShort((short) lengthArray[i]);
              byteBuffer.put(dataPage[i]);
            }
          } else if (DataTypes.SHORT_INT == selectedDataType) {
            byteBuffer = ByteBuffer.allocate((lengthArray.length * 3) + size);
            for (int i = 0; i < lengthArray.length; i++) {
              byteBuffer.put(ByteUtil.to3Bytes(lengthArray[i]));
              byteBuffer.put(dataPage[i]);
            }
          } else {
            byteBuffer = ByteBuffer.allocate((lengthArray.length * 4) + size);
            for (int i = 0; i < lengthArray.length; i++) {
              byteBuffer.putInt(lengthArray[i]);
              byteBuffer.put(dataPage[i]);
            }
          }
          byteBuffer.rewind();
          flattened = byteBuffer.array();
        } else {
          flattened = ByteUtil.flatten(dataPage);
        }
        super.compressedDataPage = compressor.compressByte(flattened);
        super.pageIndexGenerator = pageIndexGenerator;
      }

      private DataType fitLongMinMax(int max, int min) {
        if (max <= Byte.MAX_VALUE && min >= Byte.MIN_VALUE) {
          return DataTypes.BYTE;
        } else if (max <= Short.MAX_VALUE && min >= Short.MIN_VALUE) {
          return DataTypes.SHORT;
        } else if (max <= THREE_BYTES_MAX && min >= THREE_BYTES_MIN) {
          return DataTypes.SHORT_INT;
        } else {
          return DataTypes.INT;
        }
      }
    };
  }
}
