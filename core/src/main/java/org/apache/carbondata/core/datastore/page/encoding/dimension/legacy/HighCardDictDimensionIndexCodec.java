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

import java.util.Map;

import org.apache.carbondata.core.datastore.columnar.BinaryPageIndexGenerator;
import org.apache.carbondata.core.datastore.columnar.PageIndexGenerator;
import org.apache.carbondata.core.datastore.compression.Compressor;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageEncoder;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.format.Encoding;

public class HighCardDictDimensionIndexCodec extends IndexStorageCodec {

  public HighCardDictDimensionIndexCodec(boolean isSort, boolean isInvertedIndex,
      Compressor compressor) {
    super(isSort, isInvertedIndex, compressor);
  }

  @Override
  public String getName() {
    return "HighCardDictDimensionIndexCodec";
  }

  @Override public ColumnPageEncoder createEncoder(Map<String, Object> parameter) {
    return new IndexStorageEncoder(true, null, Encoding.DIRECT_STRING) {

      @Override
      protected void encodeIndexStorage(ColumnPage input) {
        PageIndexGenerator<byte[][]> pageIndexGenerator;
        byte[][] data = input.getByteArrayPage();
        int[] intPage = input.getRowOffsetPage().getIntPage();
        int[] length = new int[intPage.length -1];
        for (int i = 0; i < intPage.length -1 ; i++) {
          length[i] =  intPage[i + 1] - intPage[i];
        }
        pageIndexGenerator =
              new BinaryPageIndexGenerator(data, isSort, length);
        byte[] flattened = ByteUtil.flatten(pageIndexGenerator.getDataPage());
        super.compressedDataPage = compressor.compressByte(flattened);
        super.pageIndexGenerator = pageIndexGenerator;
      }
    };
  }
}
