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

package org.apache.carbondata.core.datastore.chunk.store.impl.safe;

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;

public class NonDictionaryVectorFillerFactory {
  public static AbstractNonDictionaryVectorFiller getVectorFiller(int length, DataType type,
      int numberOfRows, int actualDataLength, boolean isAdaptive, boolean isFixed,
      DataType lengthStoredType, int eachDataLen) {
    if (type == DataTypes.STRING) {
      if (isAdaptive) {
        return new BinaryVectorFiller(numberOfRows, lengthStoredType, actualDataLength);
      } else if (isFixed) {
        return new FixedBinaryVectorFillter(numberOfRows, eachDataLen, actualDataLength);
      } else if (length > DataTypes.SHORT.getSizeInBytes()) {
        return new LongStringVectorFiller(numberOfRows, actualDataLength);
      } else {
        return new StringVectorFiller(numberOfRows, actualDataLength);
      }
    } else if (type == DataTypes.VARCHAR || type == DataTypes.BINARY) {
      return new LongStringVectorFiller(numberOfRows, actualDataLength);
    } else if (type == DataTypes.TIMESTAMP) {
      return new TimeStampVectorFiller(numberOfRows);
    } else if (type == DataTypes.BOOLEAN) {
      return new BooleanVectorFiller(numberOfRows);
    } else if (type == DataTypes.SHORT) {
      return new ShortVectorFiller(numberOfRows);
    } else if (type == DataTypes.INT) {
      return new IntVectorFiller(numberOfRows);
    } else if (type == DataTypes.LONG) {
      return new LongVectorFiller(numberOfRows);
    } else {
      throw new UnsupportedOperationException("Not supported datatype : " + type);
    }
  }
}
