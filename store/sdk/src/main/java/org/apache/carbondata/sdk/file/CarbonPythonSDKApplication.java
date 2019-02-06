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
package org.apache.carbondata.sdk.file;

import java.io.IOException;
import java.util.UUID;

import org.apache.carbondata.sdk.file.arrow.ArrowConverter;

import py4j.GatewayServer;

public class CarbonPythonSDKApplication {

  private Schema carbonSchema;

  public byte[] readSchema(String folderPath) throws IOException {
    if (null == carbonSchema) {
      this.carbonSchema = readCarbonSchema(folderPath, false);
    }
    ArrowConverter arrowConverter = new ArrowConverter(carbonSchema, 1);
    final byte[] bytes = arrowConverter.toSerializeArray();
    arrowConverter.close();
    return bytes;
  }

  private Schema readCarbonSchema(String folderPath, boolean validateSchema) throws IOException {
    if (null == carbonSchema) {
      this.carbonSchema = CarbonSchemaReader.readSchema(folderPath, validateSchema);
    }
    return carbonSchema;
  }

  public String[] getSplits(String folderPath) throws IOException {
    final CarbonReaderBuilder temp = CarbonReader.builder(folderPath, "_temp");
    return temp.getSplits();
  }

  public byte[] readArrowBatch(String filePath, String columnNamesString)
      throws Exception {
    CarbonReader reader = null;
    try {
      UUID uuid = UUID.randomUUID();
      final String[] columnNames = columnNamesString.split(",");
      reader =
          CarbonReader.builder(filePath, "_temp" + uuid).withFile(filePath).projection(columnNames)
              .build();
      return reader.readArrowBatch(readCarbonSchema(filePath, false));
    } finally {
      if (null != reader) {
        reader.close();
      }
    }
  }



  public static void main(String[] args) throws Exception {
    //TODO start jvm server in carbon.py using jvm
    CarbonPythonSDKApplication carbonPythonSDKApplication = new CarbonPythonSDKApplication();
    GatewayServer gatewayServer = new GatewayServer(carbonPythonSDKApplication);
    gatewayServer.start();
  }
}
