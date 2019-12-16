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

package org.apache.carbondata.core.extrenalschema;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.carbondata.core.metadata.schema.table.Writable;

public class ExternalSchema implements Writable {
  private String querySchema;
  private String handOffSchema;
  private Map<String, String> extraParams;

  public ExternalSchema() {

  }

  public ExternalSchema(String querySchema, String handOffSchema, Map<String, String> extraParams) {
    this.querySchema = querySchema;
    this.handOffSchema = handOffSchema;
    this.extraParams = extraParams;
  }

  public String getQuerySchema() {
    return querySchema;
  }

  public String getHandOffSchema() {
    return handOffSchema;
  }

  public Map<String, String> getExtraParams() {
    return extraParams;
  }

  public String getParam(String key) {
    return extraParams.get(key);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(querySchema);
    out.writeUTF(handOffSchema);
    out.writeInt(Objects.nonNull(extraParams) ? extraParams.size() : 0);
    if (Objects.nonNull(extraParams)) {
      extraParams.forEach((k, v) -> {
        try {
          out.writeUTF(k);
          out.writeUTF(v);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      });
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.querySchema = in.readUTF();
    this.handOffSchema = in.readUTF();
    this.extraParams = new HashMap<>();
    int extrasParamSize = in.readInt();
    for (int i = 0; i < extrasParamSize; i++) {
      this.extraParams.put(in.readUTF(), in.readUTF());
    }
  }

}
