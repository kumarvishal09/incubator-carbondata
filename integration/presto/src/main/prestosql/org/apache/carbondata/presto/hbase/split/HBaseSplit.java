/*
 * Copyright (C) 2018-2020. Huawei Technologies Co., Ltd. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.carbondata.presto.hbase.split;

import java.io.IOException;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.carbondata.presto.hbase.metadata.HbaseCarbonTable;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.prestosql.spi.HostAddress;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.predicate.Marker;
import io.prestosql.spi.predicate.Range;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.BooleanType;
import io.prestosql.spi.type.DateType;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.DoubleType;
import io.prestosql.spi.type.IntegerType;
import io.prestosql.spi.type.TimestampType;
import io.prestosql.spi.type.TinyintType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarcharType;
import org.apache.hadoop.hbase.HRegionInfo;

/**
 * HBaseSplit
 *
 * @since 2020-03-30
 */
public class HBaseSplit
{
    private final List<HostAddress> addresses;

    private final String startRow;

    private final String endRow;

    private final Map<String, List<Range>> ranges;

    private final HRegionInfo regionInfo;

    private final HbaseCarbonTable table;

    private final boolean isBatchGet;

    private final long timestamp;

    /**
     * constructor
     *
     * @param addresses addresses
     * @param startRow startRow
     * @param endRow endRow
     * @param ranges search ranges
     * @param regionInfo regionInfo
     */
    @JsonCreator
    public HBaseSplit(
            @JsonProperty("addresses") List<HostAddress> addresses,
            @JsonProperty("startRow") String startRow,
            @JsonProperty("endRow") String endRow,
            @JsonProperty("ranges") Map<String, List<Range>> ranges,
            @JsonProperty("regionInfo") HRegionInfo regionInfo,
            @JsonProperty("hbaseTable") HbaseCarbonTable table,
            @JsonProperty("isBatchGet") boolean isBatchGet,
            @JsonProperty("timestamp") long timestamp)
    {
        this.addresses = addresses;
        this.startRow = startRow;
        this.endRow = endRow;
        this.ranges = ranges;
        this.regionInfo = regionInfo;
        this.table = table;
        this.isBatchGet = isBatchGet;
        this.timestamp = timestamp;
    }

    public List<HostAddress> getAddresses()
    {
        return addresses;
    }


    @JsonProperty
    public String getStartRow()
    {
        return startRow;
    }

    @JsonProperty
    public String getEndRow()
    {
        return endRow;
    }

    @JsonProperty
    public Map<String, List<Range>> getRanges()
    {
        return ranges;
    }

    @JsonProperty
    public HRegionInfo getRegionInfo()
    {
        return regionInfo;
    }

    @JsonProperty
    public HbaseCarbonTable getTable() {
        return table;
    }

    @JsonProperty
    public long getTimestamp() {
        return timestamp;
    }

    public static String getJsonString(HBaseSplit split) throws Exception {
        GsonBuilder mapper = new GsonBuilder();
//        SimpleModule simpleModule = new SimpleModule();
//        simpleModule.addSerializer(Marker.class, new JsonSerializer<Marker>() {
//            @Override public void serialize(Marker marker, JsonGenerator jsonGenerator,
//                SerializerProvider serializerProvider) throws IOException, JsonProcessingException {
//                SliceOutput output = new DynamicSliceOutput(64);
//                TestingBlockEncodingSerde blockEncodingSerde = new TestingBlockEncodingSerde();
//                blockEncodingSerde.writeBlock(output, marker.getValueBlock().get());
//                String encoded = Base64.getEncoder().encodeToString(output.slice().getBytes());
//                jsonGenerator.writeString(marker.getType().getDisplayName() + "@"+ encoded + "@"+marker.getBound().toString() );
//            }
//        });
        mapper.registerTypeAdapter(Marker.class, new com.google.gson.JsonSerializer<Marker>() {
            @Override public JsonElement serialize(Marker marker, java.lang.reflect.Type type,
                JsonSerializationContext jsonSerializationContext) {
                SliceOutput output = new DynamicSliceOutput(64);
                TestingBlockEncodingSerde blockEncodingSerde = new TestingBlockEncodingSerde();
                blockEncodingSerde.writeBlock(output, marker.getValueBlock().isPresent()?marker.getValueBlock().get():null);
                String encoded = Base64.getEncoder().encodeToString(output.slice().getBytes());
                JsonObject jsonMerchant = new JsonObject();

                jsonMerchant.addProperty("Id", marker.getType().getDisplayName() + "@"+ encoded + "@"+marker.getBound().toString());

                return jsonMerchant;
            }
        });
//        mapper.registerModule(simpleModule);
//        return mapper.writeValueAsString(split);
        return mapper.create().toJson(split);
    }

    public static HBaseSplit convertSplit(String multiSplitJson) {
//        ObjectMapper mapper = new ObjectMapper();
//        mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        GsonBuilder mapper = new GsonBuilder();
//        SimpleModule module = new SimpleModule();
//        module.addDeserializer(Marker.class, new JsonDeserializer<Marker>() {
//            @Override public Marker deserialize(JsonParser jsonParser,
//                DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
//                TestingBlockEncodingSerde blockEncodingSerde = new TestingBlockEncodingSerde();
//                String[] str = jsonParser.readValueAs(String.class).split("@");
//                byte[] decoded = Base64.getDecoder().decode(str[1]);
//                BasicSliceInput input = Slices.wrappedBuffer(decoded).getInput();
//                Block readBlock = blockEncodingSerde.readBlock(input);
//                return new Marker(getType(str[0]), Optional.of(readBlock), Marker.Bound.valueOf(str[2]));
//            }
//        });
        mapper.registerTypeAdapter(Marker.class, new com.google.gson.JsonDeserializer<Marker>() {
            @Override
            public Marker deserialize(JsonElement jsonElement, java.lang.reflect.Type type,
                JsonDeserializationContext jsonDeserializationContext) throws JsonParseException {
                TestingBlockEncodingSerde blockEncodingSerde = new TestingBlockEncodingSerde();
                String[] str = jsonElement.getAsJsonObject().get("Id").getAsString().split("@");
                byte[] decoded = Base64.getDecoder().decode(str[1]);
                BasicSliceInput input = Slices.wrappedBuffer(decoded).getInput();
                Block readBlock = blockEncodingSerde.readBlock(input);
                return new Marker(getType(str[0]), readBlock==null?Optional.empty() : Optional.of(readBlock), Marker.Bound.valueOf(str[2]));
            }
        });
//        mapper.registerModule(module);
        try {
//            return mapper.readValue(multiSplitJson, HBaseSplit.class);
            return mapper.create().fromJson(multiSplitJson, HBaseSplit.class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static Type getType(String ids) {
        if (ids.equalsIgnoreCase(IntegerType.INTEGER.getDisplayName())) {
            return IntegerType.INTEGER;
        } else if (ids.equalsIgnoreCase(VarcharType.VARCHAR.getDisplayName())) {
            return VarcharType.VARCHAR;
        } else if (ids.equalsIgnoreCase(TinyintType.TINYINT.getDisplayName())) {
            return TinyintType.TINYINT;
        } else if (ids.equalsIgnoreCase(DateType.DATE.getDisplayName())) {
            return DateType.DATE;
        } else if (ids.contains("decimal")) {
            if (ids.indexOf("(") > 0) {
                return DecimalType.createDecimalType(Integer.parseInt(ids.substring(ids.indexOf("(") + 1, ids.indexOf(","))),
                    Integer.parseInt(ids.substring(ids.indexOf(".")+1, ids.indexOf(")"))));
            } else {
                return DecimalType.createDecimalType();
            }
        } else if (ids.equalsIgnoreCase(BigintType.BIGINT.getDisplayName())) {
            return BigintType.BIGINT;
        } else if (ids.equalsIgnoreCase(TimestampType.TIMESTAMP.getDisplayName())) {
            return TimestampType.TIMESTAMP;
        } else if (ids.equalsIgnoreCase(DoubleType.DOUBLE.getDisplayName())) {
            return DoubleType.DOUBLE;
        } else if (ids.equalsIgnoreCase(BooleanType.BOOLEAN.getDisplayName())) {
            return BooleanType.BOOLEAN;
        }
        return null;
    }

    @JsonProperty
    public boolean isBatchGet() {
        return isBatchGet;
    }
}
