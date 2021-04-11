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
package org.apache.carbondata.presto.hbase.serializers;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.presto.hbase.metadata.HbaseCarbonTable;
import org.apache.carbondata.presto.hbase.metadata.HbaseColumn;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.prestosql.plugin.hive.HiveColumnHandle;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeUtils;
import io.prestosql.spi.type.VarcharType;
import org.apache.hadoop.hbase.client.Result;

import static io.prestosql.spi.StandardErrorCode.NOT_FOUND;

/**
 * HBaseRowSerializer
 *
 * @since 2020-03-30
 */
public interface HBaseRowSerializer
{
    /**
     * Gets the default HBaseRowSerializer,
     *
     * @return Default serializer
     */
    static HBaseRowSerializer getDefault()
    {
        return new PrimitiveRowSerializer();
    }

    /**
     * Sets the Hetu name which maps to the HBase row ID.
     *
     * @param name Hetu column name
     */
    void setRowIdName(HbaseColumn[] name, Type[] types);

    /**
     * Sets the mapping for the Hetu column name to HBase family and qualifier.
     *
     * @param name Hetu name
     * @param family HBase family
     * @param qualifier HBase qualifier
     */
    void setMapping(String name, String family, String qualifier);

    /**
     * setColumnHandleList
     *
     * @param list list
     */
    void setColumnHandleList(List<HiveColumnHandle> list);

    /**
     * Reset the state of the serializer to prepare for a new set of entries with the same row ID.
     */
    void reset();

    /**
     * setObjectBytes
     *
     * @param type Type
     * @param value Object
     * @return byte[]
     */
    byte[] setObjectBytes(Type type, Object value);

    /**
     * getBytesObject
     *
     * @param type Type
     * @param columnName String
     * @param <T> Type
     * @return Type
     */
    <T> T getBytesObject(Type type, String columnName);

    /**
     * Deserialize Result into puts
     *
     * @param result deserialize Result to puts
     * @param defaultValue default value ,if the cell's value is null, set the default value.
     */
    void deserialize(Result result, String defaultValue, HbaseCarbonTable table);

    /**
     * Gets a Boolean value indicating whether or not the Hetu column is a null value.
     *
     * @param name Column name
     * @return True if null, false otherwise.
     */
    boolean isNull(String name);


    /**
     * return Map type block.
     *
     * @param name Column name
     * @param type Map type
     * @return Map value
     */
    Block getMap(String name, Type type);

    /**
     * return List type block
     *
     * @param elementType Array element type
     * @param block Array block
     * @return List of values
     */
    static List<Object> getArrayFromBlock(Type elementType, Block block)
    {
        List<Object> list = new ArrayList<>();
        for (int i = 0; i < block.getPositionCount(); ++i) {
            list.add(readObject(elementType, block, i));
        }
        return ImmutableList.copyOf(list.iterator());
    }

    public byte[] encodeCompositeRowKey(Object[] row, Type[] types);

    public Object[] decodeCompositeRowKey(byte[] row, Type[] types);

    /**
     * Encodes the given list into a Block.
     *
     * @param elementType Element type of the array
     * @param array Array of elements to encode
     * @return Hetu Block
     */
    static Block getBlockFromArray(Type elementType, List<?> array)
    {
        BlockBuilder builder = elementType.createBlockBuilder(null, array.size());
        for (Object item : array) {
            writeObject(builder, elementType, item);
        }
        return builder.build();
    }

    /**
     * Encodes the given map into a Block.
     *
     * @param mapType Hetu type of the map
     * @param map Map of key/value pairs to encode
     * @return Hetu Block
     */
    static Block getBlockFromMap(Type mapType, Map<?, ?> map)
    {
        Type keyType = mapType.getTypeParameters().get(0);
        Type valueType = mapType.getTypeParameters().get(1);

        BlockBuilder mapBlockBuilder = mapType.createBlockBuilder(null, 1);
        BlockBuilder builder = mapBlockBuilder.beginBlockEntry();

        for (Map.Entry<?, ?> entry : map.entrySet()) {
            writeObject(builder, keyType, entry.getKey());
            writeObject(builder, valueType, entry.getValue());
        }

        mapBlockBuilder.closeEntry();
        return (Block) mapType.getObject(mapBlockBuilder, 0);
    }

    /**
     * writeObject
     *
     * @param builder Block builder
     * @param type Hetu type
     * @param obj Object to write to the block builder
     */
    static void writeObject(BlockBuilder builder, Type type, Object obj)
    {
        if (type.getTypeSignature().getBase().equals(StandardTypes.ARRAY)) {
            BlockBuilder arrayBuilder = builder.beginBlockEntry();
            Type elementType = type.getTypeParameters().get(0);
            for (Object item : (List<?>) obj) {
                writeObject(arrayBuilder, elementType, item);
            }
            builder.closeEntry();
        }
        else if (type.getTypeSignature().getBase().equals(StandardTypes.MAP)) {
            BlockBuilder mapBlockBuilder = builder.beginBlockEntry();
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) obj).entrySet()) {
                writeObject(mapBlockBuilder, type.getTypeParameters().get(0), entry.getKey());
                writeObject(mapBlockBuilder, type.getTypeParameters().get(1), entry.getValue());
            }
            builder.closeEntry();
        }
        else {
            TypeUtils.writeNativeValue(type, builder, obj);
        }
    }

    /**
     * readObject
     *
     * @param type Hetu type
     * @param block Block to decode
     * @param position Position in the block to get
     * @return Java object from the Block
     */
    static Object readObject(Type type, Block block, int position)
    {
        if (type.getJavaType() == Slice.class) {
            Object object = TypeUtils.readNativeValue(type, block, position);
            if (object instanceof Slice) {
                Slice slice = (Slice) object;
                return type.equals(VarcharType.VARCHAR) ? slice.toStringUtf8() : slice.getBytes();
            }
        }

        return TypeUtils.readNativeValue(type, block, position);
    }

    /**
     * getSerializerInstance
     *
     * @param serializerClassName serializerClassName
     * @return HBaseRowSerializer
     */
    @JsonIgnore
    static HBaseRowSerializer getSerializerInstance(String serializerClassName)
    {
        try {
            return (HBaseRowSerializer) Class.forName(serializerClassName).getConstructor().newInstance();
        }
        catch (ClassNotFoundException
                | InstantiationException
                | IllegalAccessException
                | InvocationTargetException
                | NoSuchMethodException e) {
            throw new PrestoException(NOT_FOUND, "Configured serializer class not found", e);
        }
    }
}
