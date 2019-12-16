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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.presto.hbase.Utils;
import org.apache.carbondata.presto.hbase.metadata.HbaseCarbonTable;
import org.apache.carbondata.presto.hbase.metadata.HbaseColumn;

import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.prestosql.plugin.hive.HiveColumnHandle;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarcharType;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TimeType.TIME;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * PrimitiveRowSerializer
 */
public class PrimitiveRowSerializer
        implements HBaseRowSerializer
{
    private static final Logger LOG = Logger.get(PrimitiveRowSerializer.class);

    private final Map<String, Map<String, String>> familyQualifierColumnMap = new HashMap<>();

    private final Map<String, byte[]> columnValues = new HashMap<>();

    private HbaseColumn[] rowIdName;

    private List<HiveColumnHandle> columnHandles;

    /**
     * set columnHandle
     *
     * @param columnHandleList columnHandleList
     */
    public void setColumnHandleList(List<HiveColumnHandle> columnHandleList)
    {
        this.columnHandles = columnHandleList;
    }

    @Override
    public void setRowIdName(HbaseColumn[] name, Type[] types)
    {
        this.rowIdName = name;
    }

    @Override
    public void setMapping(String name, String family, String qualifier)
    {
        columnValues.put(name, null);
        Map<String, String> qualifierColumnMap = familyQualifierColumnMap.get(family);
        if (qualifierColumnMap == null) {
            qualifierColumnMap = new HashMap<>();
            familyQualifierColumnMap.put(family, qualifierColumnMap);
        }

        qualifierColumnMap.put(qualifier, name);
    }

    @Override
    public void reset()
    {
        columnValues.clear();
    }

    public Map<String, byte[]> getColumnValues()
    {
        return columnValues;
    }

    /**
     * deserialize
     *
     * @param result Entry to deserialize
     * @param defaultValue defaultValue
     */
    public void deserialize(Result result, String defaultValue, HbaseCarbonTable table)
    {
//        if (!columnValues.containsKey(rowIdName)) {
//            columnValues.put(rowIdName[0], result.getRow());
//        }

        String family;
        String qualifer;
        String value = null;
        byte[] bytes;
        for (HiveColumnHandle hc : columnHandles) {
            HbaseColumn hbaseColumn = Utils.getHbaseColumn(table, hc);
//            if (!hbaseColumn.getColName().equals(rowIdName)) {
                family = hbaseColumn.getCf();
                qualifer = hc.getName();
                bytes = result.getValue(family.getBytes(UTF_8), qualifer.getBytes(UTF_8));
                columnValues.put(familyQualifierColumnMap.get(family).get(qualifer), bytes);
//            }
        }
    }

    /**
     * set Object Bytes
     *
     * @param type Type
     * @param value Object
     * @return get byte[] for HBase add column.
     */
    @Override
    public byte[] setObjectBytes(Type type, Object value)
    {
        if (type.equals(BIGINT) && value instanceof Integer) {
            return Bytes.toBytes(((Integer) value).longValue());
        }
        else if (type.equals(BIGINT) && value instanceof Long) {
            return Bytes.toBytes((Long) value);
        }
        else if (type.equals(BOOLEAN)) {
            return Bytes.toBytes((Boolean) (value.equals(Boolean.TRUE)));
        }
        else if (type.equals(DATE)) {
            return Bytes.toBytes((Long) value);
        }
        else if (type.equals(DOUBLE)) {
            return Bytes.toBytes((Double) value);
        }
        else if (type.equals(INTEGER) && value instanceof Integer) {
            return Bytes.toBytes((Integer) value);
        }
        else if (type.equals(INTEGER) && value instanceof Long) {
            return Bytes.toBytes(((Long) value).intValue());
        }
        else if (type.equals(SMALLINT)) {
            return Bytes.toBytes((Short) value);
        }
        else if (type.equals(TIME)) {
            return Bytes.toBytes((Long) value);
        }
        else if (type.equals(TINYINT)) {
            return Bytes.toBytes((byte) value);
        }
        else if (type.equals(TIMESTAMP)) {
            return Bytes.toBytes((Long) value);
        }
        else if (type instanceof VarcharType && value instanceof String) {
            return ((String) value).getBytes(UTF_8);
        }
        else if (type instanceof VarcharType && value instanceof Slice) {
            return ((Slice) value).toStringUtf8().getBytes(UTF_8);
        }
        else {
            LOG.error("getBytes: Unsupported type %s", type);
            throw new UnsupportedOperationException("Unsupported type " + type);
        }
    }

    /**
     * get Bytes Object
     *
     * @param type Type
     * @param columnName String
     * @param <T> Type
     * @return read from HBase, set into output
     */
    public <T> T getBytesObject(Type type, String columnName)
    {
        byte[] fieldValue = getFieldValue(columnName);

        if (type.equals(BIGINT)) {
            return (T) (Long) Bytes.toLong(fieldValue);
        }
        else if (type.equals(BOOLEAN)) {
            return (T) (Boolean) (fieldValue[0] != 0);
        }
        else if (type.equals(DATE)) {
            return (T) (Long) Bytes.toLong(fieldValue);
        }
        else if (type.equals(DOUBLE)) {
            return (T) (Double) Bytes.toDouble(fieldValue);
        }
        else if (type.equals(INTEGER)) {
            return (T) (Long)((Integer)Bytes.toInt(fieldValue)).longValue();
        }
        else if (type.equals(SMALLINT)) {
            return (T) (Long)((Short) Bytes.toShort(fieldValue)).longValue();
        }
        else if (type.equals(TIME)) {
            return (T) (Long) Bytes.toLong(fieldValue);
        }
        else if (type.equals(TIMESTAMP)) {
            return (T) (Long) Bytes.toLong(fieldValue);
        }
        else if (type.equals(TINYINT)) {
            return (T) (Long)((Byte) fieldValue[0]).longValue();
        }
        else if (type instanceof VarcharType) {
            return (T) Slices.utf8Slice(new String(fieldValue));
        }
        else {
            LOG.error("decode: StringRowSerializer does not support decoding type %s", type);
            throw new PrestoException(NOT_SUPPORTED, "StringRowSerializer does not support decoding type " + type);
        }
    }

    @Override
    public boolean isNull(String name)
    {
        return columnValues.get(name) == null || columnValues.get(name).equals("NULL");
    }

    @Override
    public Block getMap(String name, Type type)
    {
        return HBaseRowSerializer.getBlockFromMap(type, getBytesObject(type, name));
    }

    @Override public byte[] encodeCompositeRowKey(Object[] row, Type[] types) {
        throw new UnsupportedOperationException("Not supported");
    }

    private byte[] getFieldValue(String name)
    {
        return columnValues.get(name);
    }

    @Override public Object[] decodeCompositeRowKey(byte[] row, Type[] types) {
        throw new UnsupportedOperationException("not supported");
    }
}
