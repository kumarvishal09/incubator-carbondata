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
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarcharType;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PDatum;
import org.apache.phoenix.schema.RowKeySchema;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.types.*;

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
 * PhoenixRowSerializer
 */
public class PhoenixRowSerializer
        implements HBaseRowSerializer
{
    private static final Logger LOG = Logger.get(PhoenixRowSerializer.class);

    private final Map<String, Map<String, String>> familyQualifierColumnMap = new HashMap<>();

    private final Map<String, byte[]> columnValues = new HashMap<>();

    private HbaseColumn[] rowIdName;

    private Type[] rowTypes;

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
        this.rowTypes = types;
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
//        if (rowIdName.length < 2) {
//            if (!columnValues.containsKey(rowIdName[0])) {
//                columnValues.put(rowIdName[0], result.getRow());
//            }
//        } else {
//            for (int i = 0; i < rowIdName.length; i++) {
//                if (!columnValues.containsKey(rowIdName[i])) {
//                    columnValues.put(rowIdName[i], result.getRow());
//                }
//            }
//        }

        String family;
        String qualifer;
        byte[] bytes;
        for (HiveColumnHandle hc : columnHandles) {
            HbaseColumn hbaseColumn = Utils.getHbaseColumn(table, hc);
//            if (!Utils.contains(hbaseColumn.getColName(), rowIdName)) {
                family = hbaseColumn.getCf();
                qualifer = hc.getName();
                bytes = result.getValue(family.getBytes(UTF_8), qualifer.getBytes(UTF_8));
                columnValues.put(familyQualifierColumnMap.get(family).get(qualifer), bytes);
//            }
        }
    }


    int getIndex(String colName,  String[] vals) {
        for (int i = 0; i < vals.length; i++) {
            if (vals[i].equals(colName)) {
                return i;
            }
        }
        return -1;
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
            return PLong.INSTANCE.toBytes(((Integer) value).longValue());
        }
        else if (type.equals(BIGINT) && value instanceof Long) {
            return PLong.INSTANCE.toBytes(value);
        }
        else if (type.equals(BOOLEAN)) {
            return PBoolean.INSTANCE.toBytes(value.equals(Boolean.TRUE));
        }
        else if (type.equals(DATE)) {
            return PLong.INSTANCE.toBytes(value);
        }
        else if (type.equals(DOUBLE)) {
            return PDouble.INSTANCE.toBytes(value);
        }
        else if (type.equals(INTEGER) && value instanceof Integer) {
            return PInteger.INSTANCE.toBytes(value);
        }
        else if (type.equals(INTEGER) && value instanceof Long) {
            return PInteger.INSTANCE.toBytes(((Long) value).intValue());
        }
        else if (type.equals(SMALLINT)) {
            return PSmallint.INSTANCE.toBytes(value);
        }
        else if (type.equals(TIME)) {
            return PLong.INSTANCE.toBytes(value);
        }
        else if (type.equals(TINYINT)) {
            return PTinyint.INSTANCE.toBytes(value);
        }
        else if (type.equals(TIMESTAMP)) {
            return PLong.INSTANCE.toBytes(value);
        }
        else if (type instanceof VarcharType && value instanceof String) {
            return PVarchar.INSTANCE.toBytes(value);
        }
        else if (type instanceof VarcharType && value instanceof Slice) {
            return PVarchar.INSTANCE.toBytes(((Slice) value).toStringUtf8());
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
//        int index = getIndex(columnName, rowIdName);
//        if (rowIdName.length > 1 && index > -1) {
//            Object[] objects = decodeCompositeRowKey(fieldValue, rowTypes);
//            if (rowTypes[index].equals(INTEGER)) {
//                return (T) (Long)((Integer)objects[index]).longValue();
//            } else if (rowTypes[index].equals(SMALLINT)) {
//                return (T) (Long)((Short)objects[index]).longValue();
//            }  else if (rowTypes[index].equals(TINYINT)) {
//                return (T) (Long)((Byte)objects[index]).longValue();
//            } else if (rowTypes[index] instanceof VarcharType) {
//                return (T) Slices.utf8Slice(objects[index].toString());
//            } else {
//                return (T) objects[index];
//            }
//        }

        if (type.equals(BIGINT)) {
            return (T) PLong.INSTANCE.toObject(fieldValue);
        }
        else if (type.equals(BOOLEAN)) {
            return (T) PBoolean.INSTANCE.toObject (fieldValue);
        }
        else if (type.equals(DATE)) {
            return (T) PLong.INSTANCE.toObject(fieldValue);
        }
        else if (type.equals(DOUBLE)) {
            return (T) PDouble.INSTANCE.toObject(fieldValue);
        }
        else if (type.equals(INTEGER)) {
            return (T) (Long)((Integer)PInteger.INSTANCE.toObject(fieldValue)).longValue();
        }
        else if (type.equals(SMALLINT)) {
            return (T) (Long)((Short) PSmallint.INSTANCE.toObject(fieldValue)).longValue();
        }
        else if (type.equals(TIME)) {
            return (T) PLong.INSTANCE.toObject(fieldValue);
        }
        else if (type.equals(TIMESTAMP)) {
            return (T) PLong.INSTANCE.toObject(fieldValue);
        }
        else if (type.equals(TINYINT)) {
            return (T) (Long)((Byte) PTinyint.INSTANCE.toObject(fieldValue)).longValue();
        }
        else if (type instanceof VarcharType) {
            return (T) Slices.utf8Slice(PVarchar.INSTANCE.toObject(fieldValue).toString());
        }
        else {
            LOG.error("decode: StringRowSerializer does not support decoding type %s", type);
            throw new PrestoException(NOT_SUPPORTED, "StringRowSerializer does not support decoding type " + type);
        }
    }

    public byte[] encodeCompositeRowKey(Object[] row, Type[] types) {
        byte[][] bytes = new byte[row.length][];
        for (int i = 0; i < row.length; i++) {
            bytes[i] = setObjectBytes(types[i], row[i]);
            if (types[i] instanceof VarcharType && i < row.length - 1) {
                byte[] temp =
                    new byte[bytes[i].length + QueryConstants.SEPARATOR_BYTE_ARRAY.length];
                System.arraycopy(bytes[i], 0, temp, 0, bytes[i].length);
                System.arraycopy(QueryConstants.SEPARATOR_BYTE_ARRAY, 0, temp, bytes[i].length,
                    QueryConstants.SEPARATOR_BYTE_ARRAY.length);
                bytes[i] = temp;
            }
        }
        int size = 0;
        for (int i = 0; i < bytes.length; i++) {
            size += bytes[i].length;
        }
        byte[] rowKey = new byte[size];
        int runningLen = 0;
        for (int i = 0; i < bytes.length; i++) {
            System.arraycopy(bytes[i], 0, rowKey, runningLen, bytes[i].length);
            runningLen += bytes[i].length;
        }
        return rowKey;
    }

    public Object[] decodeCompositeRowKey(byte[] row, Type[] types) {
        RowKeySchema schema = buildSchema(types);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        int maxOffest = schema.iterator(row, 0, row.length, ptr);
        Object[] ret = new Object[types.length];
        for (int i = 0; i < schema.getFieldCount(); i++) {
            if (schema.next(ptr, i, maxOffest) != null) {
                Object value = convertToTypeInstance(types[i])
                    .toObject(ptr, schema.getField(i).getDataType(), SortOrder.getDefault());
                ret[i] = value;
            }
        }
        return ret;
    }

    private RowKeySchema buildSchema(Type[] types) {
        RowKeySchema.RowKeySchemaBuilder builder = new RowKeySchema.RowKeySchemaBuilder(types.length);
        for (Type type : types) {
            builder.addField(buildPDatum(type), false, SortOrder.getDefault());
        }

        return builder.build();
    }

    private PDatum buildPDatum(Type type) {
        return new PDatum() {
            @Override public boolean isNullable() {
                return false;
            }

            @Override public PDataType getDataType() {
                return convertToTypeInstance(type);
            }

            @Override public Integer getMaxLength() {
                return null;
            }

            @Override public Integer getScale() {
                return null;
            }

            @Override public SortOrder getSortOrder() {
                return null;
            }
        };
    }

    private PDataType convertToTypeInstance(Type type) {
        if (type.equals(BOOLEAN)) {
            return PBoolean.INSTANCE;
        } else if (type.equals(DATE)) {
            return PLong.INSTANCE;
        } else if (type.equals(TINYINT)) {
            return PTinyint.INSTANCE;
        } else if (type.equals(DOUBLE)) {
            return PDouble.INSTANCE;
        } else if (type.equals(INTEGER)) {
            return PInteger.INSTANCE;
        } else if (type.equals(BIGINT)) {
            return PLong.INSTANCE;
        } else if (type.equals(SMALLINT)) {
            return PSmallint.INSTANCE;
        } else if (type instanceof VarcharType) {
            return PVarchar.INSTANCE;
        } else if (type instanceof DecimalType) {
            return PDecimal.INSTANCE;
        } else if (type.equals(TIMESTAMP)) {
            return PLong.INSTANCE;
        } else {
            throw new UnsupportedOperationException("unsupported data type " + type);
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

    private byte[] getFieldValue(String name)
    {
        return columnValues.get(name);
    }
}
