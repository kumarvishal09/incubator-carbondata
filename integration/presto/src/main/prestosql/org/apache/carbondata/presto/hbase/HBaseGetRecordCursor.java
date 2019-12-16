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
package org.apache.carbondata.presto.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.carbondata.presto.hbase.metadata.HbaseColumn;
import org.apache.carbondata.presto.hbase.serializers.HBaseRowSerializer;
import org.apache.carbondata.presto.hbase.split.HBaseSplit;

import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.prestosql.plugin.hive.HiveColumnHandle;
import io.prestosql.spi.predicate.Range;
import io.prestosql.spi.type.Type;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * HBaseGetRecordCursor
 *
 * @since 2020-03-18
 */
public class HBaseGetRecordCursor
        extends HBaseRecordCursor
{
    private static final Logger LOG = Logger.get(HBaseGetRecordCursor.class);

    private Connection conn;

    private Result[] results;

    private int currentRecordIndex;

    public HBaseGetRecordCursor(
            List<HiveColumnHandle> columnHandles,
            HBaseSplit hBaseSplit,
            Connection connection,
            HBaseRowSerializer serializer,
            List<Type> columnTypes,
            HbaseColumn[] rowIdName,
            String[] fieldToColumnName,
            String defaultValue)
    {
        super(columnHandles, columnTypes, serializer, rowIdName, fieldToColumnName, defaultValue, hBaseSplit);
        startTime = System.currentTimeMillis();
        this.columnHandles = columnHandles;

        this.rowIdName = rowIdName;

        this.split = hBaseSplit;
        this.conn = connection;
        try (Table table =
                connection.getTable(TableName.valueOf(split.getTable().getNamespace(), split.getTable().getName()))) {
            Map<String, List<Object>> rowKeys = new LinkedHashMap<>();
            Type[] types = new Type[hBaseSplit.getTable().getRow().getHbaseColumns().length];
            int sizeOfRows = 0;
            if (Arrays.stream(hBaseSplit.getTable().getRow().getHbaseColumns()).allMatch(col -> hBaseSplit.getRanges().containsKey(col.getColNameWithoutCf()))) {
                int i = 0;
                for (HbaseColumn hbaseColumn : hBaseSplit.getTable().getRow().getHbaseColumns()) {
                    List<Object> vals =
                        rowKeys.computeIfAbsent(hbaseColumn.getColName(), k -> new ArrayList<>());
                    Type type = null;
                    for (Range range : hBaseSplit.getRanges().get(hbaseColumn.getColNameWithoutCf())) {
                        type = range.getType();
                        Object object = range.getSingleValue();
                        if (object instanceof Slice) {
                            vals.add( ((Slice) object).toStringUtf8());
                        }
                        else {
                            vals.add(range.getSingleValue());
                        }
                    }
                    sizeOfRows = vals.size();
                    types[i++] = type;
                }
            }

            this.results = getResults(convertToRows(rowKeys, sizeOfRows), table, types);
        }
        catch (IOException e) {
            LOG.error(e, e.getMessage());
            this.close();
        }
        this.bytesRead = 0L;
    }

    private List<Object[]> convertToRows(Map<String, List<Object>> rowKeys, int size) {
        List<Object[]>  list = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            Object[] row = new Object[rowKeys.size()];
            int k = 0;
            for (Map.Entry<String, List<Object>> entry : rowKeys.entrySet()) {
                row[k++] = entry.getValue().get(i);
            }
            list.add(row);
        }
        return list;
    }

    private Result[] getResults(List<Object[]> rowKeys, Table table, Type[] types) {

        List<Get> gets =
                rowKeys.stream()
                        .map(
                                rowKey -> {

                                    Get get = new Get(serializer.encodeCompositeRowKey(rowKey, types));
                                    for (HiveColumnHandle hch : columnHandles) {
//                                            if (!Utils.contains(Utils.getHbaseColumn(split.getTable(), hch).getColName(), rowIdName)) {
                                                get.addColumn(
                                                        Bytes.toBytes(Utils.getHbaseColumn(split.getTable(), hch).getCf()),
                                                        Bytes.toBytes(hch.getName()));
//                                            }
                                    }
                                    try {
                                        if (split.getTimestamp() > 0) {
                                            get.setTimeRange(split.getTimestamp(), Long.MAX_VALUE);
                                        }
                                    } catch (IOException e) {
                                        throw new RuntimeException(e);
                                    }
                                    return get;
                                })
                        .collect(Collectors.toList());

        try {
            return table.get(gets);
        }
        catch (IOException e) {
            LOG.error(e.getMessage(), e);
            return new Result[0];
        }
    }



    @Override
    public boolean advanceNextPosition()
    {
        if (this.currentRecordIndex >= this.results.length) {
            return false;
        }
        else {
            Result record = this.results[this.currentRecordIndex];
            serializer.reset();
            if (record.getRow() != null) {
                serializer.deserialize(record, this.defaultValue, split.getTable());
            }
            this.currentRecordIndex++;
            return true;
        }
    }

    @Override
    public void close() {}
}
