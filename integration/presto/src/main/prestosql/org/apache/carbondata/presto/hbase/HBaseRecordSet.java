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
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.presto.hbase.metadata.HbaseColumn;
import org.apache.carbondata.presto.hbase.serializers.HBaseRowSerializer;
import org.apache.carbondata.presto.hbase.serializers.PhoenixRowSerializer;
import org.apache.carbondata.presto.hbase.split.HBaseSplit;

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.prestosql.plugin.hive.HiveColumnHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.RecordCursor;
import io.prestosql.spi.connector.RecordSet;
import io.prestosql.spi.predicate.Marker;
import io.prestosql.spi.predicate.Range;
import io.prestosql.spi.type.Decimals;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarcharType;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static java.util.Objects.requireNonNull;

/**
 * HBaseRecordSet
 *
 * @since 2020-03-18
 */
public class HBaseRecordSet
        implements RecordSet
{
    private static final Logger LOG = Logger.get(HBaseRecordSet.class);

    private List<HiveColumnHandle> columnHandles;

    private List<Type> columnTypes;

    private HBaseRowSerializer serializer;

    private HbaseColumn[] rowIdName;

    private ResultScanner scanner;

    private String[] fieldToColumnName;

    private HBaseSplit split;

    private HBaseConnection conn;

    private Scan scan;

    private String defaultValue;

    /**
     * constructor
     *
     * @param hbaseConn hbaseConn
     * @param session session
     * @param split split
     * @param columnHandles columnHandles
     */
    public HBaseRecordSet(
            HBaseConnection hbaseConn,
            ConnectorSession session,
            HBaseSplit split,
            List<HiveColumnHandle> columnHandles)
    {
        requireNonNull(session, "session is null");

        this.split = split;
        this.conn = hbaseConn;
        this.serializer = HBaseRowSerializer.getSerializerInstance(PhoenixRowSerializer.class.getName());
        this.columnHandles = columnHandles;
        rowIdName = split.getTable().getRow().getHbaseColumns();
        Type[] rowTypes = new Type[rowIdName.length];
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (HiveColumnHandle column : columnHandles) {
            types.add(column.getType());
            for (int i = 0; i < rowIdName.length; i++) {
                if (Utils.contains(column.getName(), rowIdName)) {
                    rowTypes[i] = column.getType();
                }
            }
        }
        this.serializer.setRowIdName(rowIdName, rowTypes);
        this.columnTypes = types.build();
        this.defaultValue = null;

        scan = new Scan();
        fieldToColumnName = new String[columnHandles.size()];
        for (int i = 0; i < this.columnHandles.size(); i++) {
            HiveColumnHandle hc = columnHandles.get(i);
            fieldToColumnName[i] = hc.getName();
            HbaseColumn hbaseColumn = Utils.getHbaseColumn(split.getTable(), hc);
//            if (!Utils.contains(hbaseColumn.getColName(), rowIdName)) {
                String cf = hbaseColumn.getCf();
                scan.addColumn(
                        cf.getBytes(Charset.forName("UTF-8")),
                    hbaseColumn.getCol().getBytes(Charset.forName("UTF-8")));
                this.serializer.setMapping(hc.getName(), cf, hbaseColumn.getCol());
//            }
        }
    }


    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        if (this.split.isBatchGet()) {
            return new HBaseGetRecordCursor(
                    columnHandles,
                    split,
                    conn.getConn(),
                    serializer,
                    columnTypes,
                    rowIdName,
                    fieldToColumnName,
                    this.defaultValue);
        }
        else {
            try {
                columnHandles.stream()
                        .forEach(
                                columnHandle -> {
                                    HiveColumnHandle hBaseColumnHandle = columnHandle;
                                    HbaseColumn hbaseColumn =
                                        Utils.getHbaseColumn(split.getTable(), hBaseColumnHandle);
                                    String cf = hbaseColumn.getCf();
//                                    if (this.rowIdName == null
//                                            || !Utils.contains(hbaseColumn.getColName(), this.rowIdName)) {
                                        scan.addColumn(
                                                Bytes.toBytes(cf),
                                                Bytes.toBytes(hBaseColumnHandle.getName()));
//                                    }
                                });
                Map<String, List<Range>> domainMap = this.split.getRanges();
                FilterList filters = getFiltersFromDomains(domainMap);
                if (split.getTimestamp() > 0) {
                    scan = scan.setTimeRange(split.getTimestamp(), Long.MAX_VALUE);
                }
                if (filters.getFilters().size() != 0) {
                    scan.setFilter(filters);
                }

                if (split.getStartRow() != null && !split.getStartRow().isEmpty()) {
                    scan.setStartRow(Bytes.toBytes(split.getStartRow()));
                }

                if (split.getEndRow() != null && !split.getEndRow().isEmpty()) {
                    scan.setStopRow(Bytes.toBytes(split.getEndRow()));
                }
                TableName hbaseTableName = TableName.valueOf(split.getTable().getNamespace(),split.getTable().getName());
                scanner = conn.getConn().getTable(hbaseTableName).getScanner(scan);
            }
            catch (IOException e) {
                LOG.error("HBaseRecordSet : setScanner failed... cause by %s", e.getMessage());
                throw new RuntimeException(e);
            }
            return new HBaseRecordCursor(
                    columnHandles, columnTypes, serializer, scanner, fieldToColumnName, rowIdName, this.defaultValue, split);
        }
    }

    /**
     * getHBaseConnection
     *
     * @return HBaseConnection
     */
    public HBaseConnection getHBaseConnection()
    {
        return this.conn;
    }


    /**
     * transform domain to filters
     *
     * @param domainMap domains
     * @return filterList
     */
    public FilterList getFiltersFromDomains(Map<String, List<Range>> domainMap)
    {
        FilterList andFilters = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        // select count(rowKey) / rowKey from table_xxx;
        if (this.columnHandles.size() == 0) {
            scan.setCaching(Constants.SCAN_CACHING_SIZE);
            scan.setCacheBlocks(false);
            andFilters.addFilter(new FirstKeyOnlyFilter());
            andFilters.addFilter(new KeyOnlyFilter());
        }

        if (domainMap == null || domainMap.isEmpty()) {
            return andFilters;
        }

        domainMap.entrySet().stream()
                .forEach(
                        entry -> {
                            String columnId = entry.getKey();
                            List<Range> ranges = entry.getValue();
                            HiveColumnHandle columnHandle =
                                    columnHandles.stream()
                                            .filter(col -> checkPredicateType(col) && columnId.equalsIgnoreCase(col.getName()))
                                            .findAny()
                                            .orElse(null);
                            if (ranges == null || columnHandle == null) {
                                return;
                            }

                            // inFilters: put "="
                            List<Filter> inFilters = new ArrayList<>();
                            // filters: put "<" "<=" ">" ">="
                            List<Filter> filters = new ArrayList<>();
                            HbaseColumn hbaseColumn =
                                Utils.getHbaseColumn(split.getTable(), columnHandle);
                            getFiltersFromRanges(ranges, hbaseColumn, andFilters, inFilters, filters);

                            // operate is IN / =
                            if (inFilters.size() != 0) {
                                FilterList columnFilter = new FilterList(FilterList.Operator.MUST_PASS_ONE, inFilters);
                                andFilters.addFilter(columnFilter);
                            }
                            // NOT IN (3,4) become "id < 3" or "3 < id < 4" or "id > 4"
                            // != 3 become "id < 3" or "id > 3"
                            if (ranges.size() > 1 && filters.size() != 0) {
                                FilterList orFilter = new FilterList(FilterList.Operator.MUST_PASS_ONE, filters);
                                andFilters.addFilter(orFilter);
                            }
                            if (ranges.size() <= 1 && filters.size() != 0) {
                                FilterList andFilter = new FilterList(filters);
                                andFilters.addFilter(andFilter);
                            }
                            // operate IS NULL
                            if (ranges.isEmpty()) {
                                boolean isKey = false; //Utils.contains(columnHandle.getName(), this.rowIdName);
                                andFilters.addFilter(
                                        columnOrKeyFilter(hbaseColumn, null, CompareFilter.CompareOp.EQUAL, isKey, null));
                            }
                        });

        return andFilters;
    }

    private boolean checkPredicateType(HiveColumnHandle columnHandle)
    {
        Type type = columnHandle.getType();
        // type list support to pushdown
        if (type.equals(BOOLEAN)) {
            return true;
        } else if (type.equals(DOUBLE)) {
            return true;
        } else if (type.equals(BIGINT)) {
            return true;
        } else if (type.equals(INTEGER)) {
            return true;
        } else if (type.equals(SMALLINT)) {
            return true;
        } else if (type.equals(TINYINT)) {
            return true;
        } else if (type.equals(TIMESTAMP)) {
            return true;
        } else if (type.equals(DATE)) {
            // Fix me, currently shc stores as Long so we cannot push down the date filter
            return false;
        }
        else if (type instanceof VarcharType) {
            return true;
        } else if (Decimals.isShortDecimal(type)) {
            return true;
        }
        else {
            return false;
        }
    }

    private String getRangeValue(Object object)
    {
        String value;
        if (object instanceof Slice) {
            value = ((Slice) object).toStringUtf8();
        }
        else {
            value = object.toString();
        }

        return value;
    }

    private void getFiltersFromRanges(
            List<Range> ranges,
            HbaseColumn columnHandle,
            FilterList andFilters,
            List<Filter> inFilters,
            List<Filter> filters)
    {
        final int filterSize = 2;
        boolean isKey = false;//Utils.contains (columnHandle.getColName(), this.rowIdName);

        for (Range range : ranges) {
            if (range.isSingleValue()) {
                Object columnValue = range.getSingleValue();
                inFilters.add(columnOrKeyFilter(columnHandle, columnValue, CompareFilter.CompareOp.EQUAL, isKey, range.getType()));
            }
            else {
                Object columnValueLower = null;
                Object columnValueHigher = null;
                int count = 0;
                if (!range.getLow().isLowerUnbounded()) {
                    columnValueLower =
                            (range.getLow().getValueBlock().isPresent())
                                    ? range.getLow().getValue()
                                    : null;
                    addFilterByLowerBound(range.getLow().getBound(), columnValueLower, isKey, columnHandle, filters, range.getType());
                    count++;
                }
                if (!range.getHigh().isUpperUnbounded()) {
                    columnValueHigher =
                            (range.getHigh().getValueBlock().isPresent())
                                    ? range.getHigh().getValue()
                                    : null;
                    addFilterByHigherBound(range.getHigh().getBound(), columnValueHigher, isKey, columnHandle, filters, range.getType());
                    count++;
                }
                // is not null
                if (columnValueLower == null && columnValueHigher == null) {
                    andFilters.addFilter(
                            columnOrKeyFilter(columnHandle, null, CompareFilter.CompareOp.NOT_EQUAL, isKey, null));
                }
                if (count == filterSize) {
                    Filter left = filters.remove(filters.size() - 1);
                    Filter right = filters.remove(filters.size() - 1);
                    FilterList andFilter = new FilterList(FilterList.Operator.MUST_PASS_ALL);
                    andFilter.addFilter(left);
                    andFilter.addFilter(right);
                    filters.add(andFilter);
                }
            }
        }
    }

    /**
     * construct filter
     *
     * @param columnHandle columnHandle
     * @param columnValue columnValue
     * @param operator operator
     * @param isKey isKey
     * @return Filter
     */
    public Filter columnOrKeyFilter(
            HbaseColumn columnHandle, Object columnValue, CompareFilter.CompareOp operator, boolean isKey, Type type)
    {
        byte[] values = (columnValue != null) ? serializer.setObjectBytes(type, columnValue) : null;

        if (isKey) {
            return new RowFilter(operator, new BinaryComparator(values));
        }
        else {
            return new SingleColumnValueFilter(
                    Bytes.toBytes(columnHandle.getCf()),
                    Bytes.toBytes(columnHandle.getCol()),
                    operator,
                    values);
        }
    }

    /**
     * @param boundEnum ABOVE/EXACTLY
     * @param columnValueLower value
     * @param isKey is row key
     * @param columnHandle columnHandle
     * @param filters filters
     */
    public void addFilterByLowerBound(
            Marker.Bound boundEnum,
            Object columnValueLower,
            boolean isKey,
            HbaseColumn columnHandle,
            List<Filter> filters, Type type)
    {
        switch (boundEnum) {
            // >
            case ABOVE:
                filters.add(columnOrKeyFilter(columnHandle, columnValueLower, CompareFilter.CompareOp.GREATER, isKey, type));
                break;
            // >=
            case EXACTLY:
                filters.add(
                        columnOrKeyFilter(
                                columnHandle, columnValueLower, CompareFilter.CompareOp.GREATER_OR_EQUAL, isKey, type));
                break;
            default:
                throw new AssertionError("Unhandled bound: " + boundEnum);
        }
    }

    /**
     * @param boundEnum BELOW/EXACTLY
     * @param columnValueHigher value
     * @param isKey is row key
     * @param columnHandle columnHandle
     * @param filters filters
     */
    public void addFilterByHigherBound(
            Marker.Bound boundEnum,
            Object columnValueHigher,
            boolean isKey,
            HbaseColumn columnHandle,
            List<Filter> filters, Type type)
    {
        switch (boundEnum) {
            // <
            case BELOW:
                filters.add(columnOrKeyFilter(columnHandle, columnValueHigher, CompareFilter.CompareOp.LESS, isKey, type));
                break;
            // <=
            case EXACTLY:
                filters.add(
                        columnOrKeyFilter(
                                columnHandle, columnValueHigher, CompareFilter.CompareOp.LESS_OR_EQUAL, isKey, type));
                break;
            default:
                throw new AssertionError("Unhandled bound: " + boundEnum);
        }
    }
}
