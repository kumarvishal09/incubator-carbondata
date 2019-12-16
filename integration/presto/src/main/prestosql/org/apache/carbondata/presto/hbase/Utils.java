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

import java.io.File;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.carbondata.presto.hbase.metadata.HbaseCarbonTable;
import org.apache.carbondata.presto.hbase.metadata.HbaseColumn;

import io.airlift.log.Logger;
import io.prestosql.plugin.hive.HiveColumnHandle;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.Range;
import io.prestosql.spi.predicate.Ranges;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.Type;

/**
 * Utils
 *
 * @since 2020-03-18
 */
public class Utils
{
    private static final Logger LOG = Logger.get(Utils.class);

    private Utils() {}

    /**
     * Whether sql constraint contains conditions like "rowKey='xxx'" or "rowKey in ('xxx','xxx')"
     *
     * @param tupleDomain TupleDomain
     * @return true if this sql is batch get.
     */
    public static boolean isBatchGet(TupleDomain<HiveColumnHandle> tupleDomain, HbaseColumn[] rowIds)
    {
        if (tupleDomain != null && tupleDomain.getDomains().isPresent()) {
            Map<HiveColumnHandle, Domain> domains = tupleDomain.getDomains().get();
            HiveColumnHandle[] columnHandle =
                    domains.keySet().stream()
                            .filter((key -> Utils.contains(key.getName(), rowIds))).toArray(HiveColumnHandle[]::new);

            if (columnHandle.length == rowIds.length) {
                for (HiveColumnHandle handle : columnHandle) {
                    Ranges ranges = domains.get(handle).getValues().getRanges();
                    for (Range range : ranges.getOrderedRanges()) {
                        if (!range.isSingleValue()) {
                            return false;
                        }
                    }
                }
                return true;
            }
        }
        return false;
    }

    /**
     * createTypeByName
     *
     * @param type String
     * @return Type
     */
    public static Type createTypeByName(String type)
    {
        Type result = null;
        try {
            Class clazz = Class.forName(type);
            Field[] fields = clazz.getFields();

            for (Field field : fields) {
                if (type.equals(field.getType().getName())) {
                    Object object = field.get(clazz);
                    if (object instanceof Type) {
                        return (Type) object;
                    }
                }
            }
        }
        catch (ClassNotFoundException e) {
            LOG.error("createTypeByName failed... cause by : %s", e);
        }
        catch (IllegalAccessException e) {
            LOG.error("createTypeByName failed... cause by : %s", e);
        }

        return Optional.ofNullable(result).orElse(result);
    }

    public static HbaseColumn getHbaseColumn(HbaseCarbonTable table, HiveColumnHandle handle){
        return table.getsMap().values().stream().filter(f -> !f.getCf().equals("rowkey") && f.getColNameWithoutCf().equalsIgnoreCase(handle.getName())).findAny().get();
    }

    public static boolean containsAll(List<HiveColumnHandle> handles, HbaseColumn[] rowKeys){
        for (int i = 0; i < handles.size(); i++) {
            if(!handles.get(i).getName().equalsIgnoreCase(rowKeys[i].getColNameWithoutCf())) {
                return false;
            }
        }
        return true;
    }

    public static boolean contains(String colName,  HbaseColumn[] vals) {
        for (int i = 0; i < vals.length; i++) {
            if (vals[i].getColName().substring(vals[i].getCf().length()+1).equals(colName)) {
                return true;
            }
        }
        return false;
    }

    /**
     * check file exist
     *
     * @param path file path
     * @return true/false
     */
    public static boolean isFileExist(String path)
    {
        File file = new File(path);
        return file.exists();
    }
}
