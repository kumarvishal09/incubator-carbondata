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

package org.apache.carbondata.presto;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.hadoop.api.CarbonInputFormat;
import org.apache.carbondata.presto.hbase.HBaseConnection;
import org.apache.carbondata.presto.hbase.HBaseRecordSet;
import org.apache.carbondata.presto.hbase.split.HBaseSplit;
import org.apache.carbondata.presto.impl.CarbonTableCacheModel;
import org.apache.carbondata.presto.impl.CarbonTableReader;

import static org.apache.carbondata.presto.Types.checkType;

import com.google.inject.Inject;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HiveColumnHandle;
import io.prestosql.plugin.hive.HiveConfig;
import io.prestosql.plugin.hive.HivePageSourceFactory;
import io.prestosql.plugin.hive.HivePageSourceProvider;
import io.prestosql.plugin.hive.HiveRecordCursorProvider;
import io.prestosql.plugin.hive.HiveSplit;
import io.prestosql.plugin.hive.HiveTableHandle;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.RecordPageSource;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.type.TypeManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Provider Class for Carbondata Page Source class.
 */
public class CarbondataPageSourceProvider extends HivePageSourceProvider {

  private CarbonTableReader carbonTableReader;
  private String queryId;
  private HdfsEnvironment hdfsEnvironment;
  private HBaseConnection hBaseConnection;

  @Inject public CarbondataPageSourceProvider(
      HiveConfig hiveConfig,
      HdfsEnvironment hdfsEnvironment,
      Set<HiveRecordCursorProvider> cursorProviders,
      Set<HivePageSourceFactory> pageSourceFactories,
      TypeManager typeManager,
      CarbonTableReader carbonTableReader,
      HBaseConnection hBaseConnection) {
    super(hiveConfig, hdfsEnvironment, cursorProviders, pageSourceFactories, typeManager);
    this.carbonTableReader = requireNonNull(carbonTableReader, "carbonTableReader is null");
    this.hdfsEnvironment = hdfsEnvironment;
    this.hBaseConnection = requireNonNull(hBaseConnection, "hBaseConnection is null");
  }

  @Override
  public ConnectorPageSource createPageSource(ConnectorTransactionHandle transactionHandle,
      ConnectorSession session, ConnectorSplit split, ConnectorTableHandle table,
      List<ColumnHandle> columns) {
    HiveSplit carbonSplit = checkType(split, HiveSplit.class, "split is not class HiveSplit");
    this.queryId = carbonSplit.getSchema().getProperty("queryId");
    if (this.queryId == null) {
      // Fall back to hive pagesource.
      return super.createPageSource(transactionHandle, session, split, table, columns);
    }
    Configuration configuration = this.hdfsEnvironment.getConfiguration(
        new HdfsEnvironment.HdfsContext(session, carbonSplit.getDatabase(), carbonSplit.getTable()),
        new Path(carbonSplit.getSchema().getProperty("tablePath")));
    configuration = carbonTableReader.updateS3Properties(configuration);
    CarbonTable carbonTable = getCarbonTable(carbonSplit, configuration);
    if (carbonSplit.getSchema().getProperty("carbonSplit") != null) {
      boolean isDirectVectorFill =
          carbonTableReader.config.getPushRowFilter() == null || carbonTableReader.config.getPushRowFilter().equalsIgnoreCase("false");
      return new CarbondataPageSource(carbonTable, queryId, carbonSplit, columns, table,
          configuration, isDirectVectorFill);
    } else {
      List<HiveColumnHandle> columnHandles =
          columns.stream().map(HiveColumnHandle.class::cast).collect(Collectors.toList());
      HBaseSplit hBaseSplit =
          HBaseSplit.convertSplit(carbonSplit.getSchema().getProperty("hbaseSplit"));
      HBaseRecordSet recordSet = new HBaseRecordSet(hBaseConnection, session, hBaseSplit, columnHandles.stream().filter(f -> !f.getName().equalsIgnoreCase("rowtimestamp")).collect(
          Collectors.toList()));
      return new RecordPageSource(recordSet);
    }

  }

  /**
   * @param carbonSplit
   * @return
   */
  private CarbonTable getCarbonTable(HiveSplit carbonSplit, Configuration configuration) {
    try {
      configuration.set(CarbonInputFormat.TABLE_INFO,
          carbonSplit.getSchema().getProperty(CarbonInputFormat.TABLE_INFO));
      TableInfo tableInfo = CarbonInputFormat.getTableInfo(configuration);
      if (tableInfo != null) {
        return CarbonTable.buildFromTableInfo(tableInfo);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    CarbonTableCacheModel tableCacheModel = carbonTableReader
        .getCarbonCache(new SchemaTableName(carbonSplit.getDatabase(), carbonSplit.getTable()),
            carbonSplit.getSchema().getProperty("tablePath"), configuration, null);
    checkNotNull(tableCacheModel, "tableCacheModel should not be null");
    checkNotNull(tableCacheModel.getCarbonTable(),
        "tableCacheModel.carbonTable should not be null");
    checkNotNull(tableCacheModel.getCarbonTable().getTableInfo(),
        "tableCacheModel.carbonTable.tableInfo should not be null");
    return tableCacheModel.getCarbonTable();
  }

}
