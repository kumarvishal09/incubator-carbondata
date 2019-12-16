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

package org.apache.carbondata.presto.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.carbondata.common.exceptions.sql.InvalidLoadOptionException;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datamap.DataMapFilter;
import org.apache.carbondata.core.datamap.DataMapStoreManager;
import org.apache.carbondata.core.datamap.Segment;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.indexstore.PartitionSpec;
import org.apache.carbondata.core.indexstore.PrunedSegmentInfo;
import org.apache.carbondata.core.indexstore.SegmentPrunerFactory;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.CarbonMetadata;
import org.apache.carbondata.core.metadata.SegmentFileStore;
import org.apache.carbondata.core.metadata.converter.SchemaConverter;
import org.apache.carbondata.core.metadata.converter.ThriftWrapperSchemaConverterImpl;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.datatype.Field;
import org.apache.carbondata.core.metadata.schema.PartitionInfo;
import org.apache.carbondata.core.metadata.schema.partition.PartitionType;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.readcommitter.ReadCommittedScope;
import org.apache.carbondata.core.reader.ThriftReader;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.segmentmeta.SegmentColumnMetaDataInfo;
import org.apache.carbondata.core.segmentmeta.SegmentMetaDataInfo;
import org.apache.carbondata.core.statusmanager.FileFormat;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.hadoop.CarbonInputSplit;
import org.apache.carbondata.hadoop.api.CarbonInputFormat;
import org.apache.carbondata.hadoop.api.CarbonTableInputFormat;
import org.apache.carbondata.presto.PrestoFilterUtil;
import org.apache.carbondata.presto.hbase.Constants;
import org.apache.carbondata.presto.hbase.HBaseConnection;
import org.apache.carbondata.presto.hbase.Utils;
import org.apache.carbondata.presto.hbase.metadata.HbaseCarbonTable;
import org.apache.carbondata.presto.hbase.metadata.HbaseMetastoreUtil;
import org.apache.carbondata.presto.hbase.split.HBaseSplit;
import org.apache.carbondata.sdk.file.CarbonWriterBuilder;
import org.apache.carbondata.sdk.file.Schema;

import com.google.gson.Gson;
import com.google.inject.Inject;
import io.prestosql.plugin.hive.HiveColumnHandle;
import io.prestosql.plugin.hive.HivePartition;
import io.prestosql.plugin.hive.HiveType;
import io.prestosql.plugin.hive.metastore.Column;
import io.prestosql.plugin.hive.metastore.Table;
import io.prestosql.spi.HostAddress;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.Range;
import io.prestosql.spi.predicate.TupleDomain;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.Logger;
import org.apache.thrift.TBase;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.fs.s3a.Constants.ACCESS_KEY;
import static org.apache.hadoop.fs.s3a.Constants.ENDPOINT;
import static org.apache.hadoop.fs.s3a.Constants.SECRET_KEY;

/**
 * CarbonTableReader will be a facade of these utils
 * 1:CarbonMetadata,(logic table)
 * 2:FileFactory, (physic table file)
 * 3:CarbonCommonFactory, (offer some )
 * 4:DictionaryFactory, (parse dictionary util)
 * Currently, it is mainly used to parse metadata of tables under
 * the configured carbondata-store path and filter the relevant
 * input splits with given query predicates.
 */
public class CarbonTableReader {

  public CarbonTableConfig config;

  /**
   * A cache for Carbon reader, with this cache,
   * metadata of a table is only read from file system once.
   */
  private AtomicReference<Map<SchemaTableName, CarbonTableCacheModel>> carbonCache;

  private HBaseConnection hBaseConnection;
  /**
   * unique query id used for query statistics
   */
  private String queryId;

  /**
   * Logger instance
   */
  private static final Logger LOGGER =
      LogServiceFactory.getLogService(CarbonTableReader.class.getName());

  /**
   * List Of Schemas
   */
  private List<String> schemaNames = new ArrayList<>();

  @Inject public CarbonTableReader(CarbonTableConfig config, HBaseConnection hBaseConnection) {
    this.config = Objects.requireNonNull(config, "CarbonTableConfig is null");
    this.carbonCache = new AtomicReference(new ConcurrentHashMap<>());
    this.hBaseConnection = requireNonNull(hBaseConnection, "HBaseConnection is  null");
    populateCarbonProperties();
  }

  /**
   * For presto worker node to initialize the metadata cache of a table.
   *
   * @param table the name of the table and schema.
   * @return
   */
  public CarbonTableCacheModel getCarbonCache(SchemaTableName table, String location,
      Configuration config, Table hiveTable) {
    updateSchemaTables(table, config);
    CarbonTableCacheModel carbonTableCacheModel = carbonCache.get().get(table);
    if (carbonTableCacheModel == null || !carbonTableCacheModel.isValid()) {
      return parseCarbonMetadata(table, location, config, hiveTable);
    }
    return carbonTableCacheModel;
  }

  private CarbonTable getCarbonTable(Table table) {
    LinkedHashSet<Column> set = new LinkedHashSet();
    set.addAll(table.getDataColumns());
    set.addAll(table.getPartitionColumns());
    Field[] fields = new Field[set.size()];
    int i = 0;
    for (Column col : set) {
      fields[i] = new Field(getCarbonColumn(col, i));
      i++;
    }
    Schema schema = new Schema(fields, new HashMap<>(table.getStorage().getSerdeParameters()));
    CarbonWriterBuilder builder = new CarbonWriterBuilder();
    builder.outputPath(table.getStorage().getLocation());
    builder.withTableProperties(table.getStorage().getSerdeParameters());
    CarbonTable carbonTable = null;
    try {
      carbonTable = builder.buildLoadModel(schema).getCarbonDataLoadSchema().getCarbonTable();
      String isTransactional = table.getStorage().getSerdeParameters().get("isTransactional");
      carbonTable.setTransactionalTable(isTransactional != null?Boolean.parseBoolean(isTransactional): false);
      carbonTable.getTableInfo().getFactTable().getTableProperties().putAll(table.getStorage().getSerdeParameters());
      List<ColumnSchema> partCols =
          carbonTable.getTableInfo().getFactTable().getListOfColumns().stream().filter(
              f -> table.getPartitionColumns().stream()
                  .anyMatch(f1 -> f.getColumnName().equalsIgnoreCase(f1.getName())))
              .collect(Collectors.toList());
      if (!partCols.isEmpty()) {
        carbonTable.getTableInfo().getFactTable().setPartitionInfo(new PartitionInfo(partCols, PartitionType.NATIVE_HIVE));
        CarbonTable.updateTableByTableInfo(carbonTable, carbonTable.getTableInfo());
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return carbonTable;
  }

  private ColumnSchema getCarbonColumn(Column column, int ordinal) {
    HiveType hiveType = column.getType();
    if (hiveType.equals(HiveType.HIVE_DATE)) {
      return new ColumnSchema(column.getName(), DataTypes.DATE, ordinal);
    } else if (hiveType.equals(HiveType.HIVE_TIMESTAMP)) {
      return new ColumnSchema(column.getName(), DataTypes.TIMESTAMP, ordinal);
    } else if (hiveType.equals(HiveType.HIVE_BOOLEAN)) {
      return new ColumnSchema(column.getName(), DataTypes.BOOLEAN, ordinal);
    } else if (hiveType.equals(HiveType.HIVE_INT)) {
      return new ColumnSchema(column.getName(), DataTypes.INT, ordinal);
    } else if (hiveType.equals(HiveType.HIVE_LONG)) {
      return new ColumnSchema(column.getName(), DataTypes.LONG, ordinal);
    } else if (hiveType.equals(HiveType.HIVE_SHORT)) {
      return new ColumnSchema(column.getName(), DataTypes.SHORT, ordinal);
    } else if (hiveType.equals(HiveType.HIVE_STRING)) {
      return new ColumnSchema(column.getName(), DataTypes.STRING, ordinal);
    } else if (hiveType.equals(HiveType.HIVE_DOUBLE)) {
      return new ColumnSchema(column.getName(), DataTypes.DOUBLE, ordinal);
    } else if (hiveType.equals(HiveType.HIVE_BINARY)) {
      return new ColumnSchema(column.getName(), DataTypes.BINARY, ordinal);
    } else if (hiveType.equals(HiveType.HIVE_BYTE)) {
      return new ColumnSchema(column.getName(), DataTypes.BYTE, ordinal);
    } else if (hiveType.equals(HiveType.HIVE_FLOAT)) {
      return new ColumnSchema(column.getName(), DataTypes.FLOAT, ordinal);
    } else if (hiveType.getTypeInfo() instanceof DecimalTypeInfo) {
      DecimalTypeInfo typeInfo = (DecimalTypeInfo) hiveType.getTypeInfo();
      return new ColumnSchema(column.getName(), DataTypes.createDecimalType(typeInfo.precision(), typeInfo.scale()), ordinal);
    } else {
      throw new UnsupportedOperationException("Datatype not supported : " + hiveType);
    }
  }

  /**
   * Find all the tables under the schema store path (this.carbonFileList)
   * and cache all the table names in this.tableList. Notice that whenever this method
   * is called, it clears this.tableList and populate the list by reading the files.
   */
  private void updateSchemaTables(SchemaTableName schemaTableName, Configuration config) {
    CarbonTableCacheModel carbonTableCacheModel = carbonCache.get().get(schemaTableName);
    if (carbonTableCacheModel != null &&
        carbonTableCacheModel.getCarbonTable().isTransactionalTable()) {
      CarbonTable carbonTable = carbonTableCacheModel.getCarbonTable();
      long latestTime = FileFactory.getCarbonFile(CarbonTablePath
              .getSchemaFilePath(
                  carbonTable.getTablePath()),
          config).getLastModifiedTime();
      carbonTableCacheModel.setCurrentSchemaTime(latestTime);
      if (!carbonTableCacheModel.isValid()) {
        // Invalidate datamaps
        DataMapStoreManager.getInstance()
            .clearDataMaps(carbonTableCacheModel.getCarbonTable().getAbsoluteTableIdentifier());
      }
    }
  }

  /**
   * Read the metadata of the given table
   * and cache it in this.carbonCache (CarbonTableReader cache).
   *
   * @param table name of the given table.
   * @return the CarbonTableCacheModel instance which contains all the needed metadata for a table.
   */
  private CarbonTableCacheModel parseCarbonMetadata(SchemaTableName table, String tablePath,
      Configuration config, Table hiveTable) {
    try {
      CarbonTableCacheModel cache = getValidCacheBySchemaTableName(table);
      if (cache != null) {
        return cache;
      }
      // multiple tasks can be launched in a worker concurrently. Hence need to synchronize this.
      synchronized (this) {
        // cache might be filled by another thread, so if filled use that cache.
        CarbonTableCacheModel cacheModel = getValidCacheBySchemaTableName(table);
        if (cacheModel != null) {
          return cacheModel;
        }
        // Step 1: get store path of the table and cache it.
        String schemaFilePath = CarbonTablePath.getSchemaFilePath(tablePath, config);
        // If metadata folder exists, it is a transactional table
        CarbonFile schemaFile = FileFactory.getCarbonFile(schemaFilePath, config);
        boolean isTransactionalTable = schemaFile.exists();
        org.apache.carbondata.format.TableInfo tableInfo;
        long modifiedTime = System.currentTimeMillis();
        if (isTransactionalTable) {
          //Step 2: read the metadata (tableInfo) of the table.
          ThriftReader.TBaseCreator createTBase = new ThriftReader.TBaseCreator() {
            // TBase is used to read and write thrift objects.
            // TableInfo is a kind of TBase used to read and write table information.
            // TableInfo is generated by thrift,
            // see schema.thrift under format/src/main/thrift for details.
            public TBase create() {
              return new org.apache.carbondata.format.TableInfo();
            }
          };
          ThriftReader thriftReader = new ThriftReader(schemaFilePath, createTBase, config);
          thriftReader.open();
          tableInfo = (org.apache.carbondata.format.TableInfo) thriftReader.read();
          thriftReader.close();
          modifiedTime = schemaFile.getLastModifiedTime();
        } else {
          if (hiveTable != null) {
            return new CarbonTableCacheModel(System.currentTimeMillis(), getCarbonTable(hiveTable));
          }
          tableInfo = CarbonUtil.inferSchema(tablePath, table.getTableName(), false, config);
        }
        // Step 3: convert format level TableInfo to code level TableInfo
        SchemaConverter schemaConverter = new ThriftWrapperSchemaConverterImpl();
        // wrapperTableInfo is the code level information of a table in carbondata core,
        // different from the Thrift TableInfo.
        TableInfo wrapperTableInfo = schemaConverter
            .fromExternalToWrapperTableInfo(tableInfo, table.getSchemaName(), table.getTableName(),
                tablePath);
        wrapperTableInfo.setTransactionalTable(isTransactionalTable);
        CarbonMetadata.getInstance().removeTable(wrapperTableInfo.getTableUniqueName());
        // Step 4: Load metadata info into CarbonMetadata
        CarbonMetadata.getInstance().loadTableMetadata(wrapperTableInfo);
        CarbonTable carbonTable = Objects.requireNonNull(CarbonMetadata.getInstance()
            .getCarbonTable(table.getSchemaName(), table.getTableName()), "carbontable is null");
        cache = new CarbonTableCacheModel(modifiedTime, carbonTable);
        // cache the table
        carbonCache.get().put(table, cache);
        cache.setCarbonTable(carbonTable);
      }
      return cache;
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  private CarbonTableCacheModel getValidCacheBySchemaTableName(SchemaTableName schemaTableName) {
    CarbonTableCacheModel cache = carbonCache.get().get(schemaTableName);
    if (cache != null && cache.isValid()) {
      return cache;
    }
    return null;
  }

  /**
   * Get a carbon muti-block input splits
   *
   * @param tableCacheModel cached table
   * @param filters carbonData filters
   * @param constraints presto filters
   * @param config hadoop conf
   * @return list of multiblock split
   * @throws IOException
   */
  public CarbonSplitsHolder getInputSplits(
      CarbonTableCacheModel tableCacheModel,
      Expression filters,
      TupleDomain<HiveColumnHandle> constraints,
      Configuration config,
      List<HivePartition> partitions) throws IOException {
    List<CarbonLocalInputSplit> result = new ArrayList<>();
    List<CarbonLocalMultiBlockSplit> multiBlockSplitList = new ArrayList<>();
    CarbonTable carbonTable = tableCacheModel.getCarbonTable();

    TableInfo tableInfo = tableCacheModel.getCarbonTable().getTableInfo();
    config.set(CarbonTableInputFormat.INPUT_SEGMENT_NUMBERS, "");
    String carbonTablePath = carbonTable.getAbsoluteTableIdentifier().getTablePath();
    config.set(CarbonTableInputFormat.INPUT_DIR, carbonTablePath);
    config.set(CarbonTableInputFormat.DATABASE_NAME, carbonTable.getDatabaseName());
    config.set(CarbonTableInputFormat.TABLE_NAME, carbonTable.getTableName());
    config.set("query.id", queryId);
    CarbonInputFormat.setTransactionalTable(config, carbonTable.isTransactionalTable());
    CarbonTableInputFormat.setTableInfo(config, tableInfo);
    JobConf jobConf = new JobConf(config);
    List<PartitionSpec> filteredPartitions = new ArrayList<>();
    List<HBaseSplit> extraSplits = new ArrayList<>();

    PartitionInfo partitionInfo = carbonTable.getPartitionInfo();
    if (partitionInfo != null && partitionInfo.getPartitionType() == PartitionType.NATIVE_HIVE) {
      filteredPartitions = findRequiredPartitions(constraints, carbonTable, partitions);
    }
    try {
      HbaseTableHolder tableHolder = checkPointQuery(constraints, carbonTable, extraSplits);
      if (tableHolder.isPointQuery) {
        return new CarbonSplitsHolder(multiBlockSplitList, extraSplits);
      }
      List<PrunedSegmentInfo> pruneSegment =
          SegmentPrunerFactory.INSTANCE.getSegmentPruner(carbonTable)
              .pruneSegment(carbonTable, filters, new String[0], new String[0]);
      List<Segment> validCarbonSegs = pruneSegment.stream()
          .filter(f -> f.getSegment().getLoadMetadataDetails().isCarbonFormat())
          .map(PrunedSegmentInfo::getSegment).collect(Collectors.toList());
      CarbonInputFormat.setPrunedSegments(jobConf, validCarbonSegs);
      CarbonTableInputFormat<Object> carbonTableInputFormat =
          createInputFormat(jobConf, carbonTable.getAbsoluteTableIdentifier(),
              new DataMapFilter(carbonTable, filters, true), filteredPartitions);
      Job job = Job.getInstance(jobConf);
      List<InputSplit> splits = carbonTableInputFormat.getSplits(job);
      LOGGER.info("Pruned carbon Segments : " + validCarbonSegs + "solit size " + splits.size());
      if (splits.size() > 0) {
        Gson gson = new Gson();
        for (InputSplit inputSplit : splits) {
          CarbonInputSplit carbonInputSplit = (CarbonInputSplit) inputSplit;
          result.add(new CarbonLocalInputSplit(carbonInputSplit.getSegmentId(),
              carbonInputSplit.getPath().toString(), carbonInputSplit.getStart(),
              carbonInputSplit.getLength(), Arrays.asList(carbonInputSplit.getLocations()),
              carbonInputSplit.getNumberOfBlocklets(), carbonInputSplit.getVersion().number(),
              carbonInputSplit.getDeleteDeltaFiles(), carbonInputSplit.getBlockletId(),
              gson.toJson(carbonInputSplit.getDetailInfo()),
              carbonInputSplit.getFileFormat().ordinal()));
        }
        // Use block distribution
        List<List<CarbonLocalInputSplit>> inputSplits =
            new ArrayList<>(result.stream().collect(Collectors.groupingBy(carbonInput -> {
              if (FileFormat.ROW_V1.equals(carbonInput.getFileFormat())) {
                return carbonInput.getSegmentId().concat(carbonInput.getPath())
                    .concat(carbonInput.getStart() + "");
              }
              return carbonInput.getSegmentId().concat(carbonInput.getPath());
            })).values());
        // TODO : try to optimize the below loic as it may slowdown for huge splits
        for (int j = 0; j < inputSplits.size(); j++) {
          multiBlockSplitList.add(new CarbonLocalMultiBlockSplit(inputSplits.get(j),
              inputSplits.get(j).stream().flatMap(f -> Arrays.stream(getLocations(f))).distinct()
                  .toArray(String[]::new)));
        }
      }

      getHbaseSplits(constraints, carbonTable, extraSplits, pruneSegment, tableHolder);

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return new CarbonSplitsHolder(multiBlockSplitList, extraSplits);
  }

  private void getHbaseSplits(TupleDomain<HiveColumnHandle> constraints, CarbonTable carbonTable,
      List<HBaseSplit> extraSplits, List<PrunedSegmentInfo> pruneSegment, HbaseTableHolder tableHolder) throws IOException {
    List<Segment> hbaseSegs = pruneSegment.stream().filter(f -> !f.getSegment().isCarbonSegment() && f.getSegment().
        getLoadMetadataDetails().getFileFormat().getFormat().equalsIgnoreCase("hbase")).map(
            PrunedSegmentInfo::getSegment).collect(Collectors.toList());
    if (hbaseSegs.size() > 0) {
      HbaseCarbonTable hbaseTable = tableHolder.getOrCreateHbaseCarbonTable(carbonTable);
      long timestamp = 0;
      if (!pruneSegment.get(0).isIgnoreTimeStamp()) {
        SegmentMetaDataInfo segmentMetaDataInfo = hbaseSegs.get(0).getSegmentMetaDataInfo();
        Map<String, SegmentColumnMetaDataInfo> metaDataInfoMap = segmentMetaDataInfo.getSegmentColumnMetaDataInfoMap();
        SegmentColumnMetaDataInfo rowtimestamp = metaDataInfoMap.get("rowtimestamp");
        if (rowtimestamp != null && rowtimestamp.getColumnMinValue() != null) {
          timestamp = ByteUtil.toLong(rowtimestamp.getColumnMinValue(), 0,
              rowtimestamp.getColumnMinValue().length) + 1;
        }
      }
      List<HBaseSplit> hbaseSplits;
      if(Utils.isBatchGet(constraints, hbaseTable.getRow().getHbaseColumns())) {
        hbaseSplits = getSplitsForBatchGet(constraints, hbaseTable, timestamp);
      } else {
        hbaseSplits = getSplitsForScan(constraints, hbaseTable, timestamp);
      }
      extraSplits.addAll(hbaseSplits);
      LOGGER.info("Pruned hbase segments : " + hbaseSegs + " splits size : " + hbaseSplits.size() + " with min timestamp : " + timestamp);
    }
  }

  private HbaseTableHolder checkPointQuery(TupleDomain<HiveColumnHandle> constraints,
      CarbonTable carbonTable, List<HBaseSplit> extraSplits) throws IOException {
    HbaseTableHolder tableHolder = new HbaseTableHolder();
    if (isPointQuery(carbonTable)) {
      HbaseCarbonTable hbaseTable = tableHolder.getOrCreateHbaseCarbonTable(carbonTable);
      tableHolder.hbaseTable = hbaseTable;
      if(Utils.isBatchGet(constraints, hbaseTable.getRow().getHbaseColumns())) {
        extraSplits.addAll(getSplitsForBatchGet(constraints, hbaseTable, 0));
        LOGGER.info("Point query : " + extraSplits.size());
        tableHolder.isPointQuery = true;
      }
    }
    return tableHolder;
  }

  private static HbaseCarbonTable getHbaseTable(CarbonTable carbonTable) throws IOException {
    return HbaseMetastoreUtil.getHbaseTable(
        CarbonUtil.getExternalSchema(carbonTable.getAbsoluteTableIdentifier()).getQuerySchema());
  }

  private boolean isPointQuery(CarbonTable carbonTable) {
    return carbonTable.getTableInfo().getFactTable().getTableProperties().get("pointquery") != null ||
        carbonTable.getTableInfo().getFactTable().getTableProperties().get("custom.pruner") != null;
  }

  private List<HBaseSplit> getSplitsForScan(TupleDomain<HiveColumnHandle> constraints, HbaseCarbonTable tableHandle, long timestamp)
  {
    List<HBaseSplit> splits = new ArrayList<>();
    Pair<byte[][], byte[][]> startEndKeys = null;
    TableName hbaseTableName = TableName.valueOf(tableHandle.getNamespace(),tableHandle.getName());

    try {
      if (hBaseConnection.getHbaseAdmin().getTableDescriptor(hbaseTableName) != null) {
        RegionLocator regionLocator =
            hBaseConnection.getConn().getRegionLocator(hbaseTableName);
        startEndKeys = regionLocator.getStartEndKeys();
      }
    }
    catch (TableNotFoundException e) {
      throw new RuntimeException(format(
              "table %s with schema %s not found, maybe deleted by other user", tableHandle.getName(), tableHandle.getNamespace()));
    }
    catch (IOException e) {
      LOGGER.error(e);
      throw new RuntimeException(e);
    }

    Map<String, List<Range>> ranges = new HashMap<>();
    Map<HiveColumnHandle, Domain> predicates = constraints.getDomains().get();
    predicates.forEach((key, value) -> {
      if (key != null) {
        ranges.put(key.getName(), value.getValues().getRanges().getOrderedRanges());
      }
    });

    List<HostAddress> hostAddresses = new ArrayList<>();
    if (startEndKeys == null) {
      throw new NullPointerException("null pointer found when getting splits for scan");
    }
    for (int i = 0; i < startEndKeys.getFirst().length; i++) {
      String startRow = new String(startEndKeys.getFirst()[i]);
      String endRow = new String(startEndKeys.getSecond()[i]);
      splits.add(
          new HBaseSplit(hostAddresses, startRow, endRow, ranges, null, tableHandle, false, timestamp));
    }

    return splits;
  }

  private List<HBaseSplit> getSplitsForBatchGet(TupleDomain<HiveColumnHandle> constraints, HbaseCarbonTable tableHandle, long timestamp)
  {
    List<HBaseSplit> splits = new ArrayList<>();
    Domain rowIdDomain = null;
    Map<HiveColumnHandle, Domain> domains = constraints.getDomains().get();
    Map<String, List<Range>> rangesMap = new HashMap<>();
    int rangeSize = 0;
    for (Map.Entry<HiveColumnHandle, Domain> entry : domains.entrySet()) {
      HiveColumnHandle handle = entry.getKey();
      if (Utils.contains(handle.getName(), tableHandle.getRow().getHbaseColumns())) {
        rowIdDomain = entry.getValue();
        List<Range> rowIds = rowIdDomain != null ? rowIdDomain.getValues().getRanges().getOrderedRanges() : new ArrayList<>();
        rangeSize = rowIds.size();
        rangesMap.put(handle.getName(), rowIds);
      }
    }

    int maxSplitSize;
    // Each split has at least 20 pieces of data, and the maximum number of splits is 30.
    if (rangeSize / Constants.BATCHGET_SPLIT_RECORD_COUNT > Constants.BATCHGET_SPLIT_MAX_COUNT) {
      maxSplitSize = rangeSize / Constants.BATCHGET_SPLIT_MAX_COUNT;
    }
    else {
      maxSplitSize = Constants.BATCHGET_SPLIT_RECORD_COUNT;
    }

    List<HostAddress> hostAddresses = new ArrayList<>();

    int currentIndex = 0;
    while (currentIndex < rangeSize) {
      int endIndex = rangeSize - currentIndex > maxSplitSize ? (currentIndex + currentIndex) : rangeSize;
      Map<String, List<Range>> splitRange = new HashMap<>();
      for (Map.Entry<String, List<Range>> entry : rangesMap.entrySet()) {
        splitRange.put(entry.getKey(), entry.getValue().subList(currentIndex, endIndex));
      }
      splits.add(new HBaseSplit(hostAddresses, null, null, splitRange, null, tableHandle, true, timestamp));
      currentIndex += endIndex - currentIndex;
    }

    return splits;
  }

  /**
   * Returns list of partition specs to query based on the domain constraints
   *
   * @param constraints presto filter
   * @param carbonTable carbon table
   * @throws IOException
   */
  private List<PartitionSpec> findRequiredPartitions(TupleDomain<HiveColumnHandle> constraints,
      CarbonTable carbonTable, List<HivePartition> partitions) throws IOException {
    Set<PartitionSpec> partitionSpecs = partitions.stream().map(
        f -> new PartitionSpec(Arrays.asList(f.getPartitionId().split("/")),
            carbonTable.getTablePath()+"/"+f.getPartitionId())).collect(Collectors.toSet());
    List<String> partitionValuesFromExpression =
        PrestoFilterUtil.getPartitionFilters(carbonTable, constraints);
    return partitionSpecs.stream().filter(partitionSpec -> CollectionUtils
        .isSubCollection(partitionValuesFromExpression, partitionSpec.getPartitions()))
        .collect(Collectors.toList());
  }

  private CarbonTableInputFormat<Object> createInputFormat(Configuration conf,
      AbsoluteTableIdentifier identifier, DataMapFilter dataMapFilter,
      List<PartitionSpec> filteredPartitions) {
    CarbonTableInputFormat<Object> format = new CarbonTableInputFormat<>();
    CarbonTableInputFormat
        .setTablePath(conf, identifier.appendWithLocalPrefix(identifier.getTablePath()));
    CarbonTableInputFormat.setFilterPredicates(conf, dataMapFilter);
    if (filteredPartitions.size() != 0) {
      CarbonTableInputFormat.setPartitionsToPrune(conf, filteredPartitions);
    }
    return format;
  }

  private void populateCarbonProperties() {
    addProperty(CarbonCommonConstants.UNSAFE_WORKING_MEMORY_IN_MB, config.getUnsafeMemoryInMb());
    addProperty(CarbonCommonConstants.ENABLE_UNSAFE_IN_QUERY_EXECUTION,
        config.getEnableUnsafeInQueryExecution());
    addProperty(CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE,
        config.getEnableUnsafeColumnPage());
    addProperty(CarbonCommonConstants.ENABLE_UNSAFE_SORT, config.getEnableUnsafeSort());
    addProperty(CarbonCommonConstants.ENABLE_QUERY_STATISTICS, config.getEnableQueryStatistics());
    // TODO: Support configurable
    addProperty(CarbonCommonConstants.CARBON_WRITTEN_BY_APPNAME, "Presto_Server");
  }

  public Configuration updateS3Properties(Configuration configuration) {
    configuration.set(ACCESS_KEY, Objects.toString(config.getS3A_AcesssKey(), ""));
    configuration.set(SECRET_KEY, Objects.toString(config.getS3A_SecretKey()));
    configuration
        .set(CarbonCommonConstants.S3_ACCESS_KEY, Objects.toString(config.getS3_AcesssKey(), ""));
    configuration
        .set(CarbonCommonConstants.S3_SECRET_KEY, Objects.toString(config.getS3_SecretKey()));
    configuration
        .set(CarbonCommonConstants.S3N_ACCESS_KEY, Objects.toString(config.getS3N_AcesssKey(), ""));
    configuration
        .set(CarbonCommonConstants.S3N_SECRET_KEY, Objects.toString(config.getS3N_SecretKey(), ""));
    configuration.set(ENDPOINT, Objects.toString(config.getS3EndPoint(), ""));
    return configuration;
  }

  private void addProperty(String propertyName, String propertyValue) {
    if (propertyValue != null) {
      CarbonProperties.getInstance().addProperty(propertyName, propertyValue);
    }
  }

  /**
   * @param cis
   * @return
   */
  private String[] getLocations(CarbonLocalInputSplit cis) {
    return cis.getLocations().toArray(new String[0]);
  }

  public void setQueryId(String queryId) {
    this.queryId = queryId;
  }

  private static class HbaseTableHolder {
    private boolean isPointQuery;
    private HbaseCarbonTable hbaseTable;
    private HbaseCarbonTable getOrCreateHbaseCarbonTable(CarbonTable carbonTable) throws IOException {
      if (hbaseTable == null) {
        hbaseTable = getHbaseTable(carbonTable);
      }
      return hbaseTable;
    }
  }
}
