package org.apache.carbondata.externalstreaming

import org.apache.hadoop.hbase.HBaseTestingUtility
import org.apache.spark.sql.execution.command.management.CarbonAddExternalStreamingSegmentCommand
import org.apache.spark.sql.execution.datasources.hbase.{HBaseRelation, HBaseTableCatalog, HandOffOptions, HandoffHbaseSegmentCommand, SparkHBaseConf}
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.hbase.HBaseConstants
import org.apache.spark.sql.functions._
import org.junit.Ignore

@Ignore
class TestAllDataTypeTestCase extends QueryTest with BeforeAndAfterAll {

  var htu: HBaseTestingUtility = _
  var hBaseConfPath:String = _

  val writeCatTimestamp =
    s"""{
       |"table":{"namespace":"default", "name":"shcExampleTable1", "tableCoder":"Phoenix"},
       |"rowkey":"col0:col1",
       |"columns":{
       |"rowkey.col0":{"cf":"rowkey", "col":"col0", "type":"int"},
       |"rowkey.col1":{"cf":"rowkey", "col":"col1", "type":"string"},
       |"cf2.col0":{"cf":"cf2", "col":"col0", "type":"int"},
       |"cf2.col1":{"cf":"cf2", "col":"col1", "type":"string"},
       |"cf2.col2":{"cf":"cf2", "col":"col2", "type":"bigint"},
       |"cf2.col3":{"cf":"cf2", "col":"col3", "type":"double"},
       |"cf2.col4":{"cf":"cf2", "col":"col4", "type":"string"},
       |"cf2.col5":{"cf":"cf2", "col":"col5", "type":"double"},
       |"cf2.col6":{"cf":"cf2", "col":"col6", "type":"double"},
       |"cf2.col7":{"cf":"cf2", "col":"col7", "type":"boolean"},
       |"cf2.col8":{"cf":"cf2", "col":"col8", "type":"short"},
       |"cf2.col9":{"cf":"cf2", "col":"col9", "type":"bigint"},
       |"cf2.col10":{"cf":"cf2", "col":"col10", "type":"double"}
       |}
       |}""".stripMargin


//  val writeCatTimestampAll =
//    s"""{
//       |"table":{"namespace":"default", "name":"shcExampleTable1", "tableCoder":"Phoenix"},
//       |"rowkey":"col0",
//       |"columns":{
//       |"rowkey.col0":{"cf":"rowkey", "col":"col0", "type":"int"},
//       |"cf2.col0":{"cf":"cf2", "col":"col0", "type":"int"},
//       |"cf2.col1":{"cf":"cf2", "col":"col1", "type":"string"},
//       |"cf2.col2":{"cf":"cf2", "col":"col2", "type":"bigint"},
//       |"cf2.col3":{"cf":"cf2", "col":"col3", "type":"float"},
//       |"cf2.col4":{"cf":"cf2", "col":"col4", "type":"string"},
//       |"cf2.col5":{"cf":"cf2", "col":"col5", "type":"double"},
//       |"cf2.col6":{"cf":"cf2", "col":"col6", "type":"double"},
//       |"cf2.col7":{"cf":"cf2", "col":"col7", "type":"boolean"},
//       |"cf2.col8":{"cf":"cf2", "col":"col8", "type":"short"},
//       |"cf2.col9":{"cf":"cf2", "col":"col9", "type":"timestamp"},
//       |"cf2.col10":{"cf":"cf2", "col":"col10", "type":"Decimal(10,2)"}
//       |}
//       |}""".stripMargin

  override def beforeAll: Unit = {
    htu = new HBaseTestingUtility()
//    htu.startMiniCluster(1)
    SparkHBaseConf.conf = htu.getConfiguration
    import sqlContext.implicits._
    hBaseConfPath = s"$integrationPath/hbase/src/test/resources/hbase-site-local.xml"
    CarbonProperties.getInstance()
      .addProperty(HBaseConstants.CARBON_HBASE_CONF_FILE_PATH, hBaseConfPath)

    val data = (0 until 10).map { i =>
      MultiDataTypeKeyRecordGenerator(i)
    }
    val shcExampleTable1Option = Map(HBaseTableCatalog.tableCatalog -> writeCatTimestamp,
      HBaseTableCatalog.newTable -> "5",
      HBaseRelation.HBASE_CONFIGFILE -> hBaseConfPath
    )

    val frame = sqlContext.sparkContext.parallelize(data).toDF
      .select(col("col0").as("rowkey.col0"),
        col("col1").as("rowkey.col1"),
        col("col0").as("cf2.col0"),
       col("col1").as("cf2.col1"),
      col("col2").as("cf2.col2"),
      col("col3").as("cf2.col3"),
      col("col4").as("cf2.col4"),
      col("col5").as("cf2.col5"),
      col("col6").as("cf2.col6"),
      col("col7").as("cf2.col7"),
      col("col8").as("cf2.col8"),
      col("col9").as("cf2.col9"),
      col("col10").as("cf2.col10"))

    frame.write.options(shcExampleTable1Option)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()

    sql("DROP TABLE IF EXISTS alldatatype")
    sql(
      "create table alldatatype(col0 int, col1 String, col2 long, col3 double, col4 String, col5 double, col6 double," +
      "col7 boolean, col8 short, col9 long, col10 double) " +
      "stored as carbondata")


//    sql(
//      "create table alldatatype(col0 int, col1 String, col2 long, col3 float, col4 date, col5 double, col6 double," +
//      "col7 boolean, col8 short, col9 timestamp, col10 Decimal(10,2)) " +
//      "stored as carbondata")
    var options = Map("format" -> "HBase")
    options = options + ("defaultColumnFamily" -> "cf2")
    options = options + ("rowKeyColumnFamily" -> "rowkey")
    CarbonAddExternalStreamingSegmentCommand(Some("default"),
      "alldatatype",writeCatTimestamp, None,
      options).processMetadata(
      sqlContext.sparkSession)
  }

  test("test handoff segment") {
    val prevRows = sql("select * from alldatatype").collect()
    HandoffHbaseSegmentCommand(None, "alldatatype", Option.empty, new HandOffOptions().setDeleteRows(false).setGraceTimeInMillis(0)).run(sqlContext.sparkSession)
    val frame = sql("select * from alldatatype")
    checkAnswer(frame, prevRows)
    val data = (10 until 20).map { i =>
      MultiDataTypeKeyRecordGenerator(i)
    }
    val shcExampleTable1Option = Map(HBaseTableCatalog.tableCatalog -> writeCatTimestamp,
      HBaseTableCatalog.newTable -> "5",
      HBaseRelation.HBASE_CONFIGFILE -> hBaseConfPath
    )
    import sqlContext.implicits._
    val outFrame = sqlContext.sparkContext.parallelize(data).toDF
      .select(col("col0").as("rowkey.col0"),
        col("col1").as("rowkey.col1"),
        col("col0").as("cf2.col0"),
        col("col1").as("cf2.col1"),
        col("col2").as("cf2.col2"),
        col("col3").as("cf2.col3"),
        col("col4").as("cf2.col4"),
        col("col5").as("cf2.col5"),
        col("col6").as("cf2.col6"),
        col("col7").as("cf2.col7"),
        col("col8").as("cf2.col8"),
        col("col9").as("cf2.col9"),
        col("col10").as("cf2.col10"))
    outFrame.write.options(shcExampleTable1Option)
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()
    val rows = sql("select * from alldatatype").collectAsList()
    assert(rows.size() == 20)
    assert(sql("select * from alldatatype where segmentid(1)").collectAsList().size() == 10)
    assert(sql("select * from alldatatype where segmentid(2)").collectAsList().size() == 10)
  }

}
