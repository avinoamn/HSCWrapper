package E2E

import github.avinoamn.HSCWrapper.HSCWrapper._
import github.avinoamn.HSCWrapper.models.{HBColumn, HBTable}
import utils.Consts.{testTableColumnFamily, testTableName}
import org.apache.hadoop.hbase.spark.{DefaultSourceStaticUtils, HBaseContext}
import org.apache.hadoop.hbase.{HBaseTestingUtility, StartMiniClusterOption, TableName}
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FeatureSpec}
import utils.HBaseTestUtils

case class IntKeyRecord(
  col0: Integer,
  col1: Boolean,
  col2: Double,
  col3: Float,
  col4: Int,
  col5: Long,
  col6: Short,
  col7: String,
  col8: Byte)

object IntKeyRecord {
  def apply(i: Int): IntKeyRecord = {
    IntKeyRecord(if (i % 2 == 0) i else -i,
      i % 2 == 0,
      i.toDouble,
      i.toFloat,
      i,
      i.toLong,
      i.toShort,
      s"String$i extra",
      i.toByte)
  }
}

class HSCWrapperE2E extends FeatureSpec with BeforeAndAfterEach with BeforeAndAfterAll {
  implicit val spark: SparkSession = SparkSession.builder()
    .master("local")
    .appName("HSCWrapperE2E")
    .config(new SparkConf())
    .getOrCreate()

  override def beforeAll(): Unit = {
    HBaseTestUtils.startMiniCluster()

    HBaseTestUtils.deleteAllTables()
    HBaseTestUtils.createTable(testTableName, Array(testTableColumnFamily))

    HBaseTestUtils.createHBaseContext(spark.sparkContext)
  }

  override def afterAll(): Unit = {
    HBaseTestUtils.deleteAllTables()
    HBaseTestUtils.shutdownMiniCluster()

    spark.stop()
  }

  override def beforeEach(): Unit = {
    DefaultSourceStaticUtils.lastFiveExecutionRules.clear()
  }

  feature("E2E") {
    scenario("simple write/read") {
      import spark.implicits._

      // test populate table
      val data = (0 until 32).map { i =>
        IntKeyRecord(i)
      }

      val table = HBTable(testTableName)

      val writeColumns = Array(
        "rowkey",
        s"${testTableColumnFamily}:col1",
        s"${testTableColumnFamily}:col2",
        s"${testTableColumnFamily}:col3",
        s"${testTableColumnFamily}:col4",
        s"${testTableColumnFamily}:col5",
        s"${testTableColumnFamily}:col6",
        s"${testTableColumnFamily}:col7",
        s"${testTableColumnFamily}:col8")

      val inputDS = spark.sparkContext.parallelize(data).toDS()
      write(inputDS, table, writeColumns)


      val readColumns = Array[HBColumn](
        HBColumn("rowkey", dataType = IntegerType),
        HBColumn(s"${testTableColumnFamily}:col1", "isEven_field", BooleanType),
        HBColumn(s"${testTableColumnFamily}:col2", "double_field", DoubleType),
        HBColumn(s"${testTableColumnFamily}:col3", "float_field", FloatType),
        HBColumn(s"${testTableColumnFamily}:col4", "int_field", IntegerType),
        HBColumn(s"${testTableColumnFamily}:col5", "long_field", LongType),
        HBColumn(s"${testTableColumnFamily}:col6", "short_field", ShortType),
        HBColumn(s"${testTableColumnFamily}:col7", "string_field", StringType),
        HBColumn(s"${testTableColumnFamily}:col8", "byte_field", ByteType))

      val outputDF = read(table, readColumns)
      outputDF.show(32, truncate = false)
    }
  }
}
