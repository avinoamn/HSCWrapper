package unit

import github.avinoamn.HSCWrapper.HSCWrapper._
import github.avinoamn.HSCWrapper.models.{HBColumn, HBTable}
import org.apache.hadoop.hbase.spark.datasources.{DataTypeParserWrapper, HBaseTableCatalog}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.types._
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FeatureSpec}

class BuildCatalogUnit extends FeatureSpec with BeforeAndAfterEach with BeforeAndAfterAll {
  val mapType: DataType = MapType(IntegerType, new StructType().add(StructField("varchar", StringType)))
  val arrayType: DataType = ArrayType(new StructType().add(StructField("int", IntegerType)))
  val arrayMapType: DataType = MapType(IntegerType, ArrayType(DoubleType))

  val hbTable: HBTable = HBTable("htable")
  val hbColumns: Array[HBColumn] = Array(
    HBColumn("rowkey:key1", "col1", StringType),
    HBColumn("rowkey:key2", "col2", DoubleType),
    HBColumn("cf1:col2", "col3", BinaryType),
    HBColumn("cf1:col3", "col4", TimestampType),
    HBColumn("cf1:col4", "col5", DoubleType),
    HBColumn("cf1:col5", "col6", mapType),
    HBColumn("cf1:col6", "col7", arrayType),
    HBColumn("cf1:col7", "col8", arrayMapType),
    HBColumn("cf1:col8", "col9", DateType),
    HBColumn("cf1:col9", "col10", TimestampType)
  )

  val catalog: String = buildCatalog(hbTable, hbColumns)
  val parameters: Map[String, String] = Map(HBaseTableCatalog.tableCatalog->catalog)
  val t: HBaseTableCatalog = HBaseTableCatalog(parameters)

  def checkDataType(dataTypeString: String, expectedDataType: DataType): Unit = {
    scenario(s"parse ${dataTypeString.replace("\n", "")}") {
      assert(DataTypeParserWrapper.parse(dataTypeString) === expectedDataType)
    }
  }

  feature("buildCatalog") {
    scenario("basic") {
      assert(t.getField("col1").isRowKey)
      assert(t.getPrimaryKey == "key1")
      assert(t.getField("col3").dt == BinaryType)
      assert(t.getField("col4").dt == TimestampType)
      assert(t.getField("col5").dt == DoubleType)
      assert(t.getField("col5").serdes.isEmpty)
      assert(t.getField("col4").serdes.isEmpty)
      assert(t.getField("col1").isRowKey)
      assert(t.getField("col2").isRowKey)
      assert(!t.getField("col3").isRowKey)
      assert(t.getField("col2").length == Bytes.SIZEOF_DOUBLE)
      assert(t.getField("col1").length == -1)
      assert(t.getField("col8").length == -1)
      assert(t.getField("col9").dt == DateType)
      assert(t.getField("col10").dt == TimestampType)
    }
  }

  checkDataType(
    mapType.simpleString,
    t.getField("col6").dt
  )

  checkDataType(
    arrayType.simpleString,
    t.getField("col7").dt
  )

  checkDataType(
    arrayMapType.simpleString,
    t.getField("col8").dt
  )
}
