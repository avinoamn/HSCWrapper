package github.avinoamn.HSCWrapper.E2E

import github.avinoamn.HSCWrapper.utils.HBaseTestUtils
import java.sql.{Date, Timestamp}
import org.apache.hadoop.hbase.spark.datasources.HBaseSparkConf
import github.avinoamn.HSCWrapper.utils.TestConsts.{columnFamily, t1TableName, t2TableName, t3TableName, timestamp}
import org.apache.hadoop.hbase.spark.DefaultSourceStaticUtils
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FeatureSpec}
import github.avinoamn.HSCWrapper.utils.TestDataUtils.{readTestDatafromHbaseAndRegisterTempTables, writeTestDataToHBase}

class HSCWrapperE2E extends FeatureSpec with BeforeAndAfterEach with BeforeAndAfterAll {
  implicit val spark: SparkSession = SparkSession.builder()
    .master("local")
    .appName("HSCWrapperE2E")
    .config(new SparkConf()
      .set(HBaseSparkConf.QUERY_CACHEBLOCKS, "true")
      .set(HBaseSparkConf.QUERY_BATCHSIZE, "100")
      .set(HBaseSparkConf.QUERY_CACHEDROWS, "100"))
    .getOrCreate()

  val sqlContext: SQLContext = spark.sqlContext

  override def beforeAll(): Unit = {
    HBaseTestUtils.startMiniCluster()

    HBaseTestUtils.createHBaseContext(spark.sparkContext)

    HBaseTestUtils.deleteAllTables()
    HBaseTestUtils.createTable(t1TableName, Array(columnFamily))
    HBaseTestUtils.createTable(t2TableName, Array(columnFamily))
    HBaseTestUtils.createTable(t3TableName, Array(columnFamily))

    writeTestDataToHBase()
    readTestDatafromHbaseAndRegisterTempTables()
  }

  override def afterAll(): Unit = {
    HBaseTestUtils.deleteAllTables()
    HBaseTestUtils.shutdownMiniCluster()

    spark.stop()
  }

  override def beforeEach(): Unit = {
    DefaultSourceStaticUtils.lastFiveExecutionRules.clear()
  }

  feature("HSCWrapper write/read") {
    /**
     * A example of query three fields and also only using rowkey points for the filter
     */
    scenario("Test rowKey point only rowKey query") {
      val results = sqlContext.sql("SELECT KEY_FIELD, B_FIELD, A_FIELD FROM hbaseTable1 " +
        "WHERE " +
        "(KEY_FIELD = 'get1' or KEY_FIELD = 'get2' or KEY_FIELD = 'get3')").take(10)

      val executionRules = DefaultSourceStaticUtils.lastFiveExecutionRules.poll()

      assert(results.length == 3)

      assert(executionRules.dynamicLogicExpression.toExpressionString.
        equals("( ( KEY_FIELD == 0 OR KEY_FIELD == 1 ) OR KEY_FIELD == 2 )"))

      assert(executionRules.rowKeyFilter.points.size == 3)
      assert(executionRules.rowKeyFilter.ranges.size == 0)
    }

    /**
     * A example of query three fields and also only using cell points for the filter
     */
    scenario("Test cell point only rowKey query") {
      val results = sqlContext.sql("SELECT KEY_FIELD, B_FIELD, A_FIELD FROM hbaseTable1 " +
        "WHERE " +
        "(B_FIELD = '4' or B_FIELD = '10' or A_FIELD = 'foo1')").take(10)

      val executionRules = DefaultSourceStaticUtils.lastFiveExecutionRules.poll()

      assert(results.length == 3)

      assert(executionRules.dynamicLogicExpression.toExpressionString.
        equals("( ( B_FIELD == 0 OR B_FIELD == 1 ) OR A_FIELD == 2 )"))
    }

    /**
     * A example of a OR merge between to ranges the result is one range
     * Also an example of less then and greater then
     */
    scenario("Test two range rowKey query") {
      val results = sqlContext.sql("SELECT KEY_FIELD, B_FIELD, A_FIELD FROM hbaseTable1 " +
        "WHERE " +
        "( KEY_FIELD < 'get2' or KEY_FIELD > 'get3')").take(10)

      val executionRules = DefaultSourceStaticUtils.lastFiveExecutionRules.poll()

      assert(results.length == 3)

      assert(executionRules.dynamicLogicExpression.toExpressionString.
        equals("( KEY_FIELD < 0 OR KEY_FIELD > 1 )"))

      assert(executionRules.rowKeyFilter.points.size == 0)
      assert(executionRules.rowKeyFilter.ranges.size == 2)

      val scanRange1 = executionRules.rowKeyFilter.ranges.get(0).get
      assert(Bytes.equals(scanRange1.lowerBound, Bytes.toBytes("")))
      assert(Bytes.equals(scanRange1.upperBound, Bytes.toBytes("get2")))
      assert(scanRange1.isLowerBoundEqualTo)
      assert(!scanRange1.isUpperBoundEqualTo)

      val scanRange2 = executionRules.rowKeyFilter.ranges.get(1).get
      assert(Bytes.equals(scanRange2.lowerBound, Bytes.toBytes("get3")))
      assert(scanRange2.upperBound == null)
      assert(!scanRange2.isLowerBoundEqualTo)
      assert(scanRange2.isUpperBoundEqualTo)
    }

    /**
     * A example of a OR merge between to ranges the result is one range
     * Also an example of less then and greater then
     *
     * This example makes sure the code works for a int rowKey
     */
    scenario("Test two range rowKey query where the rowKey is Int and there is a range over lap") {
      val results = sqlContext.sql("SELECT KEY_FIELD, B_FIELD, A_FIELD FROM hbaseTable2 " +
        "WHERE " +
        "( KEY_FIELD < 4 or KEY_FIELD > 2)").take(10)

      val executionRules = DefaultSourceStaticUtils.lastFiveExecutionRules.poll()


      assert(executionRules.dynamicLogicExpression.toExpressionString.
        equals("( KEY_FIELD < 0 OR KEY_FIELD > 1 )"))

      assert(executionRules.rowKeyFilter.points.size == 0)
      assert(executionRules.rowKeyFilter.ranges.size == 2)
      assert(results.length == 5)
    }

    /**
     * A example of a OR merge between to ranges the result is two ranges
     * Also an example of less then and greater then
     *
     * This example makes sure the code works for a int rowKey
     */
    scenario("Test two range rowKey query where the rowKey is Int and the ranges don't over lap") {
      val results = sqlContext.sql("SELECT KEY_FIELD, B_FIELD, A_FIELD FROM hbaseTable2 " +
        "WHERE " +
        "( KEY_FIELD < 2 or KEY_FIELD > 4)").take(10)

      val executionRules = DefaultSourceStaticUtils.lastFiveExecutionRules.poll()

      assert(executionRules.dynamicLogicExpression.toExpressionString.
        equals("( KEY_FIELD < 0 OR KEY_FIELD > 1 )"))

      assert(executionRules.rowKeyFilter.points.size == 0)

      assert(executionRules.rowKeyFilter.ranges.size == 3)

      val scanRange1 = executionRules.rowKeyFilter.ranges.get(0).get
      assert(Bytes.equals(scanRange1.upperBound, Bytes.toBytes(2)))
      assert(scanRange1.isLowerBoundEqualTo)
      assert(!scanRange1.isUpperBoundEqualTo)

      val scanRange2 = executionRules.rowKeyFilter.ranges.get(1).get
      assert(scanRange2.isUpperBoundEqualTo)

      assert(results.length == 2)
    }

    /**
     * A example of a AND merge between to ranges the result is one range
     * Also an example of less then and equal to and greater then and equal to
     */
    scenario("Test one combined range rowKey query") {
      val results = sqlContext.sql("SELECT KEY_FIELD, B_FIELD, A_FIELD FROM hbaseTable1 " +
        "WHERE " +
        "(KEY_FIELD <= 'get3' and KEY_FIELD >= 'get2')").take(10)

      val executionRules = DefaultSourceStaticUtils.lastFiveExecutionRules.poll()

      assert(results.length == 2)

      val expr = executionRules.dynamicLogicExpression.toExpressionString
      assert(expr.equals("( ( KEY_FIELD isNotNull AND KEY_FIELD <= 0 ) AND KEY_FIELD >= 1 )"), expr)

      assert(executionRules.rowKeyFilter.points.size == 0)
      assert(executionRules.rowKeyFilter.ranges.size == 1)

      val scanRange1 = executionRules.rowKeyFilter.ranges.get(0).get
      assert(Bytes.equals(scanRange1.lowerBound, Bytes.toBytes("get2")))
      assert(Bytes.equals(scanRange1.upperBound, Bytes.toBytes("get3")))
      assert(scanRange1.isLowerBoundEqualTo)
      assert(scanRange1.isUpperBoundEqualTo)

    }

    /**
     * Do a select with no filters
     */
    scenario("Test select only query") {

      val results = sqlContext.sql("SELECT KEY_FIELD FROM hbaseTable2").take(10)
      assert(results.length == 5)

      val executionRules = DefaultSourceStaticUtils.lastFiveExecutionRules.poll()

      assert(executionRules.dynamicLogicExpression == null)

    }

    /**
     * A complex query with one point and one range for both the
     * rowKey and the a column
     */
    scenario("Test SQL point and range combo") {
      val results = sqlContext.sql("SELECT KEY_FIELD FROM hbaseTable1 " +
        "WHERE " +
        "(KEY_FIELD = 'get1' and B_FIELD < '3') or " +
        "(KEY_FIELD >= 'get3' and B_FIELD = '8')").take(5)

      val executionRules = DefaultSourceStaticUtils.lastFiveExecutionRules.poll()

      assert(executionRules.dynamicLogicExpression.toExpressionString.
        equals("( ( KEY_FIELD == 0 AND B_FIELD < 1 ) OR " +
          "( KEY_FIELD >= 2 AND B_FIELD == 3 ) )"))

      assert(executionRules.rowKeyFilter.points.size == 1)
      assert(executionRules.rowKeyFilter.ranges.size == 1)

      val scanRange1 = executionRules.rowKeyFilter.ranges.get(0).get
      assert(Bytes.equals(scanRange1.lowerBound, Bytes.toBytes("get3")))
      assert(scanRange1.upperBound == null)
      assert(scanRange1.isLowerBoundEqualTo)
      assert(scanRange1.isUpperBoundEqualTo)


      assert(results.length == 3)
    }

    /**
     * A complex query with two complex ranges that doesn't merge into one
     */
    scenario("Test two complete range non merge rowKey query") {
      val results = sqlContext.sql("SELECT KEY_FIELD, B_FIELD, A_FIELD FROM hbaseTable2 " +
        "WHERE " +
        "( KEY_FIELD >= 1 and KEY_FIELD <= 2) or" +
        "( KEY_FIELD > 3 and KEY_FIELD <= 5)").take(10)


      assert(results.length == 4)
      val executionRules = DefaultSourceStaticUtils.lastFiveExecutionRules.poll()
      assert(executionRules.dynamicLogicExpression.toExpressionString.
        equals("( ( KEY_FIELD >= 0 AND KEY_FIELD <= 1 ) OR " +
          "( KEY_FIELD > 2 AND KEY_FIELD <= 3 ) )"))

      assert(executionRules.rowKeyFilter.points.size == 0)
      assert(executionRules.rowKeyFilter.ranges.size == 2)

      val scanRange1 = executionRules.rowKeyFilter.ranges.get(0).get
      assert(Bytes.equals(scanRange1.lowerBound, Bytes.toBytes(1)))
      assert(Bytes.equals(scanRange1.upperBound, Bytes.toBytes(2)))
      assert(scanRange1.isLowerBoundEqualTo)
      assert(scanRange1.isUpperBoundEqualTo)

      val scanRange2 = executionRules.rowKeyFilter.ranges.get(1).get
      assert(Bytes.equals(scanRange2.lowerBound, Bytes.toBytes(3)))
      assert(Bytes.equals(scanRange2.upperBound, Bytes.toBytes(5)))
      assert(!scanRange2.isLowerBoundEqualTo)
      assert(scanRange2.isUpperBoundEqualTo)

    }

    /**
     * A complex query with two complex ranges that does merge into one
     */
    scenario("Test two complete range merge rowKey query") {
      val results = sqlContext.sql("SELECT KEY_FIELD, B_FIELD, A_FIELD FROM hbaseTable1 " +
        "WHERE " +
        "( KEY_FIELD >= 'get1' and KEY_FIELD <= 'get2') or" +
        "( KEY_FIELD > 'get3' and KEY_FIELD <= 'get5')").take(10)

      val executionRules = DefaultSourceStaticUtils.lastFiveExecutionRules.poll()

      assert(results.length == 4)

      assert(executionRules.dynamicLogicExpression.toExpressionString.
        equals("( ( KEY_FIELD >= 0 AND KEY_FIELD <= 1 ) OR " +
          "( KEY_FIELD > 2 AND KEY_FIELD <= 3 ) )"))

      assert(executionRules.rowKeyFilter.points.size == 0)
      assert(executionRules.rowKeyFilter.ranges.size == 2)

      val scanRange1 = executionRules.rowKeyFilter.ranges.get(0).get
      assert(Bytes.equals(scanRange1.lowerBound, Bytes.toBytes("get1")))
      assert(Bytes.equals(scanRange1.upperBound, Bytes.toBytes("get2")))
      assert(scanRange1.isLowerBoundEqualTo)
      assert(scanRange1.isUpperBoundEqualTo)

      val scanRange2 = executionRules.rowKeyFilter.ranges.get(1).get
      assert(Bytes.equals(scanRange2.lowerBound, Bytes.toBytes("get3")))
      assert(Bytes.equals(scanRange2.upperBound, Bytes.toBytes("get5")))
      assert(!scanRange2.isLowerBoundEqualTo)
      assert(scanRange2.isUpperBoundEqualTo)
    }

    scenario("Test OR logic with a one RowKey and One column") {
      val results = sqlContext.sql("SELECT KEY_FIELD, B_FIELD, A_FIELD FROM hbaseTable1 " +
        "WHERE " +
        "( KEY_FIELD >= 'get1' or A_FIELD <= 'foo2') or" +
        "( KEY_FIELD > 'get3' or B_FIELD <= '4')").take(10)

      val executionRules = DefaultSourceStaticUtils.lastFiveExecutionRules.poll()

      assert(results.length == 5)

      assert(executionRules.dynamicLogicExpression.toExpressionString.
        equals("( ( KEY_FIELD >= 0 OR A_FIELD <= 1 ) OR " +
          "( KEY_FIELD > 2 OR B_FIELD <= 3 ) )"))

      assert(executionRules.rowKeyFilter.points.size == 0)
      assert(executionRules.rowKeyFilter.ranges.size == 1)

      val scanRange1 = executionRules.rowKeyFilter.ranges.get(0).get
      //This is the main test for 14406
      //Because the key is joined through a or with a qualifier
      //There is no filter on the rowKey
      assert(Bytes.equals(scanRange1.lowerBound, Bytes.toBytes("")))
      assert(scanRange1.upperBound == null)
      assert(scanRange1.isLowerBoundEqualTo)
      assert(scanRange1.isUpperBoundEqualTo)
    }

    scenario("Test OR logic with a two columns") {
      val results = sqlContext.sql("SELECT KEY_FIELD, B_FIELD, A_FIELD FROM hbaseTable1 " +
        "WHERE " +
        "( B_FIELD > '4' or A_FIELD <= 'foo2') or" +
        "( A_FIELD > 'foo2' or B_FIELD < '4')").take(10)

      val executionRules = DefaultSourceStaticUtils.lastFiveExecutionRules.poll()

      assert(results.length == 5)

      assert(executionRules.dynamicLogicExpression.toExpressionString.
        equals("( ( B_FIELD > 0 OR A_FIELD <= 1 ) OR " +
          "( A_FIELD > 2 OR B_FIELD < 3 ) )"))

      assert(executionRules.rowKeyFilter.points.size == 0)
      assert(executionRules.rowKeyFilter.ranges.size == 1)

      val scanRange1 = executionRules.rowKeyFilter.ranges.get(0).get
      assert(Bytes.equals(scanRange1.lowerBound, Bytes.toBytes("")))
      assert(scanRange1.upperBound == null)
      assert(scanRange1.isLowerBoundEqualTo)
      assert(scanRange1.isUpperBoundEqualTo)

    }

    scenario("Test single RowKey Or Column logic") {
      val results = sqlContext.sql("SELECT KEY_FIELD, B_FIELD, A_FIELD FROM hbaseTable1 " +
        "WHERE " +
        "( KEY_FIELD >= 'get4' or A_FIELD <= 'foo2' )").take(10)

      val executionRules = DefaultSourceStaticUtils.lastFiveExecutionRules.poll()

      assert(results.length == 4)

      assert(executionRules.dynamicLogicExpression.toExpressionString.
        equals("( KEY_FIELD >= 0 OR A_FIELD <= 1 )"))

      assert(executionRules.rowKeyFilter.points.size == 0)
      assert(executionRules.rowKeyFilter.ranges.size == 1)

      val scanRange1 = executionRules.rowKeyFilter.ranges.get(0).get
      assert(Bytes.equals(scanRange1.lowerBound, Bytes.toBytes("")))
      assert(scanRange1.upperBound == null)
      assert(scanRange1.isLowerBoundEqualTo)
      assert(scanRange1.isUpperBoundEqualTo)
    }

    scenario("Test mapping") {
      val results = sqlContext.sql("SELECT binary_field, boolean_field, " +
        "byte_field, short_field, int_field, long_field, " +
        "float_field, double_field, date_field, timestamp_field, " +
        "string_field FROM hbaseTestMapping").collect()

      assert(results.length == 1)

      val result = results(0)

      System.out.println("row: " + result)
      System.out.println("0: " + result.get(0))
      System.out.println("1: " + result.get(1))
      System.out.println("2: " + result.get(2))
      System.out.println("3: " + result.get(3))

      assert(result.get(0).asInstanceOf[Array[Byte]].sameElements(Array(1.toByte, 2.toByte, 3.toByte)))
      assert(result.get(1) == true)
      assert(result.get(2) == 127)
      assert(result.get(3) == 32767)
      assert(result.get(4) == 1000000)
      assert(result.get(5) == 10000000000L)
      assert(result.get(6) == 0.5)
      assert(result.get(7) == 0.125)
      // sql date stores only year, month and day, so checking it is within a day
      assert(Math.abs(result.get(8).asInstanceOf[Date].getTime - timestamp) <= 86400000)
      assert(result.get(9).asInstanceOf[Timestamp].getTime == timestamp)
      assert(result.get(10) == "string")
    }
  }
}
