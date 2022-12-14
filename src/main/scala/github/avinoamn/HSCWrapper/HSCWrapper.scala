package github.avinoamn.HSCWrapper

import github.avinoamn.HSCWrapper.models.{HBColumn, HBTable}
import github.avinoamn.HSCWrapper.utils.ColumnsUtils.{dropNullRows, getHBColumns, getRowkeyColumns, getRowkeyQualifiersString}
import github.avinoamn.HSCWrapper.utils.Consts.ROWKEY_COLUMN_FAMILY
import org.apache.hadoop.hbase.spark.datasources.HBaseTableCatalog
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object HSCWrapper {
  val HSC_FORMAT = "org.apache.hadoop.hbase.spark"

  /**
   * Wrapper to `read` function to support it with `HBTable` (with the default `namespace` value),
   * and with an array of HBase columns names.
   *
   * @param tableName HBase table name
   * @param columns Array of HBase column names
   * @param spark implicit `SparkSession`
   * @return Spark `DataFrame` of the Read HBase Table
   */
  def read(tableName: String, columns: Array[String])(implicit spark: SparkSession): DataFrame = {
    val table = HBTable(tableName)
    read(table, columns)
  }

  /**
   * Wrapper to `read` function to support it with `HBTable`,
   * and with an array of `HBColumn`s (with their default `dataType` value).
   *
   * @param table `HBTable`
   * @param columns Array of HBase column names
   * @param spark implicit `SparkSession`
   * @return Spark `DataFrame` of the Read HBase Table
   */
  def read(table: HBTable, columns: Array[String])(implicit spark: SparkSession): DataFrame = {
    val hbColumns = getHBColumns(columns)
    read(table, hbColumns)
  }

  /**
   * Wrapper to `read` function to support it with catalog (built with given `HBTable` and
   * array of `HBColumn`s).
   *
   * @param table `HBTable`
   * @param hbColumns Array of `HBColumn`
   * @param spark implicit `SparkSession`
   * @return Spark `DataFrame` of the Read HBase Table
   */
  def read(table: HBTable, hbColumns: Array[HBColumn])(implicit spark: SparkSession): DataFrame = {
    val catalog = buildCatalog(table, hbColumns)
    read(catalog)
  }

  /**
   * Read HBase table content into a Spark DataFrame.
   *
   * @param catalog HBase table catalog
   * @param spark implicit `SparkSession`
   * @return Spark `DataFrame` of the Read HBase Table
   */
  def read(catalog: String)(implicit spark: SparkSession): DataFrame = {
    spark
      .read
      .format(HSC_FORMAT)
      .options(Map(HBaseTableCatalog.tableCatalog -> catalog))
      .load()
  }

  /**
   * Wrapper to `write` function to support it with Spark `Dataset`, `HBTable` (with the default `namespace` value)
   * and an Array of HBase column names.
   *
   * @param ds Spark Dataset to write to HBase
   * @param tableName HBase table name
   * @param columns Array of HBase column names (needs to be the same order as the `Dataset` columns)
   * @param spark implicit `SparkSession`
   */
  def write[T](ds: Dataset[T], tableName: String, columns: Array[String])(implicit spark: SparkSession): Unit = {
    val table = HBTable(tableName)
    write(ds, table, columns)
  }

  /**
   * Wrapper to `write` function to support it with Spark `DataFrame`, `HBTable` and an array of `HBColumn`s.
   *
   * @param ds Spark `Dataset` to write to HBase
   * @param table `HBTable`
   * @param columns Array of HBase column names (needs to be the same order as the `Dataset` columns)
   * @param spark implicit `SparkSession`
   */
  def write[T](ds: Dataset[T], table: HBTable, columns: Array[String])(implicit spark: SparkSession): Unit = {
    val df = ds.toDF(columns: _*)
    val hbColumns = getHBColumns(df.schema.fields)

    write(df, table, hbColumns)
  }

  /**
   * Wrapper to `write` function to support it with Spark `DataFrame`, catalog (built
   * with given `HBTable` and an array of `HBColumn`s), and `newTableRegionsNumber`.
   *
   * @param df Spark `Dataset` to write to HBase
   * @param table `HBTable`
   * @param hbColumns Array of `HBColumn`
   * @param spark implicit `SparkSession`
   */
  def write(df: DataFrame, table: HBTable, hbColumns: Array[HBColumn])(implicit spark: SparkSession): Unit = {
    val droppedNullRowsDf = dropNullRows(df, hbColumns)

    val catalog = buildCatalog(table, hbColumns)
    write(droppedNullRowsDf, catalog, table.newTableRegionsNumber)
  }

  /**
   * Write Spark `DataFrame` content to an HBase table.
   *
   * @param df Spark `DataFrame` to write to HBase
   * @param catalog HBase table catalog fitting the `DataFrame`'s schema
   * @param newTableRegionsNumber If defined and larger than 3, a new table will be created with the nubmer of region specified
   * @param spark implicit `SparkSession`
   */
  def write(df: DataFrame, catalog: String, newTableRegionsNumber: String)(implicit spark: SparkSession): Unit = {
    df
      .write
      .format(HSC_FORMAT)
      .options(Map(HBaseTableCatalog.tableCatalog -> catalog, HBaseTableCatalog.newTable -> newTableRegionsNumber))
      .save()
  }

  /**
   * Build a catalog fitting given HBase table and columns.
   *
   * @param table `HBTable`
   * @param hbColumns Array of `HBColumn`
   * @return HBase table catalog
   */
  def buildCatalog(table: HBTable, hbColumns: Array[HBColumn]): String = {
    val rowkeyHBColumns = getRowkeyColumns(hbColumns)
    val rowkeyQualifiersString = getRowkeyQualifiersString(rowkeyHBColumns)

    if (rowkeyHBColumns.nonEmpty) {
      val catalogTable = s""""table":{"namespace":"${table.namespace}", "name":"${table.name}"}"""

      val catalogRowkey = s""""${ROWKEY_COLUMN_FAMILY}":"${rowkeyQualifiersString}""""

      val catalogColumns = hbColumns.map(hbColumn =>
        s""""${hbColumn.columnName}":{"cf":"${hbColumn.columnFamily}", "col":"${hbColumn.columnQualifier}", "type":"${hbColumn.columnType}"}""")

      val catalog =
        s"""{
           |  ${catalogTable},
           |  ${catalogRowkey},
           |  "columns":{
           |    ${catalogColumns.mkString(",\n\t")}
           |  }
           |}""".stripMargin

      catalog
    } else {
      throw new Exception("HBase 'rowkey' column is not defined.")
    }
  }
}
