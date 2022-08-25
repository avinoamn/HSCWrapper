package github.avinoamn.HSCWrapper.utils

import github.avinoamn.HSCWrapper.models.HBColumn
import github.avinoamn.HSCWrapper.utils.Consts.{ROWKEY_COLUMN, ROWKEY_COLUMN_FAMILY, ROWKEY_QUALIFIER}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

object ColumnsUtils {
  /*
  * Drops From DataFrame every row that all it's columns (except rowkey column) values
  * are `null` or undefined.
  *
  * We use this function before writing to HBase because it cannot accept rows that all
  * their columns are `null` or undefined.
  * */
  def dropNullRows(df: DataFrame): DataFrame = {
    val notRowkeyColumns = getNotRowkeyColumns(df.columns)
    df.na.drop("all", notRowkeyColumns)
  }

  /*
  * From array of columns names, return array subset of all the columns
  * not matching the HBase "rowkey" column.
  * */
  def getNotRowkeyColumns(columns: Array[String]): Array[String] = {
    columns.filterNot(column => column.equals(ROWKEY_COLUMN) ||  column.startsWith(s"$ROWKEY_COLUMN:"))
  }

  /*
  * From array of `HBColumn`s, return column matching the HBase "rowkey" column.
  * If "rowkey" column does not exist, return `null`.
  * */
  def getRowkeyColumn(hbColumns: Array[HBColumn]): HBColumn = {
    hbColumns.find(_.columnName.equals(ROWKEY_COLUMN)).orNull
  }

  /*
  * Gets an array of columns names, and creates `HBColumn` for each column name in
  * the array with the default Spark type `StringType`.
  * */
  def getHBColumns(columns: Array[String]): Array[HBColumn] = {
    columns.map(column => HBColumn(column))
  }

  /*
  * Gets an array of `StructField`s, and creates `HBColumn` for each `StructField` in the array .
  * */
  def getHBColumns(dfSchemaFields: Array[StructField]): Array[HBColumn] = {
    dfSchemaFields.map(dfSchemaField => HBColumn(dfSchemaField.name, dfSchemaField.dataType))
  }

  /*
  * Extracts HBase column family and qualifier from column name string.
  *
  * Examples:
  *   "rowkey"                => ("rowkey", "key")
  *   "rowkey:key1:key2:key3" => ("rowkey", "key1:key2:key3")
  *   "cf:col"                => ("cf", "col")
  * */
  def getHBColumnFamilyAndQualifier(columnName: String): (String, String) = {
    columnName.split(":").toSeq match {
      case ROWKEY_COLUMN +: Nil => (ROWKEY_COLUMN_FAMILY, ROWKEY_QUALIFIER)
      case ROWKEY_COLUMN_FAMILY +: rowkeyQualifiers => (ROWKEY_COLUMN_FAMILY, rowkeyQualifiers.mkString(":"))
      case columnFamily +: columnQualifier +: Nil => (columnFamily, columnQualifier)
      case _ => throw new Exception(s"Invalid HBase column: '${columnName}'")
    }
  }

  /*
  * Converts Spark Sql Type to it's HBase supported `typeName` value
  *
  * Examples:
  *   BooleanType => "boolean"
  *   StringType  => "string"
  *   IntegerType => "integer"
  *   etc.
  * */
  def getHBColumnType(dataType: DataType): String = {
    dataType match {
      case BooleanType => BooleanType.typeName
      case ByteType => ByteType.typeName
      case ShortType => ShortType.typeName
      case IntegerType => IntegerType.typeName
      case LongType => LongType.typeName
      case FloatType => FloatType.typeName
      case DoubleType => DoubleType.typeName
      case DateType => DateType.typeName
      case TimestampType => TimestampType.typeName
      case StringType => StringType.typeName
      case BinaryType => BinaryType.typeName
      case _ => throw new Exception(s"Unsupported Spark data type '${dataType}'")
    }
  }
}
