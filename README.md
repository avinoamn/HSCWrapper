# HSCWrapper
A wrapper for the [Apache HBase Spark Connector](https://github.com/apache/hbase-connectors/tree/master/spark) 

## Spark, Scala and Configurable Options
To generate an artifact for a different [Spark version](https://mvnrepository.com/artifact/org.apache.spark/spark-core) and/or [Scala version](https://www.scala-lang.org/download/all.html),
[Hadoop version](https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-core), or [HBase version](https://mvnrepository.com/artifact/org.apache.hbase/hbase), pass command-line options as follows (changing version numbers appropriately):

```
$ mvn -Dspark.version=3.1.2 -Dscala.version=2.12.13 -Dhadoop.version=3.2.1 -Dscala.binary.version=2.12 -Dhbase.version=2.4.13 -Dhbase-connectors.version={installed version} clean install
```

Note: to build the wrapper with the right connector version, follow [this README](https://github.com/apache/hbase-connectors/blob/master/spark/README.md) to install the connector correctly.   

Note: More HBase Spark configuration are defined [here](https://github.com/apache/hbase-connectors/blob/master/spark/hbase-spark/src/main/scala/org/apache/hadoop/hbase/spark/datasources/HBaseSparkConf.scala).

## Application Usage
The following illustrates the basic procedure on how to use the connector.   

### Define the HBase columns and table

    import github.avinoamn.HSCWrapper.models.{HBColumn, HBTable}
    import org.apache.spark.sql.types._


    val mapType: DataType = MapType(IntegerType, new StructType().add(StructField("varchar", StringType)))
    val arrayType: DataType = ArrayType(new StructType().add(StructField("int", IntegerType)))

    val table = HBTable("htable") // `namespace` defaults to "default"
                                  // `newTableRegionsNumber` defaults to 5 and ignored if the table already exists

    val columns = Array(
        HBColumn("rowkey", "ROWKEY"), // `dataType` defaults to StringType
        HBColumn("cf1:col1", "BINARY_FIELD", BinaryType),
        HBColumn("cf1:col2", "TIMESTAMP_FIELD", TimestampType),
        HBColumn("cf1:col3", "DOUBLE_FIELD", DoubleType),
        HBColumn("cf1:col4", "MAP_FIELD", mapType),
        HBColumn("cf1:col5", "ARRAY_FIELD", arrayType),
        HBColumn("cf1:col6", "DATE_FIELD", DateType))

The above defines a schema for a HBase table with name as htable, row key as key and a number of columns (col1-col6).
And it's equal to the following Catalog:

    {
        "table":{"namespace":"default", "name":"htable"},
        "rowkey":"key",
        "columns":{
            "ROWKEY":{"cf":"rowkey", "col":"key", "type":"string"},
            "BINARY_FIELD":{"cf":"cf1", "col":"col1", "type":"binary"},
            "TIMESTAMP_FIELD":{"cf":"cf1", "col":"col2", "type":"timestamp"},
            "DOUBLE_FIELD":{"cf":"cf1", "col":"col3", "type":"double"},
            "MAP_FIELD":{"cf":"cf1", "col":"col4", "type":"map<int,struct<varchar:string>>"},
            "ARRAY_FIELD":{"cf":"cf1", "col":"col5", "type":"array<struct<int:int>>"},
            "DATE_FIELD":{"cf":"cf1", "col":"col6", "type":"date"}
        }
    }

Note: Composite Rowkey is also supported. For example - `HBColumn("rowkey:key1", "KEY_1")`

### Write to HBase table to populate data

    import github.avinoamn.HSCWrapper.write

    write(sc.parallelize(data).toDF, table, columns)

Given a DataFrame with specified schema, above will create (if not exists) an HBase table with 5 regions and save the DataFrame inside.
And it's equal to the following Code:

    sc.parallelize(data).toDF.write.options(
      Map(HBaseTableCatalog.tableCatalog -> catalog, HBaseTableCatalog.newTable -> "5"))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()

### Perform DataFrame operation on top of HBase table

    import github.avinoamn.HSCWrapper.read

    // Load the dataframe
    val df = read(table, columns)

Equal to:

    val df = sqlContext
      .read
      .options(Map(HBaseTableCatalog.tableCatalog->cat))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
    

### Complicated query

    val s = df.filter((($"col0" <= "row050" && $"col0" > "row040") ||
      $"ROWKEY" === "row005" ||
      $"ROWKEY" === "row020" ||
      $"ROWKEY" ===  "r20" ||
      $"ROWKEY" <= "row005") &&
      ($"DOUBLE_FIELD" === 1.0 ||
      $"DOUBLE_FIELD" === 42.0))
      .select("ROWKEY", "BINARY_FIELD", "DOUBLE_FIELD")
    s.show

### SQL support

    df.createOrReplaceTempView("table")
    sqlContext.sql("select count(BINARY_FIELD) from table").show
