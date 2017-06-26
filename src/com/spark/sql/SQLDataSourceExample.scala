package com.spark.sql

/**
  * Created by yangtong on 17/6/15.
  */
import org.apache.spark.sql.SparkSession

object SQLDataSourceExample {
    
    case class Person(name: String, age: Long)
    
    def main(args: Array[String]) {
        val spark = SparkSession
                .builder()
                .master("local")
                .appName("Spark SQL data sources example")
                //      .config("spark.some.config.option", "some-value")
                .getOrCreate()
        
//        runBasicDataSourceExample(spark)
        //    runBasicParquetExample(spark)
            runParquetSchemaMergingExample(spark)
        //    runJsonDatasetExample(spark)
        //    runJdbcDatasetExample(spark)
        
        spark.stop()
    }
    
    private def runBasicDataSourceExample(spark: SparkSession): Unit = {
        // $example on:generic_load_save_functions$
        val usersDF = spark.read.load("resources/users.parquet")
        usersDF.show()
        usersDF.select("name", "favorite_color").write.save("namesAndFavColors.parquet")
        val namesAndFavColorsParquet = spark.read.parquet("namesAndFavColors.parquet")
        namesAndFavColorsParquet.show()
        // $example off:generic_load_save_functions$
        // $example on:manual_load_options$
        val peopleDF = spark.read.format("json").load("resources/people.json")
        peopleDF.select("name", "age").write.format("parquet").save("namesAndAges.parquet")
        // $example off:manual_load_options$
        // $example on:direct_sql$
        val sqlDF = spark.sql("SELECT * FROM parquet.`resources/users.parquet`")
        // $example off:direct_sql$
    }
    
    private def runBasicParquetExample(spark: SparkSession): Unit = {
        // $example on:basic_parquet_example$
        // Encoders for most common types are automatically provided by importing spark.implicits._
        import spark.implicits._
        
        val peopleDF = spark.read.json("resources/people.json")
        
        // DataFrames can be saved as Parquet files, maintaining the schema information
        peopleDF.write.parquet("people.parquet")
        
        // Read in the parquet file created above
        // Parquet files are self-describing so the schema is preserved
        // The result of loading a Parquet file is also a DataFrame
        val parquetFileDF = spark.read.parquet("people.parquet")
        
        parquetFileDF.show()
        
        // Parquet files can also be used to create a temporary view and then used in SQL statements
        parquetFileDF.createOrReplaceTempView("parquetFile")
        val namesDF = spark.sql("SELECT name FROM parquetFile WHERE age BETWEEN 13 AND 19")
        namesDF.map(attributes => "Name: " + attributes(0)).show()
        // +------------+
        // |       value|
        // +------------+
        // |Name: Justin|
        // +------------+
        // $example off:basic_parquet_example$
    }
    
    private def runParquetSchemaMergingExample(spark: SparkSession): Unit = {
        // $example on:schema_merging$
        // This is used to implicitly convert an RDD to a DataFrame.
        import spark.implicits._
        
        // Create a simple DataFrame, store into a partition directory
        val squaresDF = spark.sparkContext.makeRDD(1 to 5).map(i => (i, i * i)).toDF("value", "square")
        squaresDF.write.parquet("./data/sql/test_table/key=1")
        
        // Create another DataFrame in a new partition directory,
        // adding a new column and dropping an existing column
        val cubesDF = spark.sparkContext.makeRDD(6 to 10).map(i => (i, i * i * i)).toDF("value", "cube")
        cubesDF.write.parquet("./data/sql/test_table/key=2")
        
        // Read the partitioned table
        val mergedDF = spark.read.option("mergeSchema", "true").parquet("data/test_table")
        mergedDF.printSchema()
        mergedDF.show()
        
        // The final schema consists of all 3 columns in the Parquet files together
        // with the partitioning column appeared in the partition directory paths
        // root
        //  |-- value: int (nullable = true)
        //  |-- square: int (nullable = true)
        //  |-- cube: int (nullable = true)
        //  |-- key: int (nullable = true)
        // $example off:schema_merging$
    }
    
    private def runJsonDatasetExample(spark: SparkSession): Unit = {
        // $example on:json_dataset$
        // A JSON dataset is pointed to by path.
        // The path can be either a single text file or a directory storing text files
        val path = "resources/people.json"
        val peopleDF = spark.read.json(path)
        // The inferred schema can be visualized using the printSchema() method
        peopleDF.printSchema()
        // root
        //  |-- age: long (nullable = true)
        //  |-- name: string (nullable = true)
        
        // Creates a temporary view using the DataFrame
        peopleDF.createOrReplaceTempView("people")
        
        // SQL statements can be run by using the sql methods provided by spark
        val teenagerNamesDF = spark.sql("SELECT name FROM people WHERE age BETWEEN 13 AND 19")
        teenagerNamesDF.show()
        // +------+
        // |  name|
        // +------+
        // |Justin|
        // +------+
        
        // Alternatively, a DataFrame can be created for a JSON dataset represented by
        // an RDD[String] storing one JSON object per string
        val otherPeopleRDD = spark.sparkContext.makeRDD(
            """{"name":"Yin","address":{"city":"Columbus","state":"Ohio"}}""" :: Nil)
        val otherPeople = spark.read.json(otherPeopleRDD)
        otherPeople.show()
        // +---------------+----+
        // |        address|name|
        // +---------------+----+
        // |[Columbus,Ohio]| Yin|
        // +---------------+----+
        // $example off:json_dataset$
    }
    
    private def runJdbcDatasetExample(spark: SparkSession): Unit = {
        // $example on:jdbc_dataset$
        val jdbcDF = spark.read
                .format("jdbc")
                .option("url", "jdbc:postgresql:dbserver")
                .option("dbtable", "schema.tablename")
                .option("user", "username")
                .option("password", "password")
                .load()
        // $example off:jdbc_dataset$
    }
}