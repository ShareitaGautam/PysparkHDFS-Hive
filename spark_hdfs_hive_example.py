# Import SparkSession from pyspark.
# SparkSession is the main entry point to work with Spark.
from pyspark.sql import SparkSession

if __name__ == "__main__":
    # Create Spark session.
    # enableHiveSupport() is needed because later we will create and query Hive tables.
    spark = SparkSession.builder \
        .appName("Spark HDFS Hive Example") \
        .enableHiveSupport() \
        .getOrCreate()

    # Get SparkContext from SparkSession.
    # SparkContext is used for RDD operations.
    sc = spark.sparkContext


    # ------------------------------------------------------------
    # STEP 1: Read users.txt from HDFS as RDD
    # ------------------------------------------------------------
    # textFile() reads the file line by line from HDFS.
    rdd = sc.textFile("/data/spark/test/users.txt")

    # count() gives total number of lines in the file.
    print("Total records in users.txt:")
    print(rdd.count())

    # collect() brings all data to driver and prints it as a Python list.
    # Use collect() only for small data.
    print("All records from users.txt:")
    print(rdd.collect())


    # ------------------------------------------------------------
    # STEP 2: Read zipcodes1.csv from HDFS as a simple DataFrame
    # ------------------------------------------------------------
    # This reads CSV file without header and without schema inference.
    # Spark will treat all columns as string here.
    df = spark.read.csv("/data/spark/test/zipcodes.csv")

    print("Schema of simple DataFrame:")
    df.printSchema()

    print("Data from simple DataFrame:")
    df.show()


    # ------------------------------------------------------------
    # STEP 3: Write the DataFrame to HDFS in Parquet format
    # ------------------------------------------------------------
    # Parquet is a columnar storage format.
    # It is faster and better for big data processing than normal CSV.
    # mode("overwrite") will replace old data if path already exists.
    df.write.mode("overwrite").parquet("/data/spark/test/parquet/people.parquet")

    print("DataFrame written to parquet format in HDFS.")


    # ------------------------------------------------------------
    # STEP 4: Read parquet file back into a DataFrame
    # ------------------------------------------------------------
    parDF = spark.read.parquet("/data/spark/test/parquet/people.parquet")

    print("Data read from parquet file:")
    parDF.show()


    # ------------------------------------------------------------
    # STEP 5: Read CSV again with header and schema
    # ------------------------------------------------------------
    # header=True means first row is column name.
    # inferSchema=True means Spark will try to detect correct data types.
    # delimiter=',' tells Spark that file is comma separated.
    df3 = spark.read.options(
        header='True',
        inferSchema='True',
        delimiter=','
    ).csv("/data/spark/test/zipcodes.csv")

    print("Schema of CSV with header and inferred schema:")
    df3.printSchema()

    print("Data from CSV with proper columns:")
    df3.show()


    # ------------------------------------------------------------
    # STEP 6: Save DataFrame as Hive table
    # ------------------------------------------------------------
    # This creates or replaces a Hive table named zip_table in default database.
    df3.write.mode("overwrite").saveAsTable("default.zip_table")

    print("Hive table default.zip_table created.")


    # ------------------------------------------------------------
    # STEP 7: Read Hive table using Spark SQL
    # ------------------------------------------------------------
    tabDF = spark.sql("select * from default.zip_table")

    print("Data from Hive table default.zip_table:")
    tabDF.show()


    # ------------------------------------------------------------
    # STEP 8: Enable dynamic partition in Hive
    # ------------------------------------------------------------
    # These settings are needed before writing partitioned Hive tables.
    spark.conf.set("hive.exec.dynamic.partition", "true")
    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")


    # ------------------------------------------------------------
    # STEP 9: Write partitioned Hive table by city
    # ------------------------------------------------------------
    # Data will be partitioned based on city column.
    df3.write.mode("overwrite").partitionBy("city").saveAsTable("default.city_part")

    print("Partitioned Hive table default.city_part created.")


    # ------------------------------------------------------------
    # STEP 10: Write partitioned Hive table by state and city
    # ------------------------------------------------------------
    # Data will be partitioned first by state and then by city.
    df3.write.mode("overwrite").partitionBy("state", "city").saveAsTable("default.state_city")

    print("Partitioned Hive table default.state_city created.")


    # ------------------------------------------------------------
    # STEP 11: Write partitioned Hive table in parquet format
    # ------------------------------------------------------------
    # Here table is stored in parquet format and partitioned by state and city.
    df3.write.mode("overwrite") \
        .partitionBy("state", "city") \
        .format("parquet") \
        .saveAsTable("default.state_city_parquet")

    print("Partitioned parquet Hive table default.state_city_parquet created.")


    # ------------------------------------------------------------
    # STEP 12: Show all tables in default database
    # ------------------------------------------------------------
    print("All tables in default database:")
    spark.sql("show tables in default").show()


    # ------------------------------------------------------------
    # STEP 13: Query only city column where city is ASHEBORO
    # ------------------------------------------------------------
    sdf = spark.sql("""
        select city
        from default.state_city_parquet
        where city = 'ASHEBORO'
    """)

    print("Schema for query: city = ASHEBORO")
    sdf.printSchema()

    print("Result for query: city = ASHEBORO")
    sdf.show()


    # ------------------------------------------------------------
    # STEP 14: Query all columns where city is MESA and state is AZ
    # ------------------------------------------------------------
    sdf = spark.sql("""
        select *
        from default.state_city_parquet
        where city = 'MESA' and state = 'AZ'
    """)

    print("Schema for query: city = MESA and state = AZ")
    sdf.printSchema()

    print("Result for query: city = MESA and state = AZ")
    sdf.show()


