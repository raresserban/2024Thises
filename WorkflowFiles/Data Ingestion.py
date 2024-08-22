# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import *

accountkey = dbutils.secrets.get(scope = 'landingzone' , key = 'container' )
spark.conf.set("fs.azure.account.key.landingzone6291.blob.core.windows.net" , accountkey)
spark.sql('CREATE DATABASE IF NOT EXISTS ser6db') 
dblocation = 'dbfs:/user/hive/warehouse/ser6db.db'
spark.conf.set("spark.databricks.delta.properties.defaults.enableChangeDataFeed","true")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")


schema = StructType([  
    StructField("timevector", DoubleType(), True),  
    StructField("ax1", DoubleType(), True),  
    StructField("ay1", DoubleType(), True),  
    StructField("az1", DoubleType(), True),  
    StructField("gx1", DoubleType(), True),  
    StructField("gy1", DoubleType(), True),  
    StructField("gz1", DoubleType(), True),  
    StructField("qx1", DoubleType(), True),  
    StructField("qy1", DoubleType(), True),  
    StructField("qz1", DoubleType(), True),  
    StructField("qw1", DoubleType(), True),  
    StructField("wax1", DoubleType(), True),  
    StructField("way1", DoubleType(), True),  
    StructField("waz1", DoubleType(), True),  
    StructField("wgx1", DoubleType(), True),  
    StructField("wgy1", DoubleType(), True),  
    StructField("wgz1", DoubleType(), True),  
    StructField("temp1", DoubleType(), True),  
    StructField("ax2", DoubleType(), True),  
    StructField("ay2", DoubleType(), True),  
    StructField("az2", DoubleType(), True),  
    StructField("gx2", DoubleType(), True),  
    StructField("gy2", DoubleType(), True),  
    StructField("gz2", DoubleType(), True),  
    StructField("qx2", DoubleType(), True),  
    StructField("qy2", DoubleType(), True),  
    StructField("qz2", DoubleType(), True),  
    StructField("qw2", DoubleType(), True),  
    StructField("wax2", DoubleType(), True),  
    StructField("way2", DoubleType(), True),  
    StructField("waz2", DoubleType(), True),  
    StructField("wgx2", DoubleType(), True),  
    StructField("wgy2", DoubleType(), True),  
    StructField("wgz2", DoubleType(), True),  
    StructField("temp2", DoubleType(), True),  
    StructField("button1", IntegerType(), True),  
    StructField("button2", IntegerType(), True),  
    StructField("button3", IntegerType(), True),  
    StructField("_rescued_data", StringType(), True),  
    StructField("filename", StringType(), True)  
])  


# COMMAND ----------

query = (
    spark.readStream.format("cloudfiles")
    .option("cloudFiles.format", "csv")
    .option(
        "cloudFiles.schemaLocation",
        "wasbs://bloblandingzone@landingzone6291.blob.core.windows.net/metadata/bronze/schema",
    )
    .option("cloudFiles.maxBytesPerTrigger", 10 * 1024 * 1024 * 1024)
    .option("header", "true")
    .option("delimiter", ",")
    .schema(schema)
    .load("wasbs://bloblandingzone@landingzone6291.blob.core.windows.net/data/")
    .withColumn("filename", F.input_file_name())
)

streaming_query = (
    query.writeStream.option(
        "checkpointLocation",
        "wasbs://bloblandingzone@landingzone6291.blob.core.windows.net/metadata/bronze/checkpoint",
    )
    .partitionBy("filename")
    .trigger(availableNow=True)
    .table("ser6db.bronze_table")
)

streaming_query.awaitTermination()

