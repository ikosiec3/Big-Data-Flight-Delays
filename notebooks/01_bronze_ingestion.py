# Databricks notebook source
display(spark.sql("SHOW CATALOGS"))
display(spark.sql("SHOW SCHEMAS IN workspace"))


# COMMAND ----------

display(dbutils.fs.ls("/Volumes/workspace/default/ontime_raw/"))


# COMMAND ----------

raw_path = "/Volumes/workspace/default/ontime_raw/ontime_data"

df_raw = (
    spark.read
         .option("header", True)
         .option("recursiveFileLookup", "true")
         .option("pathGlobFilter", "*.csv")
         .option("inferSchema", "true")
         .csv(raw_path)
)

print("Rows:", df_raw.count())
df_raw.printSchema()



# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col

df_bronze = (
    df_raw
    .withColumn("_source_file", col("_metadata.file_path"))
    .withColumn("_ingestion_ts", current_timestamp())
)

df_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("bronze.ontime_raw")


# COMMAND ----------

spark.table("bronze.ontime_raw").count()
display(spark.table("bronze.ontime_raw").limit(5))


# COMMAND ----------

