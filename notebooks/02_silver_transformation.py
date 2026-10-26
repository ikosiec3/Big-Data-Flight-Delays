# Databricks notebook source
from pyspark.sql import functions as F

df = spark.table("bronze.ontime_raw")
print("rows:", df.count())
display(df.limit(5))


# COMMAND ----------

df_silver = (
    df
    # date
    .withColumn("flight_date", F.to_date("FL_DATE", "M/d/yyyy h:mm:ss a"))
    .withColumn("year", F.col("YEAR").cast("int"))
    .withColumn("quarter", F.col("QUARTER").cast("int"))
    .withColumn("month", F.col("MONTH").cast("int"))
    .withColumn("day_of_month", F.col("DAY_OF_MONTH").cast("int"))
    .withColumn("day_of_week", F.col("DAY_OF_WEEK").cast("int"))

    # time
    .withColumn("crs_dep_time", F.col("DEP_TIME").cast("int"))   
    .withColumn("dep_delay_new", F.col("DEP_DELAY_NEW").cast("double"))
    .withColumn("arr_time", F.col("ARR_TIME").cast("int"))
    .withColumn("arr_delay_new", F.col("ARR_DELAY_NEW").cast("double"))
    .withColumn("cancelled", F.col("CANCELLED").cast("int"))
)


# COMMAND ----------


df_silver = df_silver.withColumn(
    "dep_hour",
    (F.col("DEP_TIME").cast("int") / 100).cast("int")
)

df_silver = df_silver.withColumn(
    "dep_hour",
    F.when(
        (F.col("dep_hour") < 0) | (F.col("dep_hour") > 23),
        None
    ).otherwise(F.col("dep_hour"))
)


# COMMAND ----------

df_silver.select("DEP_TIME", "dep_hour").show(20, truncate=False)


# COMMAND ----------

df = spark.table("bronze.ontime_raw")

def try_int(c):
    return F.expr(f"try_cast({c} as int)")

def try_double(c):
    return F.expr(f"try_cast({c} as double)")

df_silver = (
    df
    .withColumn("flight_date", F.to_date("FL_DATE", "M/d/yyyy h:mm:ss a"))

    .withColumn("year",  try_int("YEAR"))
    .withColumn("month", try_int("MONTH"))
    .withColumn("day_of_month", try_int("DAY_OF_MONTH"))
    .withColumn("day_of_week",  try_int("DAY_OF_WEEK"))

    .withColumn("dep_hour", (try_int("DEP_TIME")/100).cast("int"))

    .withColumn("dep_delay_new", try_double("DEP_DELAY_NEW"))
    .withColumn("arr_delay_new", try_double("ARR_DELAY_NEW"))

    .withColumn("dep_delay_15", (F.col("dep_delay_new") > 15).cast("int"))
    .withColumn("arr_delay_15", (F.col("arr_delay_new") > 15).cast("int"))

    .withColumn("is_cancelled", try_int("CANCELLED"))
)

df_silver.write.format("delta").mode("overwrite").saveAsTable("silver.ontime_clean")



# COMMAND ----------

chk = spark.table("silver.ontime_clean")
chk.select(
    F.count("*").alias("rows"),
    F.sum(F.col("year").isNull().cast("int")).alias("year_nulls"),
    F.sum(F.col("month").isNull().cast("int")).alias("month_nulls"),
    F.sum(F.col("dep_hour").isNull().cast("int")).alias("dep_hour_nulls"),
    F.sum(F.col("arr_delay_new").isNull().cast("int")).alias("arr_delay_nulls"),
).show()


# COMMAND ----------

spark.table("silver.ontime_clean").count()

# COMMAND ----------

