# Databricks notebook source
from pyspark.sql import functions as F

df = spark.table("silver.ontime_clean")



# COMMAND ----------

gold_dow = (
    df.groupBy("day_of_week")
      .agg(
          F.count("*").alias("flights"),
          F.avg("arr_delay_15").alias("delay_probability")
      )
)

gold_dow.write.format("delta").mode("overwrite").saveAsTable("gold.delay_by_dayofweek")


# COMMAND ----------

gold_hour = (
    df.where(F.col("dep_hour").isNotNull())
      .groupBy("dep_hour")
      .agg(
          F.count("*").alias("flights"),
          F.avg("arr_delay_15").alias("delay_probability")
      )
)

gold_hour.write.format("delta").mode("overwrite").saveAsTable("gold.delay_by_hour")


# COMMAND ----------

gold_origin = (
    df.groupBy("ORIGIN", "ORIGIN_CITY_NAME", "ORIGIN_STATE_NM")
      .agg(
          F.count("*").alias("flights"),
          F.avg("arr_delay_15").alias("delay_probability")
      )
)

gold_origin.write.format("delta").mode("overwrite").saveAsTable("gold.delay_by_origin")


# COMMAND ----------

daily_stats = (
    df.groupBy("flight_date")
      .agg(
          F.count("*").alias("flights"),
          F.avg("arr_delay_15").alias("delay_probability")
      )
      .where(F.col("flights") >= 1000)  
)


# COMMAND ----------

baseline = daily_stats.agg(
    F.avg("delay_probability").alias("global_avg_delay")
).collect()[0]["global_avg_delay"]

baseline


# COMMAND ----------

threshold = daily_stats.approxQuantile(
    "delay_probability", [0.85], 0.01
)[0]

high_risk_days = (
    daily_stats
    .withColumn(
        "risk_flag",
        (F.col("delay_probability") >= threshold).cast("int")
    )
)



# COMMAND ----------

high_risk_days.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold.delay_by_date")


# COMMAND ----------

weekly_stats = (
    df
    .withColumn("year", F.year("flight_date"))
    .withColumn("week", F.weekofyear("flight_date"))
    .withColumn(
        "year_week",
        F.concat_ws("-", F.col("year"), F.lpad(F.col("week"), 2, "0"))
    )
    .groupBy("year_week")
    .agg(
        F.count("*").alias("flights"),
        F.avg("arr_delay_15").alias("delay_probability")
    )
    .where(F.col("flights") >= 7000)
)

# COMMAND ----------

weekly_threshold = weekly_stats.approxQuantile(
    "delay_probability", [0.85], 0.01
)[0]


# COMMAND ----------

weekly_risk = (
    weekly_stats
    .withColumn(
        "risk_flag",
        (F.col("delay_probability") >= weekly_threshold).cast("int")
    )
)


# COMMAND ----------

weekly_risk.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold.delay_by_week")


# COMMAND ----------

weekly_risk.agg(
    F.avg("risk_flag").alias("share_flagged")
).show()

# COMMAND ----------

spark.sql("""
CREATE VOLUME IF NOT EXISTS workspace.default.exports
""")


# COMMAND ----------

export_base = "/Volumes/workspace/default/exports"


# COMMAND ----------

spark.table("gold.delay_by_origin") \
    .coalesce(1) \
    .write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(f"{export_base}/delay_by_origin")


# COMMAND ----------

tables = [
    "delay_by_hour",
    "delay_by_dayofweek",
    "delay_by_date",
    "delay_by_week"
]

for t in tables:
    spark.table(f"gold.{t}") \
        .coalesce(1) \
        .write \
        .mode("overwrite") \
        .option("header", True) \
        .csv(f"{export_base}/{t}")



# COMMAND ----------

display(dbutils.fs.ls(f"{export_base}/delay_by_origin"))


# COMMAND ----------


delay_col_candidates = [
    "ARR_DELAY_NEW", "arr_delay_new",
    "ARR_DELAY", "arr_delay",
    "arr_delay_min", "arr_delay_minutes"
]

delay_col = next((c for c in delay_col_candidates if c in df.columns), None)
if delay_col is None:
    raise Exception(
        "Nie znalazłam kolumny z minutami opóźnienia (np. ARR_DELAY_NEW). "
        "Sprawdź schema silver.ontime_clean i dodaj nazwę do delay_col_candidates."
    )


df60 = df

if "CANCELLED" in df.columns:
    df60 = df60.where(F.expr("try_cast(CANCELLED as double)") == 0.0)

df60 = df60.withColumn("delay_min", F.expr(f"try_cast({delay_col} as double)"))

df60 = df60.withColumn(
    "is_arr_delay_60",
    (F.coalesce(F.col("delay_min"), F.lit(0.0)) >= 60.0).cast("int")
)



high_delay_days_60m = (
    df60.groupBy("flight_date")
        .agg(
            F.count("*").alias("flights"),
            F.sum("is_arr_delay_60").alias("flights_delay_60"),
            (F.sum("is_arr_delay_60") / F.count("*")).alias("share_delay_60"),
            F.avg(F.when(F.col("is_arr_delay_60") == 1, F.col("delay_min"))).alias("avg_delay_60_min"),
            F.max("delay_min").alias("max_delay_min"),
        )
        .where(F.col("flights") >= 1000)   
)


thr_60 = high_delay_days_60m.approxQuantile("share_delay_60", [0.85], 0.01)[0]

high_delay_days_60m = high_delay_days_60m.withColumn(
    "risk_flag_60",
    (F.col("share_delay_60") >= F.lit(thr_60)).cast("int")
)


high_delay_days_60m.write.format("delta").mode("overwrite").saveAsTable("gold.high_delay_days_60m")

print(f"✅ Saved: gold.high_delay_days_60m | threshold (top15%) share_delay_60 >= {thr_60}")


# COMMAND ----------

export_base = "/Volumes/workspace/default/exports"

spark.table("gold.high_delay_days_60m") \
    .coalesce(1) \
    .write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(f"{export_base}/high_delay_days_60m")


# COMMAND ----------

