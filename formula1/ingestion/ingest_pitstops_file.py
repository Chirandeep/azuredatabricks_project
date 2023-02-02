# Databricks notebook source
dbutils.widgets.text("p_data_source","")
v_data_source=dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-28")
v_file_date=dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

pitstops_schema=StructType(fields=[StructField('raceId',IntegerType(),False),
                                   StructField('driverId',IntegerType(),True),
                                   StructField('stop',IntegerType(),True),
                                   StructField('lap',IntegerType(),True),
                                   StructField('time',StringType(),True),
                                   StructField('duration',StringType(),True),
                                   StructField('miliseconds',IntegerType(),True),
                                  ])

# COMMAND ----------

pitstop_df=spark.read \
.schema(pitstops_schema)\
.option('multiline',True)\
.json(f"{raw_folder_path}/{v_file_date}/pit_stops.json")

# COMMAND ----------

from pyspark.sql.functions import*

# COMMAND ----------

pitstops_renamed_df=pitstop_df.withColumnRenamed('raceId','race_id')\
                   .withColumnRenamed('driverId','driver_id')\
                   


# COMMAND ----------

overwrite_partition(pitstops_renamed_df,'f1_processed','pitstops','race_id')

# COMMAND ----------

pitstops_final_df=add_ingestion_date(pitstops_renamed_df)

# COMMAND ----------

# pitstops_final_df.write.mode('overwrite').format('parquet').saveAsTable("f1_processed.pitstops")

# COMMAND ----------

 display(spark.read.parquet(f"{processed_folder_path}/pitstops"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id, count(1) from f1_processed.pitstops
# MAGIC group by race_id
# MAGIC order by race_id desc limit 3;

# COMMAND ----------


