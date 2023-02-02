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

lap_times_schema=StructType(fields=[StructField('raceId',IntegerType(),False),
                                    StructField('driverId',IntegerType(),True),
                                    StructField('lap',IntegerType(),True),
                                    StructField('position',IntegerType(),True),
                                    StructField('time',StringType(),True),
                                    StructField('milliseconds',IntegerType(),True)
                                   ])

# COMMAND ----------

lap_times_df=spark.read\
.schema(lap_times_schema)\
.csv(f"{raw_folder_path}/{v_file_date}/lap_times/lap_times_split*.csv")

# COMMAND ----------

lap_times_df.count()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

lap_times_renamed_df=lap_times_df.withColumnRenamed('raceId','race_id')\
                               .withColumnRenamed('driverId','driver_id')\
                               

# COMMAND ----------

lap_times_final_df=add_ingestion_date(lap_times_renamed_df)

# COMMAND ----------

overwrite_partition(lap_times_final_df,'f1_processed','lap_times','race_id')

# COMMAND ----------

# lap_times_final_df.write.mode('overwrite').format('parquet').saveAsTable("f1_processed.lap_times")

# COMMAND ----------

# display(spark.read.parquet(f"{processed_folder_path}/lap_times"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id, count(1) from f1_processed.lap_times
# MAGIC group by race_id
# MAGIC order by race_id desc limit 3;
