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

qualifying_schema=StructType(fields=[StructField('qualifyId',IntegerType(),False),
                                     StructField('raceId',IntegerType(),True),
                                     StructField('driverId',IntegerType(),True),
                                     StructField('constructorId',IntegerType(),True),
                                     StructField('number',IntegerType(),True),
                                     StructField('position',IntegerType(),True),
                                     StructField('q1',StringType(),True),
                                     StructField('q2',StringType(),True),
                                     StructField('q3',StringType(),True)
                         
                                    ])

# COMMAND ----------

qualifying_df=spark.read\
.schema(qualifying_schema)\
.option('multiline',True)\
.json(f"{raw_folder_path}/{v_file_date}/qualifying")

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

qualifying_renamed_df=qualifying_df.withColumnRenamed('qualifyId','qualify_id')\
                                   .withColumnRenamed('raceId','race_id')\
                                   .withColumnRenamed('driverId','driver_id')\
                                   .withColumnRenamed('constructorId','constructor_id')\
                                  

# COMMAND ----------

qualifying_final_df=add_ingestion_date(qualifying_renamed_df)

# COMMAND ----------

overwrite_partition(qualifying_final_df,'f1_processed','qualifying','race_id')

# COMMAND ----------

# qualifying_final_df.write.mode('overwrite').format('parquet').saveAsTable("f1_processed.qualifying")

# COMMAND ----------

# display(spark.read.parquet('/mnt/formula1pdl/processed/qualifying'))

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id, count(1) from f1_processed.qualifying
# MAGIC group by race_id
# MAGIC order by race_id desc limit 3;

# COMMAND ----------


