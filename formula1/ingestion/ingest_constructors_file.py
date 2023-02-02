# Databricks notebook source
dbutils.widgets.text('p_data_source',"")
v_data_source=dbutils.widgets.get('p_data_source')

# COMMAND ----------

dbutils.widgets.text('p_file_date','2021-03-21')
v_file_date=dbutils.widgets.get('p_file_date')

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType

# COMMAND ----------

constructor_schema=StructType(fields=[StructField('constructorId',IntegerType(),False),
                                      StructField('constructorRef',StringType(),True),
                                      StructField('name',StringType(),True),
                                      StructField('nationality',StringType(),True),
                                      StructField('url',StringType(),True)
                                     
                                     
])

# COMMAND ----------

constructor_df=spark.read\
.schema(constructor_schema)\
.json(f"{raw_folder_path}/{v_file_date}/constructors.json")

# COMMAND ----------

display(constructor_df)


# COMMAND ----------

constructor_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# DBTITLE 1,####step 3- drop unwanted column
# MAGIC %md

# COMMAND ----------

constructor_dropped_df=constructor_df.drop(col('url'))

# COMMAND ----------

# MAGIC %md
# MAGIC ####step 4-rename columns and add ingestion_date with current timestamp

# COMMAND ----------

constructor_renamed_df=constructor_dropped_df.withColumnRenamed('constructorId','constructor_id')\
                                           .withColumnRenamed('constructoRef','constructor_ref')\
                                           .withColumn('data_source',lit(v_data_source))\
                                           .withColumn('file_date',lit(v_file_date))
                                           

# COMMAND ----------

constructor_final_df=add_ingestion_date(constructor_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC step 4 write to parquet file

# COMMAND ----------

constructor_final_df.write.mode('overwrite').format('parquet').saveAsTable("f1_processed.constructor")


# COMMAND ----------

df1=spark.read.parquet(f"{processed_folder_path}/constructor")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1pdl/processed

# COMMAND ----------

display(df1)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.constructor

# COMMAND ----------


