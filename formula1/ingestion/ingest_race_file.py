# Databricks notebook source
dbutils.widgets.text('p_data_source',"")
v_data_source=dbutils.widgets.get('p_data_source')

# COMMAND ----------

dbutils.widgets.text('p_file_date',"2021-03-21")
v_file_date=dbutils.widgets.get('p_file_date')

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### ingestion of race file

# COMMAND ----------

# MAGIC %md
# MAGIC #### step 1 read race.csv file

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1pdl/raw

# COMMAND ----------

race_df=spark.read.option('header',True).csv(f"{raw_folder_path}/{v_file_date}/races.csv")

# COMMAND ----------

display(race_df)

# COMMAND ----------

race_df.printSchema()

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

race_schema = StructType(fields=[StructField('raceId',IntegerType(),False),
                                 StructField('year',IntegerType(),True),
                                 StructField('round',IntegerType(),True),
                                 StructField('circuitId',IntegerType(),True),
                                 StructField('name',StringType(),True),
                                 StructField('date',DateType(),True),
                                 StructField('time',StringType(),True),
                                 StructField('url',StringType(),True)    
])

# COMMAND ----------

race_df=spark.read.option('header',True)\
.schema(race_schema)\
.csv(f"{raw_folder_path}/{v_file_date}/races.csv")

# COMMAND ----------

race_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC step 2 select the required columns

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

race_selected_df=race_df.select(col('raceId'),col('year'),col('round'),col('circuitId'),col('name'),col('date'),col('time'))

# COMMAND ----------

display(race_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####step 3 rename columns

# COMMAND ----------

race_renamed_df=race_selected_df.withColumnRenamed('raceId','race_id')\
                                .withColumnRenamed('year','race_year')\
                                .withColumnRenamed('circuitId','circuit_id')\
                                .withColumn('data_source',lit(v_data_source))\
                                .withColumn('file_date',lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC step 4  adding new columns

# COMMAND ----------

race_new_col_df=race_renamed_df.withColumn('race_timestamp',to_timestamp(concat(col('date'),lit(''),col('time')),'yyyy-MM-dd HH-mm-ss'))

# COMMAND ----------

race_final_df=race_new_col_df.withColumn('ingestion_date',current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC step 5 write race data to datalake as parquet file.

# COMMAND ----------

race_final_df.write.mode('overwrite').format('parquet').saveAsTable("f1_processed.race")

# COMMAND ----------

df=spark.read.parquet('/mnt/formula1pdl/processed/race')

# COMMAND ----------

display(df)

# COMMAND ----------


