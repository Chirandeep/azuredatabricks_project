# Databricks notebook source
dbutils.widgets.text('p_data_source',"")
v_data_source=dbutils.widgets.get('p_data_source')

# COMMAND ----------

dbutils.widgets.text('p_file_date','2021-03-21')
v_file_date=dbutils.widgets.get('p_file_date')

# COMMAND ----------

v_file_date

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 1.Read the circuits.csv file using spark dataframe reader.

# COMMAND ----------

#dbutils.fs.mounts()

# COMMAND ----------

#%fs
#ls /mnt/formula1pdl/raw

# COMMAND ----------

# MAGIC %md
# MAGIC #circuits_df=spark.read.option('header',True)\
# MAGIC #.option('InferSchema',True)\
# MAGIC .csv('/mnt/formula1pdl/raw/circuits.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC display(circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC circuits_df.describe().show

# COMMAND ----------

# MAGIC %md
# MAGIC circuits_df.printSchema()

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

circuits_schema=StructType(fields=[StructField('circuitId',IntegerType(),False),
                                StructField('circuitRef',StringType(),True),
                                StructField('name',StringType(),True),
                                StructField('location',StringType(),True),
                                StructField('country',StringType(),True),
                                StructField('lat',DoubleType(),True),
                                StructField('lng',DoubleType(),True),
                                StructField('alt',IntegerType(),True),
                                StructField('url',StringType(),True)])
                        

# COMMAND ----------

circuits_df=spark.read.option('header',True)\
.schema(circuits_schema)\
.csv(f"{raw_folder_path}/{v_file_date}/circuits.csv")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #####selecting the required columns

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

circuits_selected_df=circuits_df.select(col('circuitId'),col('circuitRef'),col('name'),col('location'),col('country'),col('lat'),col('lng'),col('alt'))

# COMMAND ----------

display(circuits_selected_df)

# COMMAND ----------

circuits_renamed_df=circuits_selected_df.withColumnRenamed('circuitId','circuit_id')\
                                        .withColumnRenamed('circuitRef','circuit_ref')\
                                        .withColumnRenamed('lat','latitude')\
                                        .withColumnRenamed('lng','longitude')\
                                        .withColumnRenamed('alt','altitude')\
                                        .withColumn('data_source',lit(v_data_source))\
                                        .withColumn('file_date',lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC step4 adding new column ingestion_date

# COMMAND ----------

circuits_final_df=add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC step 5 write circuits.csv to datalake as parquet file

# COMMAND ----------

circuits_final_df.write.mode('overwrite').format('parquet').saveAsTable('f1_processed.circuits')

# COMMAND ----------

# MAGIC %fs

# COMMAND ----------

ls /mnt/formula1pdl/processed/circuits

# COMMAND ----------

df=spark.read.parquet('/mnt/formula1pdl/processed/circuits')

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.circuits;

# COMMAND ----------


