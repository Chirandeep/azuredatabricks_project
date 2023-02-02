# Databricks notebook source
dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date=dbutils.widgets.get("p_file_date")

# COMMAND ----------

v_file_date

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

races_df=spark.read.parquet(f"{processed_folder_path}/race")\
.withColumnRenamed('name','race_name')\
.withColumnRenamed('race_timestamp','race_date')

# COMMAND ----------

#display(races_df)

# COMMAND ----------

#results_df=spark.read.parquet(f"{processed_folder_path}/results")

# COMMAND ----------

results_df=spark.read.parquet(f"{processed_folder_path}/results")\
.where(f"file_date ='{v_file_date}'")\
.withColumnRenamed('time','race_time')\
.withColumnRenamed('race_id','results_race_id')

# COMMAND ----------

display(results_df)

# COMMAND ----------

`constructors_df=spark.read.parquet(f"{processed_folder_path}/constructor")\
.withColumnRenamed('name','team')

# COMMAND ----------

drivers_df=spark.read.parquet(f"{processed_folder_path}/driver")\
.withColumnRenamed('number','driver_number')\
.withColumnRenamed('name','driver_name')\
.withColumnRenamed('nationality','driver_nationality')

# COMMAND ----------

circuits_df=spark.read.parquet(f"{processed_folder_path}/circuits")\
.withColumnRenamed('location','circuit_location')

# COMMAND ----------

circuits_races_df=circuits_df.join(races_df,circuits_df.circuit_id==races_df.circuit_id)

# COMMAND ----------

race_results_df=results_df.join(circuits_races_df,circuits_races_df.race_id==results_df.results_race_id)\
                          .join(constructors_df,constructors_df.constructor_id==results_df.constructor_id)\
                          .join(drivers_df,drivers_df.driver_id==results_df.driver_id)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df=race_results_df.select('race_id','race_year','race_name','race_date','circuit_location','driver_name','driver_number','driver_nationality','team','grid','fastest_lap','race_time','points','position')\
.withColumn('created_date',current_timestamp())

# COMMAND ----------

display(final_df)

# COMMAND ----------

#display(final_df.filter('race_year==2020 and race_name=="Abu Dhabi Grand Prix"').orderBy(final_df.points.desc()))

# COMMAND ----------

# final_df.write.mode('overwrite').format('parquet').saveAsTable('f1_presentation.race_results')

# COMMAND ----------

overwrite_partition(final_df,'f1_presentation','race_results','race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_presentation.race_results;

# COMMAND ----------

#display(spark.read.parquet(f"{presentation_folder_path}/race_results"))

# COMMAND ----------

#%fs 
#ls /mnt/formula1pdl

# COMMAND ----------


