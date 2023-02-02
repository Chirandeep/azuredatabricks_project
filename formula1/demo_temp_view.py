# Databricks notebook source
# MAGIC %run ../includes/configuration"

# COMMAND ----------

race_results_df=spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

race_results_df.createOrReplaceTempView('v_race_results')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from v_race_results;

# COMMAND ----------

race_result_2019_df=spark.sql('select * from v_race_results where race_year=2019')
