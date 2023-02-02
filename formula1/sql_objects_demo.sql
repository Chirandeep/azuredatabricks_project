-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS demo;

-- COMMAND ----------

show databases;

-- COMMAND ----------

describe database demo;

-- COMMAND ----------

describe database extended demo;

-- COMMAND ----------

select current_database();

-- COMMAND ----------

use demo;

-- COMMAND ----------

show tables;

-- COMMAND ----------


