-- Databricks notebook source
USE f1_processed;

-- COMMAND ----------

CREATE TABLE f1_presentation.calculated_race_results
USING parquet
AS
SELECT race.race_year,
constructor.name as team_name,
driver.name as driver_name,
results.position,
results.points,
11-results.position as calculated_points
FROM f1_processed.results
JOIN f1_processed.driver ON results.driver_id=driver.driver_id
JOIN f1_processed.constructor ON results.constructor_id=constructor.constructor_id
JOIN f1_processed.race ON results.race_id=race.race_id
WHERE results.position <=10;

-- COMMAND ----------


