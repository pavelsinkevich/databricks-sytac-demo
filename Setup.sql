-- Databricks notebook source
CREATE SCHEMA IF NOT EXISTS streaming_bronze
MANAGED LOCATION 'abfss://bronze@sytacdemo.dfs.core.windows.net/';

CREATE SCHEMA IF NOT EXISTS streaming_silver
MANAGED LOCATION 'abfss://silver@sytacdemo.dfs.core.windows.net/';

CREATE SCHEMA IF NOT EXISTS streaming_gold
MANAGED LOCATION 'abfss://gold@sytacdemo.dfs.core.windows.net/'

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS streaming_bronze.Taxi
(
vendorID STRING,
tpepPickupDateTime LONG,
tpepDropoffDateTime LONG,
passengerCount INT,
tripDistance DOUBLE,
fareAmount DOUBLE,
totalAmount Double
) using DELTA


-- COMMAND ----------

CREATE TABLE IF NOT EXISTS streaming_silver.Taxi
(
vendor_Id STRING,
pickup_Datetime TIMESTAMP,
dropoff_Datetime TIMESTAMP,
passenger_Count INT,
trip_Distance DOUBLE,
fair_Amount DOUBLE,
total_Amount_Incl_Tax_Tip_Tolls Double,
ingestion_Date TIMESTAMP
) using DELTA

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS streaming_gold.Taxi
(
vendor_Id STRING,
Total_amount_per_vendor Double,
Total_trip_distance DOUBLE,
Total_passengers INT,
ingestion_Date TIMESTAMP
) using DELTA

-- COMMAND ----------

delete from streaming_bronze.Taxi;
delete from streaming_silver.Taxi;
delete from streaming_gold.Taxi;
