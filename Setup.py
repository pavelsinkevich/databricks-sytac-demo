# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS streaming_bronze
# MAGIC MANAGED LOCATION 'abfss://bronze@sytacdemo.dfs.core.windows.net/';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS streaming_silver
# MAGIC MANAGED LOCATION 'abfss://silver@sytacdemo.dfs.core.windows.net/';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS streaming_gold
# MAGIC MANAGED LOCATION 'abfss://gold@sytacdemo.dfs.core.windows.net/'

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table streaming_bronze.Taxi

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS streaming_bronze.Taxi
# MAGIC (
# MAGIC vendorID STRING,
# MAGIC tpepPickupDateTime LONG,
# MAGIC tpepDropoffDateTime LONG,
# MAGIC passengerCount INT,
# MAGIC tripDistance DOUBLE,
# MAGIC fareAmount DOUBLE,
# MAGIC totalAmount Double
# MAGIC ) using DELTA
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from streaming_bronze.Taxi
