-- Databricks notebook source
select * from streaming_bronze.Taxi

-- COMMAND ----------

select * from streaming_silver.Taxi

-- COMMAND ----------

select * from streaming_gold.Taxi
