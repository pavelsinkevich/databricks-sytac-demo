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
