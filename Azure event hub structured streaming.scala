// Databricks notebook source
import org.apache.spark.eventhubs.{ ConnectionStringBuilder, EventHubsConf, EventPosition }
import org.apache.spark.sql.functions.{ explode, split }

// To connect to an Event Hub, EntityPath is required as part of the connection string.
// Here, we assume that the connection string from the Azure portal does not have the EntityPath part.
val EH_CONN_STR= dbutils.secrets.get(scope="key-vault-secret", key="EventHubConnectionString")
val connectionString = ConnectionStringBuilder(EH_CONN_STR)
  .setEventHubName("sytac-demo-event-hub")
  .build
val eventHubsConf = EventHubsConf(connectionString)
 // .setStartingPosition(EventPosition.fromEndOfStream)
  
val eventhubs_df = spark.readStream
  .format("eventhubs")
  .options(eventHubsConf.toMap)
  .load()

// COMMAND ----------

display(eventhubs_df)

// COMMAND ----------

val json_df = eventhubs_df.select($"body".cast("string"))

display(json_df)

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
// Define schema
val schema = new StructType()
  .add("vendorID", StringType, true)
  .add("tpepPickupDateTime", LongType, true)
  .add("tpepDropoffDateTime", LongType, true)
  .add("passengerCount", IntegerType, true)
  .add("tripDistance", DoubleType, true)
  .add("puLocationId", StringType, true)
  .add("doLocationId", StringType, true)
  .add("startLon", DoubleType, true)
  .add("startLat", DoubleType, true)
  .add("endLon", DoubleType, true)
  .add("endLat", DoubleType, true)
  .add("rateCodeId", IntegerType, true)
  .add("storeAndFwdFlag", StringType, true)
  .add("paymentType", StringType, true)
  .add("fareAmount", DoubleType, true)
  .add("extra", DoubleType, true)
  .add("mtaTax", DoubleType, true)
  .add("improvementSurcharge", StringType, true)
  .add("tipAmount", DoubleType, true)
  .add("tollsAmount", DoubleType, true)
  .add("totalAmount", DoubleType, true)

// Parse JSON and create new columns
val parsed_json_df = json_df.withColumn("parsed_body", from_json(col("body"), schema))

// Select individual fields from parsed JSON
val bronze_Df = parsed_json_df.select(
  col("parsed_body.vendorID").alias("vendorID"),
  col("parsed_body.tpepPickupDateTime").alias("tpepPickupDateTime"),
  col("parsed_body.tpepDropoffDateTime").alias("tpepDropoffDateTime"),
  col("parsed_body.passengerCount").alias("passengerCount"),
  col("parsed_body.tripDistance").alias("tripDistance"),
  // col("parsed_body.puLocationId").alias("puLocationId"),
  // col("parsed_body.doLocationId").alias("doLocationId"),
  // col("parsed_body.startLon").alias("startLon"),
  // col("parsed_body.startLat").alias("startLat"),
  // col("parsed_body.endLon").alias("endLon"),
  // col("parsed_body.endLat").alias("endLat"),
  // col("parsed_body.rateCodeId").alias("rateCodeId"),
  // col("parsed_body.storeAndFwdFlag").alias("storeAndFwdFlag"),
  // col("parsed_body.paymentType").alias("paymentType"),
  col("parsed_body.fareAmount").alias("fareAmount"),
  // col("parsed_body.extra").alias("extra"),
  // col("parsed_body.mtaTax").alias("mtaTax"),
  // col("parsed_body.improvementSurcharge").alias("improvementSurcharge"),
  // col("parsed_body.tipAmount").alias("tipAmount"),
  // col("parsed_body.tollsAmount").alias("tollsAmount"),
  col("parsed_body.totalAmount").alias("totalAmount")
)

display(bronze_Df)

// COMMAND ----------

bronze_Df.writeStream
  .outputMode("append")
  .format("delta") // Use appropriate format for your table, e.g., "hive" for Hive tables
  .option("checkpointLocation", "abfss://checkpoint@sytacdemo.dfs.core.windows.net/bronze") // Checkpoint directory for fault tolerance
  .toTable("streaming_bronze.Taxi")

// COMMAND ----------

// MAGIC %python
// MAGIC from pyspark.sql.functions import col, current_timestamp, from_json, cast, lit, from_unixtime, to_timestamp, sum, avg, when, count, round, max
// MAGIC from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DoubleType, LongType, TimestampType
// MAGIC
// MAGIC python_bronze_df = spark.readStream.format("delta")\
// MAGIC   .option("ignoreDeletes", "true")\
// MAGIC   .table("streaming_bronze.Taxi")\
// MAGIC   .withColumn("vendor_ID",col("vendorID"))\
// MAGIC   .withColumn("pickup_Datetime", to_timestamp(from_unixtime(col("tpepPickupDateTime")/1000)))\
// MAGIC   .withColumn("dropoff_Datetime", to_timestamp(from_unixtime(col("tpepDropoffDateTime")/1000)))\
// MAGIC   .withColumn("passenger_Count", col("passengerCount"))\
// MAGIC   .withColumn("trip_Distance", col("tripDistance"))\
// MAGIC   .withColumn("fair_Amount", col("fareAmount"))\
// MAGIC   .withColumn("total_Amount_Incl_Tax_Tip_Tolls", col("totalAmount"))\
// MAGIC   .withColumn("ingestion_Date", current_timestamp())\
// MAGIC   .drop("vendorID","tpepPickupDateTime","tpepDropoffDateTime","passengerCount","tripDistance","fareAmount","totalAmount")
// MAGIC   
// MAGIC display(python_bronze_df)
// MAGIC

// COMMAND ----------

// MAGIC %python
// MAGIC (python_bronze_df.writeStream
// MAGIC   .format("delta")
// MAGIC   .outputMode("append")
// MAGIC   .option("checkpointLocation", "abfss://checkpoint@sytacdemo.dfs.core.windows.net/silver/")
// MAGIC   .toTable("streaming_silver.Taxi")
// MAGIC )

// COMMAND ----------

// MAGIC %python
// MAGIC gold_df = spark.readStream.format("delta") \
// MAGIC   .option("ignoreDeletes", "true") \
// MAGIC   .table("streaming_silver.Taxi") \
// MAGIC   .groupBy("vendor_id") \
// MAGIC   .agg(round(sum("total_Amount_Incl_Tax_Tip_Tolls"), 2).alias("Total_amount_per_vendor"), 
// MAGIC        round(sum("trip_Distance"), 2).alias("Total_trip_distance"), 
// MAGIC        sum("passenger_Count").cast('Int').alias("Total_passengers"), 
// MAGIC        max("ingestion_Date").alias("ingestion_Date"))
// MAGIC
// MAGIC display(gold_df)

// COMMAND ----------

// MAGIC %python
// MAGIC (gold_df.writeStream
// MAGIC   .format("delta")
// MAGIC   .outputMode("complete")
// MAGIC   .option("checkpointLocation", "abfss://checkpoint@sytacdemo.dfs.core.windows.net/gold/")
// MAGIC   .toTable("streaming_gold.Taxi")
// MAGIC )
