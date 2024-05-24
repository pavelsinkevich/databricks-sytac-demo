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
  
val eventhubs = spark.readStream
  .format("eventhubs")
  .options(eventHubsConf.toMap)
  .load()

// COMMAND ----------

display(eventhubs)

// COMMAND ----------

// split lines by whitespaces and explode the array as rows of 'word'
val df = eventhubs.select($"body".cast("string"))

display(df)

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
val dfWithParsedJson = df.withColumn("parsed_body", from_json(col("body"), schema))

// Select individual fields from parsed JSON
val finalDf = dfWithParsedJson.select(
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

display(finalDf)

// COMMAND ----------

finalDf.writeStream
  .outputMode("append")
  .format("delta") // Use appropriate format for your table, e.g., "hive" for Hive tables
  .option("checkpointLocation", "abfss://checkpoint@sytacdemo.dfs.core.windows.net/") // Checkpoint directory for fault tolerance
  .toTable("streaming_bronze.Taxi")
