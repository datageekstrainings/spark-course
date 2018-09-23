package com.datageeks.sparkstreaming.structuredstreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
import java.sql.Timestamp
import org.apache.spark.sql.functions._

object Windowing {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "c://hadoop//")
    val sparkSession = SparkSession.builder().appName("Strcutred Streaming").master("local[4]").getOrCreate()
    //create stream from socket

    val socketStreamDf = sparkSession.readStream
      .format("socket")
      .option("host", "35.231.123.251")
      .option("port", 50050)
      .load()
    import sparkSession.implicits._
    val socketDs = socketStreamDf.as[(String, Timestamp)]
    val wordsDs = socketDs
      .flatMap(line => line._1.split(" ").map(word => {
        Thread.sleep(15000)
        (word, line._2)
      }))
      .toDF("word", "timestamp")

    val windowedCount = wordsDs.withWatermark("timestamp", "500 milliseconds")
      .groupBy(
        window($"timestamp", "15 seconds"))
      .count()
      .orderBy("window")

    val query =
      windowedCount.writeStream
        .format("console").option("truncate", "false")
        .outputMode(OutputMode.Complete()).start()

    query.awaitTermination()

  }
}