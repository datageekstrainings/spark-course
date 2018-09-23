package com.datageeks.sparkstreaming.structuredstreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.expressions.CurrentTimestamp
import java.sql.Timestamp
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.streaming.Seconds

object ConsoleReadWrite {
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

      val revenueDs=socketStreamDf.as[(String,Timestamp)].toDF("dept","ts")     
      import sparkSession.implicits._
      revenueDs.groupBy(window($"ts", "15 seconds")).count().orderBy("window")
      

    val query =
      revenueDs.writeStream
        .format("console").option("truncate", "false")
        .outputMode(OutputMode.Complete()).start()

        
        query.awaitTermination()

  }
  
}