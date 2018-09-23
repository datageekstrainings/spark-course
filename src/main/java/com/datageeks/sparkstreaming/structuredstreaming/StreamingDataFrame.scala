package com.datageeks.sparkstreaming.structuredstreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode

object StreamingDataFrame {
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
    val socketDs = socketStreamDf.as[String]
    val wordsDs = socketDs.flatMap(value => value.split(" "))
    val countDs = wordsDs.groupBy("value").count()

    val query =
      countDs.writeStream.format("console").outputMode(OutputMode.Complete())

    query.start().awaitTermination()

  }
}