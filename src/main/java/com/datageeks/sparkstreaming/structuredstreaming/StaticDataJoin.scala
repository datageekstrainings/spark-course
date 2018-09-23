package com.datageeks.sparkstreaming.structuredstreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode

object StaticDataJoin {
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
    //take customer data as static df
    val customerDs = sparkSession.read
      .format("csv")
      .option("header", true)
      .load("C://data/")
      .as[Customer]

    import sparkSession.implicits._
    val dataDf = socketStreamDf.as[String].flatMap(value ⇒ value.split(" "))
    val salesDs = dataDf
      .as[String]
      .map(value ⇒ {
        val values = value.split(",")
        Sales(values(0), values(1), values(2), values(3).toDouble)
      })

    val joinedDs = salesDs
      .join(customerDs, "customerId")
    //create sales schema
    val query =
      joinedDs.writeStream.format("console").outputMode(OutputMode.Append())

    query.start().awaitTermination()

  }
  case class Sales(
    transactionId: String,
    customerId:    String,
    itemId:        String,
    amountPaid:    Double)
  case class Customer(customerId: String, customerName: String)
}