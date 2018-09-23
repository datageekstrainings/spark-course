package com.datageeks.sparkstreaming.dstreams

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Time
import org.apache.spark.sql.SparkSession

object StreamingWordCountUpdateByKey {
  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir", "c://hadoop//")
    val sconf = new SparkConf().setAppName("Streaming Word Count").setMaster("local[4]")
    val sc = new SparkContext(sconf)
    val ssc = new StreamingContext(sc, Seconds(10))
    ssc.checkpoint("c://data1")
    val counts = ssc.socketTextStream("35.200.231.149", 9999, StorageLevel.MEMORY_AND_DISK_SER).
      flatMap(line => line.split("\\W+"))
    val mapCount = counts.map(word => (word, 1))
      .reduceByKey((v1, v2) => v1 + v2)
    //.reduceByKeyAndWindow((a: Int, b: Int) => a + b, Seconds(30), Seconds(10))
    val stateMaintaineedCount = mapCount.updateStateByKey(updateFunction)
    mapCount.print()
    stateMaintaineedCount.print()
    ssc.start()
    ssc.awaitTermination()

  }
  def updateFunction(newValues: Seq[(Int)], runningCount: Option[(Int)]): Option[(Int)] = {
    var result: Option[Int] = null
    if (newValues.isEmpty)
      result = Some(runningCount.get)
    else {
      newValues.foreach {
        x =>
          {
            if (runningCount.isEmpty)
              result = Some(x)
            else
              result = Some(x + runningCount.get)
          }
      }
    }
    return result
  }
}