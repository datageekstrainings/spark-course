package com.datageeks.sparkstreaming.dstreams

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Time
import org.apache.spark.sql.SparkSession

object StreamingWordCount {
  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir", "c://hadoop//")
    val sconf = new SparkConf().setAppName("Streaming Word Count").setMaster("local[4]")
    val sc = new SparkContext(sconf)
    val ssc = new StreamingContext(sc, Seconds(10))
    val counts = ssc.socketTextStream("35.200.231.149", 9999, StorageLevel.MEMORY_AND_DISK_SER).
      flatMap(line => line.split("\\W+")).foreachRDD { (rdd: RDD[String], time: Time) =>
        val spark = SparkSession.builder().config(rdd.sparkContext.getConf).getOrCreate()
        import spark.implicits._
        val wordsDataframe = rdd.map(word => Records(word)).toDF()
        wordsDataframe.registerTempTable("words")
        val wordcountDF = spark.sql("Select word,count(*) from words group by word")
        wordcountDF.show()
        val rddAgain = wordcountDF.rdd.map(word => (word, 1)).
          reduceByKey((v1, v2) => v1 + v2)
        rddAgain.collect().foreach(println(_))

      }

    ssc.start()
    ssc.awaitTermination()

  }

  case class Records(word: String)
}