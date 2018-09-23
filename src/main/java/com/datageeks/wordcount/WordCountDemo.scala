package com.datageeks.wordcount

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object WordCountDemo {
   def main(args: Array[String]) {
  
    System.setProperty("hadoop.home.dir", "c://hadoop//")
    val sconf = new SparkConf().setAppName("Word Count").setMaster("local[4]")
    val sc = new SparkContext(sconf)

    val counts = sc.textFile(args(0)).
       flatMap(line => line.split("\\W+")).
       map(word => (word,1)).
       reduceByKey((v1,v2) => v1+v2)
       counts.saveAsTextFile(args(1))
      //counts.take(10).foreach(println)
    
    sc.stop()
  }
}