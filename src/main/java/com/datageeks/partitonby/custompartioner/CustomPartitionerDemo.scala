package com.datageeks.partitonby.custompartioner

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object CustomPartitionerDemo {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "c://hadoop//")
    val sconf = new SparkConf().setAppName("Word Count").setMaster("local[4]")
    val sc = new SparkContext(sconf)

    val counts = sc.textFile("C://data/").
      flatMap(line => line.split("\\W+")).
      map(word => (word, 1))
    val partitionedCounts = counts.partitionBy(new CustomPartitioner(2)).reduceByKey((v1, v2) => v1 + v2)

    partitionedCounts.saveAsTextFile("C://data1/")
    sc.stop()
  }
}