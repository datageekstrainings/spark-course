package com.datageeks.exercises

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object EmpProductivity {
  def main(args: Array[String]) {

    //System.setProperty("hadoop.home.dir", "c://hadoop//")
    val sconf = new SparkConf().setAppName("Emp Productivity") //.setMaster("local[4]")
    val sc = new SparkContext(sconf)

    val tasks = sc.textFile(args(0)).map(line => {
      val array = line.split(",")
      //  if (!array(0).startsWith("S"))
      (array(1), array(0))
    })
    val jiraDetails = sc.textFile(args(1)).map(line => {
      val array = line.split(",")
      (array(0), array(2))
    })
    val joinedRDD = tasks.join(jiraDetails).values.map(line => (line, 1)).reduceByKey(_ + _)

    joinedRDD.saveAsTextFile(args(2))
    sc.stop()
  }
}