package com.datageeks.secondarysort

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

case class OrderedKey(k1: String, k2: String) extends Ordered[OrderedKey] {
  import scala.math.Ordered.orderingToOrdered
  def compare(that: OrderedKey): Int = (this.k1, this.k2) compare (that.k1, that.k2)
}
object SecondarySort {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "c://hadoop//")
    val sconf = new SparkConf().setAppName("Word Count").setMaster("local[4]")
    val sc = new SparkContext(sconf)
    val rawDataArray = sc.textFile("C://data/").map(line => {
      val array = line.split(",")
      (OrderedKey(array(0), array(1)), array(1))
    }).repartition(1).sortByKey(true).foreach(println(_))
  
   // rawDataArray.saveAsTextFile("C://data1/")
    sc.stop()
  }
  
}