import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.HashPartitioner
import org.apache.spark.RangePartitioner
import org.apache.spark.RangePartitioner

object HashPartitionerDemo {
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: WordCount <file>")
      System.exit(1)
    }
    System.setProperty("hadoop.home.dir", "c://hadoop//")
    val sconf = new SparkConf().setAppName("Word Count").setMaster("local[4]")
    val sc = new SparkContext(sconf)

    val counts = sc.textFile("C://data/").
      flatMap(line => line.split("\\W+")).
      map(word => (word, 1))
    val partitionedCounts = counts.partitionBy(new HashPartitioner(10)).reduceByKey((v1, v2) => v1 + v2)

    partitionedCounts.saveAsTextFile("C://data1/")
    sc.stop()
  }
}

