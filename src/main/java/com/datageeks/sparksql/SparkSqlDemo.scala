package com.datageeks.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.expressions.Explode
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.Column
import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset
import com.databricks.spark.xml.XmlReader;
import org.apache.spark.sql.Column
import org.apache.spark.sql.Column

object SparkSqlDemo {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "c://hadoop//")
    val sconf = new SparkConf().setAppName("Word Count").setMaster("local[4]")
    val sc = new SparkContext(sconf)
   // val sqlContext = new SQLContext(sc)

    /*val xmlDF = sqlContext.read.format("com.databricks.spark.xml").option("rowTag", "CustomerGroupList").load("C://data/")
    val exploded = xmlDF.withColumn("CustomerGroup", explode(new Column("CustomerGroup")))
exploded.a
    val selectedData =  exploded.select("CustomerGroup._CustomerIdentifier", "CustomerGroup._GroupTypeCode", "CustomerGroup._GroupTypeReferenceCode", "CustomerGroup._GroupIdentifier", "CustomerGroup._CreateSystemCode", "CustomerGroup._CreateSystemReferenceCode", "CustomerGroup._CreateTimestamp", "CustomerGroup._LastUpdateTimestamp")
    val filtered=selectedData.map(x=>_+_)(selectedData("_CustomerIdentifier")>286363)
    selectedData.show()
   filtered.registerTempTable("table1")
   sqlContext.sql("Select * from table1").show()
   
   
    val csvDF=sqlContext.read.format("csv").option("header", true).load("C://data1/")
    csvDF.registerTempTable("table2")
    sqlContext.sql("Select * from table2").show()
     sqlContext.sql("Select desc,_CustomerIdentifier from table1 JOIN table2 where code=_GroupTypeCode").show()
 */   sc.stop()
  }
}