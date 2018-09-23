package com.datageeks.graphx
import scala.util.MurmurHash

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

/*case class Flight(year: String, Month: String, DayofMonth: String, DayOfWeek: String, DepTime: String,
                  CRSDepTime: String, ArrTime: String, CRSArrTime: String, UniqueCarrier: String, FlightNum: String,
                  TailNum: String, ActualElapsedTime: String, CRSElapsedTime: String, AirTime: String, ArrDelay: String,
                  DepDelay: String, Origin: String, Dest: String, Distance: String, TaxiIn: String,
                  TaxiOut: String, Cancelled: String, CancellationCode: String, Diverted: String, CarrierDelay: String,
                  WeatherDelay: String, NASDelay: String, SecurityDelay: String, LateAircraftDelay: String)

object TopTenFlights {
  def parseFlight(str: String): Flight = {
    val line = str.split(",")
    Flight(line(0), line(1), line(2), line(3), line(4),
      line(5), line(6), line(7), line(8), line(9),
      line(10), line(11), line(12), line(13), line(14),
      line(15), line(16), line(17), line(18), line(19),
      line(20), line(21), line(22), line(23), line(24),
      line(25), line(26), line(27), line(28))
  }

  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir", "c://hadoop//")
    val sconf = new SparkConf().setAppName("Spark-Graphx-Top10Flights").setMaster("local[4]")
    val sc = new SparkContext(sconf)
    val textRDD = sc.textFile("C://data/")
    val flightsRDD = textRDD.map(parseFlight)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    //val filteredFlights = flightsRDD.filter(flight => flight.Diverted.toInt.equals(1) && flight.Month.toInt >= 5 && ((flight.Origin.equalsIgnoreCase("SFO") || flight.Dest.equalsIgnoreCase("LAX")) || (flight.Origin.equalsIgnoreCase("LAX") || flight.Dest.equalsIgnoreCase("SFO"))))
    val airports: RDD[(VertexId, String)] = flightsRDD.map(flight => (MurmurHash.stringHash(flight.Origin).toLong, flight.Origin)).distinct

    // Defining a default vertex called nowhere
    val nowhere = "nowhere"

    val routes = flightsRDD.map(flight => ((MurmurHash.stringHash(flight.Origin), MurmurHash.stringHash(flight.Dest)), flight)).distinct
    //routes.cache()
    val edges = routes.map { case ((org_id, dest_id), flight) => Edge(org_id.toLong, dest_id.toLong, flight) }

    //Defining the Graph
    val graph = Graph(airports, edges, nowhere)
    graph.edges.collect().foreach(println)
    val filteredTriplets = graph.triplets.filter(x => (x.srcAttr.equalsIgnoreCase("SFO") || x.srcAttr.equalsIgnoreCase("LAX")) && (x.dstAttr.equalsIgnoreCase("SFO") || x.dstAttr.equalsIgnoreCase("LAX")))
    //.filter(x => x.attr.Month.toInt >= 5)
    //.filter(x => x.attr.Diverted.toInt > 0)

    sc.stop()
  }
}
*/