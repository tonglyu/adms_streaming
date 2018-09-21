package db

import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object PostgreConnection {
  private val session = SparkSession.builder()
    .master("local[2]")
    .appName("PostgreConnection")
    .getOrCreate()

  val dbProperties = new Properties()
  dbProperties.load(getClass.getClassLoader().getResourceAsStream("postgre-config.properties"))

  private val hostname = dbProperties.getProperty("hostname")
  private val port = dbProperties.getProperty("port")
  private val database = dbProperties.getProperty("database")
  private val jdbc_url = s"jdbc:postgresql://${hostname}:${port}/${database}"

  def main(args: Array[String]): Unit = {
    val start_time = System.nanoTime()
    val end_time = System.nanoTime()
    val time = (end_time - start_time) / 1000000000
    println("Time: " + time + " sec")
  }

  /**
    * Get highway.highway_congestion_config_view table from PostgreSQL
    *
    * @return RDD(sensorId, (lat,lon))
    */
  def getHighwayConfig(): RDD[(Int, (Double, Double))] = {
    val config_table = session.read.jdbc(jdbc_url, "highway.highway_congestion_config_view", dbProperties)
    config_table.createOrReplaceTempView("highway_config")

    try {
      val sqlStat =
        s"""
                       select * from highway_config
                       where config_id= (select max(config_id) from highway_config)
                     """

      val rs = session.sql(sqlStat)
      val highway_config = rs.rdd.map(row => (row.getInt(4), row.getDouble(13), row.getDouble(14)))
        .map { case ((link_id), lat, lon) => (link_id, (lat, lon)) }
      highway_config
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
        null
    }
  }


  def arterialTest(): Unit = {
    val config_table = session.read.jdbc(jdbc_url, "arterial.arterial_congestion_config_view", dbProperties)
    config_table.createOrReplaceTempView("arterial_config")
    val data_table = session.read.jdbc(jdbc_url, "arterial.arterial_congestion_data", dbProperties)
    data_table.createOrReplaceGlobalTempView("arterial_data")

    var onestreet = "'1ST ST'"
    var fromStreet = "'HOPE ST'"
    var toStreet = "'VERMONT AV'"

    var lat1, lon1, lat2, lon2 = 0.0
    //First Point
    try {
      val sqlStat =
        s"""
                       select lat, lon from arterial_config
                       where config_id= (select max(config_id) from arterial_config)
                       and onstreet= $onestreet
                       and fromstreet= $fromStreet
                     """

      val rs = session.sql(sqlStat)
      if (rs != null) {
        lat1 = rs.first().getDouble(0)
        lon1 = rs.first().getDouble(1)
      }
      println(lat1, lon1)
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
        return null
    }

    //Second Point
    try {
      val sqlStat =
        s"""
                       select lat, lon from arterial_config
                       where config_id = (select max(config_id) from arterial_config)
                       and onstree t= $onestreet
                       and fromstreet = $toStreet
                     """

      val rs = session.sql(sqlStat)
      if (rs != null) {
        lat2 = rs.first().getDouble(0)
        lon2 = rs.first().getDouble(1)
      }
      println(lat2, lon2)
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
        return null
    }

    val computeDirection = identifyDirection(bearing(lat1, lon1, lat2, lon2));


    // identify the sensors on the corridor// identify the sensors on the corridor
    var sensors = ""
    val lowerBound = Math.min(lat1, lat2)
    val upperBound = Math.max(lat1, lat2)
    val leftBound = Math.min(lon1, lon2)
    val rightBound = Math.max(lon1, lon2)
    try {

      val sqlStat =
        s"""
                       select link_id from arterial_config
                       where config_id=(select max(config_id) from arterial_config)
                       and onstreet = $onestreet
                       and ((lat >= $lowerBound and lat <= $upperBound)
                       or (lon >= $leftBound and lon <= $rightBound))
                     """
      val rs = session.sql(sqlStat)
      rs.show()
      if (rs != null) {
        for (row <- rs.collect()) {
          sensors += "'" + row.getString(0) + "',"
        }
      }
      sensors = sensors.substring(0, sensors.length - 1)
      println(sensors)
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
        return null
    }
  }

  private def bearing(x1: Double, y1: Double, x2: Double, y2: Double): Double = {
    var lat1 = x1 * Math.PI / 180
    var lat2 = x2 * Math.PI / 180
    var lon1 = y1 * Math.PI / 180
    var lon2 = y2 * Math.PI / 180
    val dLon = lon2 - lon1
    val y = Math.sin(dLon) * Math.cos(lat2)
    val x = Math.cos(lat1) * Math.sin(lat2) - Math.sin(lat1) * Math.cos(lat2) * Math.cos(dLon)
    val brng = Math.atan2(y, x)
    val normalizedBrng = ((brng * (180 / Math.PI)) + 360) % 360
    normalizedBrng
  }

  private def identifyDirection(bearing: Double): Int = {
    var direction = -1
    if (bearing < 45 || bearing >= 315) { // North
      direction = 0
    }
    else if (bearing >= 45 && bearing < 135) { // East
      direction = 2
    }
    else if (bearing >= 135 && bearing < 225) { // South
      direction = 1
    }
    else if (bearing >= 225 && bearing < 315) { // West
      direction = 3
    }
    direction
  }
}
