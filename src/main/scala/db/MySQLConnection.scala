package db

import java.util.Properties
import net.liftweb.json._

object MySQLConnection {
  val dbProperties = new Properties()
  dbProperties.load(getClass.getClassLoader().getResourceAsStream("mysql-config.properties"))

  private val hostname = dbProperties.getProperty("hostname")
  private val port = dbProperties.getProperty("port")
  private val database = dbProperties.getProperty("database")
  private val user = dbProperties.getProperty("user")
  private val password = dbProperties.getProperty("password")
  private val jdbc_url = s"jdbc:mysql://${hostname}:${port}/${database}"

  private val conn = java.sql.DriverManager.getConnection(jdbc_url,user,password)

  /**
    * Insert the new batch into the lastest table in MySQL
    * @param speedAverage list of string: [{"date_and_time":"2018-09-06 11:55:32","sensorId":773507,"lat":33.994235,"lon":-117.897917,"speed":65.4},...]
    */
  def insert2LatestTable(speedAverage: List[String]):Unit = {
    val insertSql = """
                      |insert into test (date_and_time, sensorId,lat,lon,speed)
                      |values (?,?,?,?,?)
                    """.stripMargin
    val preparedStmt: java.sql.PreparedStatement = conn.prepareStatement(insertSql)
    for (snesorRecord <- speedAverage)
    {
      val sensorJsonObject = parse(snesorRecord)
      val date_and_time = (sensorJsonObject \\ "date_and_time").values("date_and_time").toString
      val sensorId = (sensorJsonObject \\ "sensorId").values("sensorId").toString.toInt
      val lat = (sensorJsonObject \\ "lat").values("lat").toString.toDouble
      val lon = (sensorJsonObject \\ "lon").values("lon").toString.toDouble
      val speed = (sensorJsonObject \\ "speed").values("speed").toString.toDouble

      println("date_and_time:" + date_and_time, "sensorId:" + sensorId, "lat:" + lat, "lon:" + lon, "speed:" + speed)

      preparedStmt.setString(1,date_and_time)
      preparedStmt.setInt(2,sensorId)
      preparedStmt.setDouble(3,lat)
      preparedStmt.setDouble(4,lon)
      preparedStmt.setDouble(5,speed)

      try {
        preparedStmt.execute
      } catch {
        case e: Exception =>
          e.printStackTrace()
          preparedStmt.close()
      }
    }
    preparedStmt.close()
  }
}
