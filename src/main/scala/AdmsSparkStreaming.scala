import java.text.SimpleDateFormat

import adms.kafka.constants.{Sensor, Topics}
import avro.AvroSchema
import com.google.gson.Gson
import db.PostgreConnection
import org.apache.kafka.common.security.JaasUtils
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._

import scala.collection.immutable.Queue
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object AdmsSparkStreaming {
  val conf = new SparkConf().setMaster("local[2]").setAppName("adms_spark_streaming")
  val ssc = new StreamingContext(conf, Seconds(1))
  System.setProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM, System.getProperty("user.dir") + "/jaas/adms-kafka-jaas.conf")

  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "dsicloud1.usc.edu:9093", //kafka集群地址
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[ByteArrayDeserializer],
    "sasl.kerberos.service.name" -> "kafka",
    "group.id" -> "admsmon_cg",
    "client.id" -> "kafkatutorial",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean),
    "security.protocol" -> "SASL_SSL",
    "sasl.mechanism" -> "GSSAPI"
  )

  val topics = Array(Topics.DEV.highwayRealtime)
  val WINDOW_SIZE = 5

  def main(args: Array[String]): Unit = {
    //Create the DStream
    val stream = KafkaUtils.createDirectStream[String, Array[Byte]](
      ssc,
      PreferConsistent,
      Subscribe[String, Array[Byte]](topics, kafkaParams))

    val highway_config_table = PostgreConnection.getHighwayConfig()

    var speedSumHistory = mutable.Map.empty[(Int, Double, Double), (String, Queue[Double])]

    stream.foreachRDD(rdd => {
      if (rdd.count > 0) {
        val records = rdd.map(iter => AvroSchema.getLoopSensorReading(iter)).filter(message => message.get("link_status").toString == "OK")
          .map(message => {
            val data_and_time = tranTime2String(message.get("date_and_time").toString.toLong)
            //(sensorId, timestamp, currentSpeed)
            (message.get("link_id").toString.toInt, (data_and_time, message.get("speed").toString.toDouble))
          })
        val sensorRecords = highway_config_table.join(records)
          .map{case(link_id,((lat,lon),(timestamp, speed))) => ((link_id, lat, lon),(timestamp, speed))}.collect().toMap
        val speed = calculateAvgSpeed(sensorRecords, speedSumHistory)
        println(speed)
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * Calculate the avg speed for last five batches(about 2.5min) for each sensor
    * @param sensorRecords the current batch of sensor records
    * @param speedSumHistory the history map recording past queue for each sensor
    * @return a json string: [{"date_and_time":"2018-09-06 11:55:32","sensorId":773507,"lat":33.994235,"lon":-117.897917,"speed":65.4},...]
    */
  def calculateAvgSpeed(sensorRecords: Map[(Int, Double, Double), (String, Double)], speedSumHistory: mutable.Map[(Int, Double, Double), (String, Queue[Double])]):
  String = {
    if (speedSumHistory.isEmpty) {
      sensorRecords.foreach(sensor => {
        speedSumHistory.getOrElseUpdate(sensor._1, (sensor._2._1, Queue(sensor._2._2)))
      })
    } else {
      sensorRecords.foreach(sensor => {
        val sensorId = sensor._1
        val timestamp = sensor._2._1
        val sensorSpeed = sensor._2._2
        val speedWindow = speedSumHistory.getOrElse(sensorId, ("", Queue()))._2
        if (speedWindow.isEmpty || speedWindow.size < WINDOW_SIZE) {
          speedSumHistory.update(sensorId, (timestamp, speedWindow.enqueue(sensorSpeed)))
        } else {
          speedSumHistory.update(sensorId, (timestamp, speedWindow.dequeue._2.enqueue(sensorSpeed)))
        }
      })
    }

    val gson = new Gson
    val speedAverage = ListBuffer.empty[String]
    speedSumHistory.foreach(sensor => {
      val element = Sensor(sensor._2._1, sensor._1._1, sensor._1._2, sensor._1._3, sensor._2._2.sum / sensor._2._2.size)
      val jsonElement = gson.toJson(element)
      speedAverage.append(jsonElement)
    })
    speedAverage.toList.toString().substring(4).replace('(', '[').replace(')', ']')
  }

  private def tranTime2String(timestamp: Long): String = {
    val date_and_time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    date_and_time.format(timestamp * 1000)
  }

}