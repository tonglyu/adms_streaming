package avro

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.{DatumReader, Decoder, DecoderFactory}
import org.apache.avro.specific.SpecificDatumReader
import org.apache.kafka.clients.consumer.ConsumerRecord

import scala.io.Source

object AvroSchema {
  val busInventory = Source.fromURL(getClass.getClassLoader.getResource("avro/bus_inventory.avsc")).mkString
  val busRealtime = Source.fromURL(getClass.getClassLoader.getResource("avro/bus_realtime.avsc")).mkString
  val loopSensor = Source.fromURL(getClass.getClassLoader.getResource("avro/loop_sensor.avsc")).mkString
  val loopSensorReading = Source.fromURL(getClass.getClassLoader.getResource("avro/loop_sensor_reading.avsc")).mkString

  def getBusInventory(consumerRecord: ConsumerRecord[String, Array[Byte]]): GenericRecord = {
    val message = consumerRecord.value()
    val schema: Schema = new Schema.Parser().parse(busInventory)
    val reader: DatumReader[GenericRecord] = new SpecificDatumReader[GenericRecord](schema)
    val decoder: Decoder = DecoderFactory.get().binaryDecoder(message, null)
    val records: GenericRecord = reader.read(null, decoder)

    records
  }

  def getBusRealtime(consumerRecord: ConsumerRecord[String, Array[Byte]]): GenericRecord = {
    val message = consumerRecord.value()
    val schema: Schema = new Schema.Parser().parse(busRealtime)
    val reader: DatumReader[GenericRecord] = new SpecificDatumReader[GenericRecord](schema)
    val decoder: Decoder = DecoderFactory.get().binaryDecoder(message, null)
    val records: GenericRecord = reader.read(null, decoder)

    records
  }

  def getLoopSensor(consumerRecord: ConsumerRecord[String, Array[Byte]]): GenericRecord = {
    val message = consumerRecord.value()
    val schema: Schema = new Schema.Parser().parse(loopSensor)
    val reader: DatumReader[GenericRecord] = new SpecificDatumReader[GenericRecord](schema)
    val decoder: Decoder = DecoderFactory.get().binaryDecoder(message, null)
    val records: GenericRecord = reader.read(null, decoder)

    records
  }

  def getLoopSensorReading(consumerRecord: ConsumerRecord[String, Array[Byte]]): GenericRecord = {
    val message = consumerRecord.value()
    val schema: Schema = new Schema.Parser().parse(loopSensorReading)
    val reader: DatumReader[GenericRecord] = new SpecificDatumReader[GenericRecord](schema)
    val decoder: Decoder = DecoderFactory.get().binaryDecoder(message, null)
    val records: GenericRecord = reader.read(null, decoder)

    records
  }
}
