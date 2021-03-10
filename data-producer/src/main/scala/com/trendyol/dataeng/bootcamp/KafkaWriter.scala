package com.trendyol.dataeng.bootcamp

import java.util.Properties

import com.trendyol.dataeng.bootcamp.DataProducer.KafkaConfig
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import scala.annotation.tailrec

class KafkaWriter(kafkaConfig: KafkaConfig) extends LazyLogging {
  val properties = new Properties()
  properties.put("bootstrap.servers", kafkaConfig.bootstrapServers)

  val producer = new KafkaProducer[String, String](properties, new StringSerializer(), new StringSerializer())

  def write(records: Iterator[String], topic: String): Unit = {
    logger.info(s"Starting to produce data to $topic topic.")

    @tailrec
    def sendData(records: Iterator[String]): Unit = if (records.nonEmpty) {
      val (batch, others) = records.splitAt(kafkaConfig.batchSize)
      batch.map(makeProducerRecord(topic)).foreach(producer.send(_).get())
      Thread.sleep(kafkaConfig.batchInterval.toMillis)
      sendData(others)
    }

    sendData(records)
  }

  def makeProducerRecord(topic: String)(data: String): ProducerRecord[String, String] =
    new ProducerRecord[String, String](topic, data)
}
