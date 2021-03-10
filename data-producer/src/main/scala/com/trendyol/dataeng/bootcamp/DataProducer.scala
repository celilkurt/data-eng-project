package com.trendyol.dataeng.bootcamp

import com.typesafe.scalalogging.LazyLogging
import pureconfig.ConfigSource
import pureconfig.generic.auto._

import scala.collection.parallel.CollectionConverters._
import scala.concurrent.duration.Duration
import scala.io.Source

object DataProducer extends LazyLogging {

  case class AppConfig(kafka: KafkaConfig, orders: ProducerConfig, products: ProducerConfig)
  case class KafkaConfig(bootstrapServers: String, batchSize: Int, batchInterval: Duration)
  case class ProducerConfig(sourcePath: String, targetTopic: String)

  def main(args: Array[String]): Unit = {
    logger.info("Data Producer has started running.")
    val AppConfig(kafkaConfig, orders, products) = ConfigSource.default.loadOrThrow[AppConfig]
    val kafkaWriter                              = new KafkaWriter(kafkaConfig)

    List(orders, products).par.foreach(config => {
      val dataSource = Source.fromFile(config.sourcePath)
      kafkaWriter.write(dataSource.getLines(), config.targetTopic)
      dataSource.close()
    })
    logger.info("Data Producer has finished.")
  }
}
