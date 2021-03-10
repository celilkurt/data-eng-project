import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows

import java.sql.Timestamp
import java.util.Properties


object Main {

  def main(args: Array[String]): Unit = {



    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(60000,CheckpointingMode.AT_LEAST_ONCE)
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(100,15000))
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


    env.setParallelism(1)
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")

    val producer = new FlinkKafkaProducer[String]("stats",new SimpleStringSchema,properties)
    val consumer = new FlinkKafkaConsumer[String]("orders",  new SimpleStringSchema(), properties)


    val keyedStream = env
      .addSource(consumer)
      // String tipindeki veriler 'Order'a dönüştürülüyor.
      .map(data => (new ObjectMapper() with ScalaObjectMapper)
          .registerModule(DefaultScalaModule)
          .readValue(data,classOf[Order]))
      .assignTimestampsAndWatermarks( new BoundedOutOfOrdernessTimestampExtractor[Order](Time.seconds(15)) {

        override def extractTimestamp(element: Order): Long =
          element.orderDateToTimeStamp

      })
      .keyBy(_.location)


    /**
     * 15 saniyelik gecikmeyi tolare edecek şekilde 5 dakikalık ardışık window'lar oluşturuluyor.
     * Farklı 'location'lara denk gelen her bir grup için aşağıdaki istatistikler hesaplanıyor.
     *  - Satış yapan farklı satıcı sayısı
     *  - Satılan farklı ürün sayısı
     * Hesaplamalar, satılan ürün ve satış yapan kullanıcı denildiği için elde edilen status'ü 'Created'
     * olan 'Order' lar üzerinden yapılıyor.
     * 'getCreatedOrders' fonsiyonu ile kayıtlar status'ü 'Created' olanlar kalacak şekilde filtreleniyor.
     * 'distinctSellerCount' fonksiyonu ile farklı satıcı sayısı hesaplanıyor.
     * 'distinctProductCount' fonsiyonu ile farklı ürün sayısı hesaplanıyor.
     * Elde edilen istatistikler, lokasyon ve timestamp ile birlikte 'collector'a ekleniyor.
     * 'collector'da toplanan veriler 'kafkaProducer' ile 'stats' adındaki kafka topic'ine yazılıyor.
     * */
    keyedStream
      .window(TumblingEventTimeWindows.of(Time.minutes(5)))
      .process(
        new ProcessWindowFunction[Order, String, String, TimeWindow] {
          override def process(
                                key: String,
                                context: Context,
                                elements: Iterable[Order],
                                out: Collector[String]
                              ): Unit = {
            val elemList = getCreatedOrders(elements.toList)
            val seller_count = distinctSellerCount(elemList)
            val product_count = distinctProductCount(elemList)
            out.collect(ProductStats(seller_count, product_count, key, new Timestamp(context.window.getStart) ).toString)
          }
        }
      )
      .addSink(producer)

    env.execute()




  }



  def getCreatedOrders(orders: List[Order]): List[Order] = {
    if(orders.nonEmpty) orders.filter(_.status == "Created")
    else orders
  }

  def distinctSellerCount(orders: List[Order]): Int =
    orders
      .map(_.seller_id)
      .distinct
      .size

  def distinctProductCount(orders: List[Order]): Int =
    orders
      .map(_.product_id)
      .distinct
      .size




}

