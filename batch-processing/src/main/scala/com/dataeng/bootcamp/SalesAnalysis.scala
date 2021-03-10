package com.dataeng.bootcamp


import com.dataeng.bootcamp.util.{PriceChangeUtil, ProductStatsUtil, TopSellerStatsUtil}
import org.apache.spark.sql.{Encoders, SaveMode, SparkSession}

object SalesAnalysis {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Batch Processing")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")



    import spark.implicits._


    /**
     * Siparişler belirtilen dosyadan Dataset çıktısı verecek şekilde okunur.
     * */
    val ordersSchema = Encoders.product[Order].schema
    val orders = spark.read
      .schema(ordersSchema)
      .json("../raw-data/orders.json")
      .as[Order]



    /**
     * 'orders' dataset'i 'product_id'ye göre gruplanır.
     * Her grup için istatiksel hesaplamalar yapılır.
     * Elde edilen 'Collection', 'ProductSalesStats' tipinde bir dataset'e
     * dönüştürülür.
     * */
    val productStats = orders.groupByKey(_.product_id)
      .mapGroups((product_id,ordersIterator) => {

        /**
         * Her bir ürün için aşağıdaki istatistikler hesaplanır ve bir
         * 'ProductSalesStats' nesnesi ile döndürülür.
         * - Net satış adedi
         * - Net satış tutarı
         * - Brüt satış adedi
         * - Brüt satış tutarı
         * - Son beş günde ortalama satış adedi
         * - En çok satıldığı lokasyon
         * */
        ProductStatsUtil(ordersIterator.toList, product_id)
          .getProductStats

      })
      .as[ProductSalesStats]

    /**
     * 'productStats' dataset'i json tipinde diske yazdırılır.
     * */
    productStats
      .write
      .mode(SaveMode.Overwrite)
      .json("output/batch/product-stats")



    /**
     * 'Product' kayıtları belirtilen json dosyasından okunur ve typesafe
     * çalışmak amacıyla dataset'e çevirilir.
     * */
    val productsSchema = Encoders.product[Product].schema
    val products = spark.read
      .schema(productsSchema)
      .json("../raw-data/products.json")
      .as[Product]




    /**
     * 'Order' kayıtları 'category_name' ile zenginleştirilir.
     * */
    val ordersWithCategory = orders.join(products, products("productid") === orders("product_id"),"inner")
      .select(orders("product_id"),orders("seller_id"),products("categoryname"),orders("price"),orders("status"),orders("order_date"))
      .as[OrderForProblem2]


    /**
     * 'categoryname' ile zenginleştirilmiş sipariş kayıtları, oluşturuldukları
     * güne göre gruplanır.
     * Her bir grup için en çok gelir elde eden 10 satıcının, net satış tutarı
     * ve en çok gelir elde ettiği kategori bilgisi 'TopSellingSeller' list'i olarak döndürüllür.
     * 'flatMapGroups' kullanıldığı için her güne karşılık döndürülen 'TopSellingSeller' list'leri
     * tek bir list'e döndürülür.
     * Son olarak typesafe çalışılabilmesi için list, 'TopSellingSeller' dataset'ine döndürülür.
     * */
    val sellerStatsByDay = ordersWithCategory.groupByKey(_.date)
      .flatMapGroups((date,orderIterator) => {

        TopSellerStatsUtil(orderIterator.toList, date).getTopTenSellerStats

      })
      .as[TopSellingSeller]

    /**
     * Satıcı istatistikleri 'date'e göre kalsörlenecek şekilde disk'e,
     * json dosyası tipinde yazdırılır.
     * */
    sellerStatsByDay
      .write
      .partitionBy("date")
      .mode(SaveMode.Overwrite)
      .json("output/batch/seller-stats-by-days")



    /**
     * Ürünlerin fiyat değişimlerinin tespiti.
     *
     * Siparişler 'product_id' üzerinden gruplanır.
     * Her bir grup için fiyat değişimleri incelenir.
     * */
    val productByPriceChanges = orders.groupByKey(_.product_id)
      .flatMapGroups((product_id,orderIterator) => {

        PriceChangeUtil(orderIterator.toList,product_id).getPriceChanges

      })
      .as[ProductPriceChange]


    /**
     * Ürün fiyat değişimleri disk'e yazdırılır.
     * */
    productByPriceChanges
      .write
      .mode(SaveMode.Overwrite)
      .json("output/batch/price-changes")


  }






}


trait DateExtractor {

  lazy val date = timestampToDate
  val order_date: String

  def timestampToDate: String =
    if(order_date.length >= 10) order_date.substring(0,10)
    else ""
}

case class Order(customer_id: String,
                 location:    String,
                 seller_id:   String,
                 order_date:  String,
                 order_id:    String,
                 price:       Double,
                 product_id:  String,
                 status:      String) extends DateExtractor

case class OrderForProblem2(product_id:   String,
                            seller_id:    String,
                            price:        Double,
                            status:       String,
                            order_date:   String,
                            categoryname: String) extends DateExtractor

case class TopSellingSeller(seller_id:            String,
                            sales_amount:         Int,
                            top_selling_category: String,
                            date:                 String)

case class ProductPriceChange(product_id: String,
                              price:      Double,
                              date:       String,
                              change:     String)

case class Product(brandname:    String,
                   categoryname: String,
                   productid:    String,
                   productname:  String)

case class ProductSalesStats(product_id:                     String,
                             net_sales_amount:               Int,
                             net_sales_price:                Double,
                             gross_sales_amount:             Int,
                             gross_sales_price:              Double,
                             avarage_sales_amount_of_5_days: Double,
                             top_selling_location:           String )






