package com.dataeng.bootcamp.util

import com.dataeng.bootcamp.{Order, ProductSalesStats}

case class ProductStatsUtil(orders: List[Order], product_id: String) {

  def this(ordersI: Iterator[Order], product_id: String) =
    this(ordersI.toList, product_id)


  /**
   * 'Created', 'Cancelled' ve 'Returned' olmak üzere 3 çeşit status vardır.
   * 'status'ü 'Created' olan kayıtlar 'createdOrders' listesinde, status'ü 'Cancelled' veya
   * 'Returned' olan kayıtlar 'returnOrCancelOrders' listesinde toplanır
   * */
  val (createdOrders, returnOrCancelOrders) = orders.partition(order => order.status == "Created")


  /**
   * orders'daki kayıtlar üzerinden gerekli
   * hesaplamaları yapar ve bir 'ProductSalesStats' nesnesi döndürür.
   * */
  def getProductStats: ProductSalesStats = {

    val (grossSalesPrice, netSalesPrice) = getGrossAndNetSalesPrice
    val (grossSalesAmount, netSalesAmount) = getGrossAndNetSalesAmount

    ProductSalesStats(product_id,
      netSalesAmount,
      netSalesPrice,
      grossSalesAmount,
      grossSalesPrice,
      getLastFiveDaysAvarage,
      getLocationByMaxSales)
  }


  /**
   * Satış olan son beş günü döndürür.
   *
   * Sipariş listesini tarih (string tipinde) listesine dönüştürür
   * ve 'distinct' fonksiyonu ile kayıtların tekrar etmediği bir list
   * elde edilir.
   * Elde edilen tarih list'esi büyükten küçüğe doğru sıralanır ve
   * ilk beş kayıt yani satışın olduğu son beş gün döndürülür.
   * */
  def getLastFiveDays: List[String] =
    createdOrders
      .map(_.date)
      .distinct
      .sorted(Ordering[String].reverse)
      .take(5)


  /**
   * Son beş günün brüt satış ortalamasını döndürür.
   *
   * Satış yapılan son beş gün elde edildikten sonra, product'ın
   * satılmamış olması mümkün olduğundan bunun kontrolü yapılır.
   * Eğer satış yapıldıysa
   *  - 'createdOrders' list'inde date'i 'lastFiveDays'den biri olan kayıtların
   *    eleman sayısı, satış gün sayısına bölünerek ortalama bulunur.
   *    Eğer son beş günde satış olmadıysa
   *  - 0/0 durumundan kaçınılması için sıfır döndürür.
   * */
  def getLastFiveDaysAvarage: Double =
    if (createdOrders.nonEmpty) {
      val lastFiveDays = getLastFiveDays
      createdOrders
        .count(order => lastFiveDays.contains(order.date)) / lastFiveDays.size.toDouble
    }
    else 0.0

  /**
   * En çok satışın (brüt satış) gerçekleştiği lokasyonu döndürür.
   *
   * Ürünün hiç satılmamış olması mümkün olduğundan önce bunun
   * kontrolü yapılır.
   * Eğer product hiç satılmamışsa
   *  - En çok satıldığı lokasyon yok anlamında boş bir string döndürülür
   *    Eğer product satıldıysa
   *  - Her bir location'a karşılık satış miktarı hesaplanır.
   *  - Satış miktarı en fazla olan location döndürülür.
   * */
  def getLocationByMaxSales: String =
    if (createdOrders.nonEmpty) {
      createdOrders
        .groupBy(_.location)
        .mapValues(_.size)
        .maxBy(_._2)
        ._1
    } else { //Satış yapılmamış
      ""
    }


  /**
   * 'orders'daki kayıtların toplam price'ını döndürür.
   * */
  def getSalesPrice(orders: List[Order]): Double =
    orders
      .foldLeft(0.0)((sum, order) => sum + order.price)

  /**
   * 'orders'ın eleman sayısını döndürür.
   * */
  def getSalesAmount(orders: List[Order]): Int = orders.size


  /**
   * Net satış, brüt satışa bağlıdır. İşlem tekrarı yapılmaması için
   * net ve brüt satış miktarı aynı fonksiyonda hesaplanmıştır.
   * Brüt satış miktarı (grossSalesAmount), status'ü 'Created' olan 'Order'ların
   * sayısına eşittir. Net satış miktarı da 'grossSalesAmount'tan status'ü 'Returned'
   * yada 'Cancelled' olan order'ların (retOrCanOrders) sayısının çıkarılması ile bulunur.
   * */
  def getGrossAndNetSalesAmount: (Int, Int) = {
    val grossSalesAmount = getSalesAmount(createdOrders)
    val returnedSalesAmount = getSalesAmount(returnOrCancelOrders)

    (
      grossSalesAmount,
      grossSalesAmount - returnedSalesAmount
    )
  }

  /**
   * Net satış, brüt satışa bağlıdır. İşlem tekrarı yapılmaması için
   * net ve brüt satış miktarı aynı fonksiyonda hesaplanmıştır.
   * Brüt satış tutarı (grossSalesPrice), status'ü 'Created' olan 'Order'ların
   * price'ının toplamına eşittir. Net satış miktarı da 'grossSalesPrice'tan status'ü 'Returned'
   * yada 'Cancelled' olan order'ların (retOrCanOrders) toplam tutarının çıkarılması ile bulunur.
   * */
  def getGrossAndNetSalesPrice: (Double, Double) = {
    val grossSalesPrice = getSalesPrice(createdOrders)
    val returnedSalesPrice = getSalesPrice(returnOrCancelOrders)

    (
      grossSalesPrice,
      grossSalesPrice - returnedSalesPrice
    )
  }

}
