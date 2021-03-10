package com.dataeng.bootcamp.util

import com.dataeng.bootcamp.{OrderForProblem2, TopSellingSeller}

case class TopSellerStatsUtil(orders: List[OrderForProblem2], date: String) {


  /**
   * 'orders' list'i bir günün sipariş kayıtlarını içermektedir.
   * En çok net kar eden 10 müşterinin kayıtları 'getTopTenSellerOrders'
   * fonsiyonu ile elde ediliyor.
   * Bu kayıtlar 'seller_id' üzerinden gruplanıyor.
   * Her grup için net satış adedi ve en çok gelir elde edilen kategori
   * hesaplanarak TopSellingSeller nesnesi ile döndürülüyor.
   * */
  def getTopTenSellerStats: Iterable[TopSellingSeller] =

    getTopTenSellerOrders
      .groupBy(_.seller_id)
      .map(group => {

        TopSellingSeller(group._1, // seller_id
          getNetSalesAmount(group._2), // net satış adedi
          getTopCategoryByNetPrice(group._2), // en çok kar edilen kategori
          date //günün tarihi
        )
      })


  /**
   * 'getTopTenSellerIdByNetSalesPrice' fonksiyonu ile 'orders' listesindeki
   * kayıtlara göre en çok kar eden ilk on satıcının id'si döndürülür.
   * 'orders' , seller_id'si 'topTenSellerIds' list'inde olanlar alınacak
   * şekilde filtreleniyor.
   * */
  def getTopTenSellerOrders: List[OrderForProblem2] = {
    val topTenSellerIds = getTopTenSellerIdByNetSalesPrice

    orders
      .filter(order => topTenSellerIds.contains(order.seller_id))
  }

  /**
   * 'orders', 'seller_id'ye göre gruplanıyor.
   * Her bir grubun net satış tutarı hesaplanıyor.
   * Elde edilen 'Map', 'sortBy' vb metodların kullanılabilmesi için 'List'e çeviriliyor.
   * list içerdiği 'Tuple'ların ikinci elemanlarına (Net satış tutarı) göre
   * azalan şekilde sıralanıyor.
   * Sıralı list'in ilk on elemanı (en fazla on elemanı) alınıyor.
   * Tuple list'i string list'ine (id list'ine) çevriliyor.
   * */
  def getTopTenSellerIdByNetSalesPrice: List[String] =
    orders
      .groupBy(_.seller_id)
      .mapValues(group => getTotalNetPrice(group))
      .toList
      .sortBy(_._2)(Ordering[Double].reverse)
      .take(10)
      .map(_._1)

  /**
   * 'orders' list'i, status'lerinin 'Created olup olmamasına göre iki gruba ayrılıyor.
   * İade veya iptal tutarının brüt satış tutardan çıkarılmasıyla net tutar hesaplanıyor.
   * */
  def getTotalNetPrice(orders: List[OrderForProblem2]): Double = {
    val (createdOrders, returnOrCancelOrders) = orders.partition(_.status == "Created")
    getTotalPrice(createdOrders) - getTotalPrice(returnOrCancelOrders)
  }

  /**
   * 'orders' list'indeki kayıtların 'price'ı toplanıp döndürülür.
   * */
  def getTotalPrice(orders: List[OrderForProblem2]): Double =
    orders
      .foldLeft(0.0)((sum, order) =>
        sum + order.price)


  /**
   * Brüt satış miktarı hesaplanır.
   * Sipariş adedinden brüt miktarının çıkarılmasıyla iade veya iptal
   * edilen siparişlerin miktarı hesaplanır.
   * Brüt satış miktarından iade ve iptal edilen sipariş miktarının
   * çıkarılmasıyla net satış miktarı hesaplanır.
   * */
  def getNetSalesAmount(orders: List[OrderForProblem2]): Int = {
    val createdOrderAmount = orders.count(_.status == "Created")
    val returnedOrderAmount = orders.size - createdOrderAmount
    createdOrderAmount - returnedOrderAmount
  }

  /**
   * 'orders', 'categoryname' üzerinden gruplanır.
   * Her grup için net satış tutarı hesaplanır.
   * Elde edilen 'Tuple' ların ikinci elemanına (net satış tutarına) göre
   * en büyük olan kayıt 'maxBy' fonsiyonu ile bulunur.
   * Bu 'Tuple' ın birinci elemanı (categoryname) döndürülür.
   * */
  def getTopCategoryByNetPrice(orders: List[OrderForProblem2]): String =
    orders
      .groupBy(_.categoryname)
      .mapValues(values => getTotalNetPrice(values))
      .maxBy(_._2)
      ._1


}
