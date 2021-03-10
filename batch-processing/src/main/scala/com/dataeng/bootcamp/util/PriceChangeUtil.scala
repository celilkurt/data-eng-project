package com.dataeng.bootcamp.util

import com.dataeng.bootcamp.{Order, ProductPriceChange}

case class PriceChangeUtil(orders: List[Order], product_id: String) {

  /**
   * Fiyat değişimi bütün kayıtlar üzerinden mi yoksa sadece created üzerinden mi hesaplanmalı
   *
   * */

  def this(ordersI: Iterator[Order], product_id: String) =
    this(ordersI.toList, product_id)


  /**
   * Siparişler oluşturulma tarihlerine göre artan sırayla sıralanır.
   * Başlangıç değerleri ilk kaydın fiyatı ve boş bir 'ProductPriceChange'
   * collection'ı olacak şekilde her bir kayıt için fiyat değişimi 'foldLeft'
   * fonksiyonu ile incelenir.
   * Eğer fiyat aynı kaldıysa parametrelerde beğişiklik olmaz.
   * Eğer fiyat azaldıysa 'product_id', 'price', 'date' ve 'status'ten,
   * 'ProductPriceChange' nesnesi oluşturulur, oluşturulan nesne değişimleri
   * tutan collection'a eklenir ve 'foldLeft' parametresi olarak kullandığımız
   * ürün fiyatı güncellenir.
   * Eğer fiyat arttıysa yukarıdaki işlemler, sadece 'status'ü değişecek şekilde
   * tekrarlanır.
   * */
  def getPriceChanges: Seq[ProductPriceChange] =
    orders
      .sortBy(_.order_date)
      .foldLeft((orders.head.price, Seq.empty[ProductPriceChange]))((changeGroup, order) => {

        if (order.price == changeGroup._1) { // ürünün fiyatı değişmemişse
          changeGroup
        } else if (order.price > changeGroup._1) { // ürünün fiyatı artmışsa
          (order.price, ProductPriceChange(order.product_id, order.price, getDate(order.order_date), "rise") +: changeGroup._2)
        } else { // ürünün fiyatı düşmüşse
          (order.price, ProductPriceChange(order.product_id, order.price, getDate(order.order_date), "fall") +: changeGroup._2)
        }
      })
      ._2


  /**
   * 'order_date' istenen formata dönüştürülür.
   * '2021-01-22T01:20:32.000...' => '2021-01-22 01:20:32'
   * */
  def getDate(order_date: String): String = {
    if (order_date == null) {
      ""
    } else if (order_date.length < 19) {
      ""
    } else {
      order_date
        .substring(0, 19)
        .replace("T", " ")
    }

  }


}
