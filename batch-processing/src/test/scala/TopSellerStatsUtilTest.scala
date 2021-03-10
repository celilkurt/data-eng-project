import com.dataeng.bootcamp.OrderForProblem2
import com.dataeng.bootcamp.util.TopSellerStatsUtil
import org.junit.Test

class TopSellerStatsUtilTest {

  val orders = List(OrderForProblem2("product_", "seller_1", 1200.0, "Created","2021-01-22T22:20:32.000+03:00","bilgisayar1"),
                    OrderForProblem2("product_4", "seller_2", 1000.0, "Cancelled","2021-01-22T22:20:32.000+03:00","telefon"),
                    OrderForProblem2("product_1", "seller_7", 102.0, "Created","2021-01-22T22:20:32.000+03:00","bilgisayar7"),
                    OrderForProblem2("product_5", "seller_2", 7000.0, "Created","2021-01-22T22:20:32.000+03:00","telefon"),
                    OrderForProblem2("product_0", "seller_3", 2000.0, "Created","2021-01-22T22:20:32.000+03:00","bilgisayar3"),
                    OrderForProblem2("product_2", "seller_3", 1000.0, "Created","2021-01-22T22:20:32.000+03:00","bilgisayar3"),
                    OrderForProblem2("product_0", "seller_2", 3000.0, "Created","2021-01-22T22:20:32.000+03:00","bilgisayar"),
                    OrderForProblem2("product_9", "seller_5", 385.0, "Created","2021-01-22T22:20:32.000+03:00","bilgisayar5"),
                    OrderForProblem2("product_3", "seller_4", 46.0, "Created","2021-01-22T22:20:32.000+03:00","bilgisayar4"),
                    OrderForProblem2("product_1", "seller_8", 878.0, "Created","2021-01-22T22:20:32.000+03:00","bilgisayar8"),
                    OrderForProblem2("product_6", "seller_4", 456.0, "Created","2021-01-22T22:20:32.000+03:00","bilgisayar4"),
                    OrderForProblem2("product_5", "seller_9", 780.0, "Created","2021-01-22T22:20:32.000+03:00","bilgisayar9"),
                    OrderForProblem2("product_5", "seller_10", 796.0, "Created","2021-01-22T22:20:32.000+03:00","bilgisayar10"),
                    OrderForProblem2("product_1", "seller_6", 44.0, "Created","2021-01-22T22:20:32.000+03:00","bilgisayar6"),
                    OrderForProblem2("product_1", "seller_5", 786.0, "Created","2021-01-22T22:20:32.000+03:00","bilgisayar5"),
                    OrderForProblem2("product_8", "seller_2", 800.0, "Returned","2021-01-22T22:20:32.000+03:00","telefon"),
                    OrderForProblem2("product_1", "seller_2", 3800.0, "Created","2021-01-22T22:20:32.000+03:00","bilgisayar"),
                    OrderForProblem2("product_5", "seller_12", 1325.0, "Created","2021-01-22T22:20:32.000+03:00","bilgisayar12"),
                    OrderForProblem2("product_0", "seller_11", 900.0, "Created","2021-01-22T22:20:32.000+03:00","bilgisayar11"))


  val topSellerStatsUtil = TopSellerStatsUtil(orders,"2021-01-22")


  /**
   * 'seller_2' nin siparişlerinin toplam tutarının 13800 olduğunu
   * biliyoruz. Eğer dönen değer 13800 ise metod doğru çalışıyordur.
   * */
  @Test def getTotalPriceTest(): Unit = {

    val createdOrdersOfSeller2 = orders.filter(order => order.seller_id == "seller_2" && order.status == "Created")
    assert(topSellerStatsUtil.getTotalPrice(createdOrdersOfSeller2) == 13800.0)

  }

  /**
   * 'seller_2' nin net satış tutarının 12000 olduğunu biliyoruz.
   * Eğer dönen değer 12000'e eşitse metod doğru çalışıyordur.
   * */
  @Test def getTotalNetPriceTest(): Unit = {
    val ordersOfSeller2 = orders.filter(_.seller_id == "seller_2")
      assert(topSellerStatsUtil.getTotalNetPrice(ordersOfSeller2) == 12000.0)
  }

  /**
   * Örnek list'teki seller'lar arasında en çok kar eden
   * seller 'seller_2' dir.
   *
   * 'getTopTenSellerIdByNetPrice', en çok kar eden kullanıcıları
   * sıralı döndürdüğünden, 'topTenSellerIds' in ilk elemanı 'seller_2'
   * olmalıdır.
   * */
  @Test def getTopTenSellerIdByNetSalesPriceTest: Unit = {

    val topTenSellerIds = topSellerStatsUtil.getTopTenSellerIdByNetSalesPrice(0)
    assert(topTenSellerIds == "seller_2")

  }

  /**
   * En çok kar eden kullanıcının 'seller_2' olduğunu ve
   * net satış tutarını(12000.0) biliyoruz.
   *
   * Eğer 'getTopTenSellerOrders' metodundan dönen 'seller_2'
   * order'larının net satışı bildiğimiz tutarla eşleşiyorsa
   * metod doğru çalışıyordur.
   * */
  @Test def getTopTenSellerOrdersTest: Unit = {
    val topTenSellerOrders = topSellerStatsUtil.getTopTenSellerOrders
    val ordersOfSeller2 = topTenSellerOrders.filter(_.seller_id == "seller_2")

    assert(topSellerStatsUtil.getTotalNetPrice(ordersOfSeller2) == 12000.0)

  }
  /**
   * 'seller_2' nin brüt 3, net 1 satışı vardır. Eğer metodun
   * çıktısı 1 ise metod doğru çalışıyordur.
   * */
  @Test def getNetSalesAmountTest: Unit = {
    val ordersOfSeller2 = orders.filter(_.seller_id == "seller_2")
    assert(topSellerStatsUtil.getNetSalesAmount(ordersOfSeller2) == 1)

  }


  /**
   * 'seller_2' nin en fazla kar ettiği kategori 'bilgisayar' dır.
   * Eğer metodun çıktısı 'bilgisayar' ise metod doğru çalışıyordur.
   * */
  @Test def getTopCategoryByNetPriceTest:Unit = {

    val ordersOfSeller2 = orders.filter(_.seller_id == "seller_2")
    assert(topSellerStatsUtil.getTopCategoryByNetPrice(ordersOfSeller2) == "bilgisayar")

  }



}
