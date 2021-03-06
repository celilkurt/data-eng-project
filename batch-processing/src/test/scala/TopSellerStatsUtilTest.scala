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
   * 'seller_2' nin sipari??lerinin toplam tutar??n??n 13800 oldu??unu
   * biliyoruz. E??er d??nen de??er 13800 ise metod do??ru ??al??????yordur.
   * */
  @Test def getTotalPriceTest(): Unit = {

    val createdOrdersOfSeller2 = orders.filter(order => order.seller_id == "seller_2" && order.status == "Created")
    assert(topSellerStatsUtil.getTotalPrice(createdOrdersOfSeller2) == 13800.0)

  }

  /**
   * 'seller_2' nin net sat???? tutar??n??n 12000 oldu??unu biliyoruz.
   * E??er d??nen de??er 12000'e e??itse metod do??ru ??al??????yordur.
   * */
  @Test def getTotalNetPriceTest(): Unit = {
    val ordersOfSeller2 = orders.filter(_.seller_id == "seller_2")
      assert(topSellerStatsUtil.getTotalNetPrice(ordersOfSeller2) == 12000.0)
  }

  /**
   * ??rnek list'teki seller'lar aras??nda en ??ok kar eden
   * seller 'seller_2' dir.
   *
   * 'getTopTenSellerIdByNetPrice', en ??ok kar eden kullan??c??lar??
   * s??ral?? d??nd??rd??????nden, 'topTenSellerIds' in ilk eleman?? 'seller_2'
   * olmal??d??r.
   * */
  @Test def getTopTenSellerIdByNetSalesPriceTest: Unit = {

    val topTenSellerIds = topSellerStatsUtil.getTopTenSellerIdByNetSalesPrice(0)
    assert(topTenSellerIds == "seller_2")

  }

  /**
   * En ??ok kar eden kullan??c??n??n 'seller_2' oldu??unu ve
   * net sat???? tutar??n??(12000.0) biliyoruz.
   *
   * E??er 'getTopTenSellerOrders' metodundan d??nen 'seller_2'
   * order'lar??n??n net sat?????? bildi??imiz tutarla e??le??iyorsa
   * metod do??ru ??al??????yordur.
   * */
  @Test def getTopTenSellerOrdersTest: Unit = {
    val topTenSellerOrders = topSellerStatsUtil.getTopTenSellerOrders
    val ordersOfSeller2 = topTenSellerOrders.filter(_.seller_id == "seller_2")

    assert(topSellerStatsUtil.getTotalNetPrice(ordersOfSeller2) == 12000.0)

  }
  /**
   * 'seller_2' nin br??t 3, net 1 sat?????? vard??r. E??er metodun
   * ????kt??s?? 1 ise metod do??ru ??al??????yordur.
   * */
  @Test def getNetSalesAmountTest: Unit = {
    val ordersOfSeller2 = orders.filter(_.seller_id == "seller_2")
    assert(topSellerStatsUtil.getNetSalesAmount(ordersOfSeller2) == 1)

  }


  /**
   * 'seller_2' nin en fazla kar etti??i kategori 'bilgisayar' d??r.
   * E??er metodun ????kt??s?? 'bilgisayar' ise metod do??ru ??al??????yordur.
   * */
  @Test def getTopCategoryByNetPriceTest:Unit = {

    val ordersOfSeller2 = orders.filter(_.seller_id == "seller_2")
    assert(topSellerStatsUtil.getTopCategoryByNetPrice(ordersOfSeller2) == "bilgisayar")

  }



}
