import com.dataeng.bootcamp.Order
import com.dataeng.bootcamp.util.ProductStatsUtil
import org.junit._
import org.junit.Assert.assertEquals

class ProductStatsUtilTest {

  val orders: List[Order] = List(Order("cus_1","İstanbul","seller_1","2021-01-22T22:20:32.000+03:00","1",9.49,"product_1","Created"),
                                  Order("cus_1","İstanbul","seller_2","2021-01-23T22:20:32.000+03:00","2",9.49,"product_2","Cancelled"),
                                  Order("cus_3","Diyarbakır","seller_2","2021-01-24T22:20:32.000+03:00","3",9.49,"product_2","Cancelled"),
                                  Order("cus_2","Adana","seller_1","2021-01-20T22:20:32.000+03:00","4",9.49,"product_3","Created"),
                                  Order("cus_3","İstanbul","seller_4","2021-01-01T22:20:32.000+03:00","5",9.49,"product_1","Cancelled"),
                                  Order("cus_3","İstanbul","seller_2","2021-01-03T22:20:32.000+03:00","6",9.49,"product_1","Created"),
                                  Order("cus_4","Malkara","seller_1","2021-01-12T22:20:32.000+03:00","7",9.49,"product_1","Returned"),
                                  Order("cus_4","Esenler","seller_3","2021-01-13T22:20:32.000+03:00","8",9.49,"product_2","Returned"),
                                  Order("cus_2","Bartın","seller_3","2021-01-15T22:20:32.000+03:00","9",9.49,"product_2","Created"),
                                  Order("cus_5","Muş","seller_3","2021-01-17T22:20:32.000+03:00","10",9.49,"product_4","Created"),
                                  Order("cus_6","Artvin","seller_4","2021-01-18T22:20:32.000+03:00","11",9.49,"product_4","Returned"),
                                  Order("cus_5","İstanbul","seller_4","2021-01-22T22:20:32.000+03:00","12",9.49,"product_3","Created"))

  val productStatsUtil = ProductStatsUtil(orders.filter(_.product_id == "product_1"), "product_1")

  @Test def getLastFiveDaysTest:Unit = {

    assertEquals("2021-01-22",productStatsUtil.getLastFiveDays(0))
  }


  @Test def getLastFiveDaysAvarageTest:Unit = {

    assert(productStatsUtil.getLastFiveDaysAvarage == 1.0)
  }

  @Test def getLocationByMaxSalesTest:Unit = {

    assertEquals("İstanbul", productStatsUtil.getLocationByMaxSales)
  }

  @Test def getSalesPriceTest:Unit = {
    assert(productStatsUtil.getSalesPrice(orders.slice(0,2)) == 9.49*2)
  }

  @Test def getGrossAndNetSalesAmountTest:Unit = {

    val netAndGrossSalesAmount = productStatsUtil.getGrossAndNetSalesAmount
    assertEquals((2,0).toString, netAndGrossSalesAmount.toString)

  }

  @Test def getGrossAndNetSalesPriceTest: Unit = {

    val netAndGrossSalesPrice = productStatsUtil.getGrossAndNetSalesPrice
    assertEquals((2*9.49,0.0).toString(), netAndGrossSalesPrice.toString())
  }






}
