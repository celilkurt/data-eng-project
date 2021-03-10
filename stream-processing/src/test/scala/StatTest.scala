
import Main.{distinctProductCount, distinctSellerCount, getCreatedOrders}
import org.junit._

class StatTest {

  val orders: List[Order] = List(Order("cus_1","İstanbul","seller_1","2021-01-22T22:20:32.000+03:00","1",9.49,"product_1","Created"),
    Order("cus_1","İstanbul","seller_2","2021-01-23T22:20:32.000+03:00","2",9.49,"product_2","Cancelled"),
    Order("cus_3","Van","seller_2","2021-01-24T22:20:32.000+03:00","3",9.49,"product_2","Cancelled"),
    Order("cus_2","Adana","seller_1","2021-01-20T22:20:32.000+03:00","4",9.49,"product_3","Created"),
    Order("cus_3","İstanbul","seller_4","2021-01-01T22:20:32.000+03:00","5",9.49,"product_1","Cancelled"),
    Order("cus_3","İstanbul","seller_3","2021-01-03T22:20:32.000+03:00","6",9.49,"product_4","Created"),
    Order("cus_4","Malkara","seller_1","2021-01-12T22:20:32.000+03:00","7",9.49,"product_5","Returned"))


  @Test def getCreatedOrdersTest: Unit =
    assert(getCreatedOrders(orders).size == 3)

  @Test def getCreatedOrdersEmptyListTest: Unit =
    assert(getCreatedOrders(List.empty).size == 0)

  @Test def distinctSellerCountTest: Unit =
    assert(distinctSellerCount(orders) == 4)

  @Test def distinctSellerCountEmptyListTest: Unit =
    assert(distinctSellerCount(List.empty) == 0)

  @Test def distinctProductCountTest: Unit =
    assert(distinctProductCount(orders) == 5)

  @Test def distinctProductCountEmptyListTest: Unit =
    assert(distinctProductCount(List.empty) == 0)

}
