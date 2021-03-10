
  import com.dataeng.bootcamp.Order
  import com.dataeng.bootcamp.util.PriceChangeUtil
  import org.junit._
  import org.junit.Assert.assertEquals

  class PriceChangeTest {

    val orders: List[Order] = List(Order("cus_1","İstanbul","seller_1","2021-01-01T22:20:32.120+03:00","1",9.00,"product_1","Created"),
                                  Order("cus_1","İstanbul","seller_2","2021-01-02T22:20:32.002+03:00","2",9.49,"product_1","Cancelled"),
                                  Order("cus_3","Diyarbakır","seller_2","2021-01-03T22:20:32.023+03:00","3",8.49,"product_1","Cancelled"),
                                  Order("cus_2","Adana","seller_1","2021-01-04T22:20:32.010+03:00","4",9.00,"product_1","Created"),
                                  Order("cus_3","İstanbul","seller_4","2021-01-05T22:20:32.040+03:00","5",9.49,"product_1","Cancelled"),
                                  Order("cus_3","İstanbul","seller_2","2021-01-06T22:20:32.030+03:00","6",9.49,"product_1","Created"),
                                  Order("cus_4","Malkara","seller_1","2021-01-07T22:20:32.017+03:00","7",9.00,"product_1","Returned"),
                                  Order("cus_4","Esenler","seller_3","2021-01-08T22:20:32.028+03:00","8",9.49,"product_1","Returned"),
                                  Order("cus_2","Bartın","seller_3","2021-01-09T22:20:32.080+03:00","9",9.49,"product_1","Created"),
                                  Order("cus_5","Muş","seller_3","2021-01-10T22:20:32.065+03:00","10",8.49,"product_1","Created"),
                                  Order("cus_6","Artvin","seller_4","2021-01-11T22:20:32.004+03:00","11",9.49,"product_1","Returned"),
                                  Order("cus_5","İstanbul","seller_4","2021-01-12T22:20:32.067+03:00","12",9.00,"product_1","Created"))

    val priceChangeUtil: PriceChangeUtil = PriceChangeUtil(orders,"product_1")


    /**
     * Doğru sayıda fiyat değişikliği bulunuyor mu?
     * */
    @Test def priceChangesCountTest: Unit = {
      assertEquals(9,priceChangeUtil.getPriceChanges.size)
    }

    /**
     * 'getPriceChanges' metodu fiyat değişikliklerini sıralı döndürür, bu halde
     * ardışık iki fiyatın eşit olmaması gerekir.
     * */
    @Test def priceChangesTest: Unit = {

      val priceChanges = priceChangeUtil.getPriceChanges
      /**
       * Ardışık iki kaydın fiyatı eşit mi kontrolü yapar.
       * */
      assert(!Range(1,priceChanges.size).exists(i => priceChanges(i).price == priceChanges(i-1).price))
    }


    @Test def getDateTest:Unit = {

      assertEquals("2021-01-22 22:20:32",priceChangeUtil.getDate("2021-01-22T22:20:32.000+03:00"))
    }

    @Test def getDateEmptyStringTest:Unit = {

      assertEquals("",priceChangeUtil.getDate(""))
    }

    @Test def getDateNullStringTest:Unit = {

      assertEquals("",priceChangeUtil.getDate(null))
    }



  }
