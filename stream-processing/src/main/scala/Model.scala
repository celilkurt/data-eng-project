import java.sql.Timestamp

case class Order(customer_id: String,
                 location: String,
                 seller_id: String,
                 order_date: String,
                 order_id: String,
                 price: Double,
                 product_id: String,
                 status: String){
  def orderDateToTimeStamp: Long = Timestamp.valueOf(order_date.substring(0, 23).replace('T', ' ')).getTime
}


case class ProductStats(seller_count: Long, product_count: Long, location: String, date: Timestamp)

