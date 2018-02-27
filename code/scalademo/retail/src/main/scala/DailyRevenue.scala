import org.apache.spark.{SparkContext,SparkConf}

object DailyRevenue {
 def main(args: Array[String]) = {
//setMaster() local,yarn client, mesose
val conf = new SparkConf().setMaster(args(3)).setAppName("Daily Revenue")
val sc = new SparkContext(conf)

// Read orders and order_items

 //val baseDir = "F:\\DataScience\\spark\\data-master\\retail_db\\"
 val baseDir = args(0)
 val orders = sc.textFile(baseDir + "orders")
 val orderItems = sc.textFile(baseDir + "order_items")
 
// Filter for completed or closed orders

 orders.
  map(order => order.split(",")(3)).
  distinct.
  collect.
  foreach(println)
 
 val ordersFiltered = orders.
  filter(order => order.split(",")(3) == "COMPLETE" || order.split(",")(3) == "CLOSED")

// convert both filtered orders and order_items to key value pairs

 val ordersMap = ordersFiltered.
  map(order => (order.split(",")(0).toInt, order.split(",")(1)))
 
 val orderItemsMap = orderItems.
  map(oi => (oi.split(",")(1).toInt,(oi.split(",")(2).   toInt, oi.split(",")(4).toFloat)))
  
 
// Join the tow data sets

val ordersJoin = ordersMap.join(orderItemsMap)

//(order_id,(order_date,(order_item_product_id, order_item_subtotal)))
// Get daily revenue per product id
val ordersJoinMap = ordersJoin.map(rec => ((rec._2._1, rec._2._2._1), rec._2._2._2))

//((order_date, order_item_product_id), order_item_subtotal)

val dailyRevenuePerProductId = ordersJoinMap.
 reduceByKey((revenue, order_item_subtotal) => revenue + order_item_subtotal)

//((order_date, order_item_product_id), daily_revenue_per_productid)

// Load products from local file system and convert into RDD
import scala.io.Source
//val productsPath = "F:\\DataScience\\spark\\data-master\\retail_db\\products\\part-00000"
val productsPath = args(1)
val productsRaw = Source.
 fromFile(productsPath).
 getLines.
 toList
val products = sc.parallelize(productsRaw)

// Join daily revenue per product id with products to get daily revenue per product(by name)
val productsMap = products.
 map(product => (product.split(",")(0).toInt, product.split(",")(2)))

//((order_date, order_product_id), daily_revenue_per_product_id)
val dailyRevenuePerProductIdMap = dailyRevenuePerProductId.
 map(rec => (rec._1._2, (rec._1._1, rec._2)))
//(order_product_id, (order_date, daily_revenue_per_product_id))

val dailyRevenuePerProductJoin = dailyRevenuePerProductIdMap.join(productsMap)
//(order_product_id, ((order_date, daily_revenue_per_product_id), product_name))

// Sort the data by date in ascending order and by daily revenue per product in descending order
val dailyRevenuePerProductSorted = dailyRevenuePerProductJoin.
 map(rec => ((rec._2._1._1, rec._2._1._1), (rec._2._1._1, rec._2._1._2, rec._2._2))).
 sortByKey()
 //((order_date_asc, daily_revenue_per_product_id_desc), (order_date, daily_revenue_per_product,product_name))
 
// Get data to desired formate - order_date, daily_revenue_per_product, product_name
val dailyRevenuePerProduct = dailyRevenuePerProductSorted.
 map(rec => rec._2._1 + "," + rec._2._2 + "," + rec._2._3)
dailyRevenuePerProduct.take(10).foreach(println)
// Save final output into HDFS in avro file format as well as text file format

// HDFS location - text format /user/YOUR_USER_ID/daily_revenue_txt_scala
//val outputPath = "F:\\DataScience\\spark\\data-master\\retail_db\\daily_revenue_txt_scala"
val outputPath = args(2)
dailyRevenuePerProduct.saveAsTextFile(outputPath) 
 
 }
}