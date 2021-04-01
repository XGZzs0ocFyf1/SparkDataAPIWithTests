import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object DemoRDD extends App {
  val spark = SparkSession
    .builder()
    .appName("Introduction to RDDs")
    .config("spark.master", "local")
    .getOrCreate()

  val context = spark.sparkContext

  import spark.implicits._
  import model._

  val taxiZoneRDD: RDD[TaxiZone] = context
    .textFile("src/main/resources/taxi_zones.csv")
    .map(l => l.split(","))
    .filter(t => t(3).toUpperCase() == t(3))
    .map(t => TaxiZone(t(0).toInt, t(1), t(2), t(3)))

  val taxiFactsDF: DataFrame =
    spark.read
      .load("src/main/resources/yellow_taxi_jan_25_2018")

  val taxiFactsDS: Dataset[TaxiRide] =
    taxiFactsDF
      .as[TaxiRide]

  val taxiFactsRDD: RDD[TaxiRide] =
    taxiFactsDS.rdd


  val mappedTaxiZoneRDD: RDD[(Int, String)] = taxiZoneRDD
    .map(z => (z.LocationID, z.Borough))

  val mappedTaxiFactRDD: RDD[(Int, Int)] =
    taxiFactsRDD
      .map(x => (x.DOLocationID, 1))

  val joinedAndGroupedRDD =
    mappedTaxiFactRDD
      .join(mappedTaxiZoneRDD)
      .map {
        case (_, (cnt, bor)) => (bor, cnt)
      }
      .reduceByKey(_ + _)
      .sortBy(_._2, false)
      .take(10)
      .foreach(x => println(x))
}
