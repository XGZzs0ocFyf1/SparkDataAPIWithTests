import Helpers._
import SourceType.{CSV, PARQUETE}
import model.{TaxiRide, TaxiZone}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object L17HW {

  implicit val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()
  val taxiPath = "src/main/resources/yellow_taxi_jan_25_2018"
  val zonesPath = "src/main/resources/taxi_zones.csv"

  import ReadWriteUtils._
  import spark.implicits._

  val taxiFactsDF: DataFrame = read(PARQUETE, taxiPath)
  val taxiZones: DataFrame = read(CSV, zonesPath)

  def mostPopularDistance(taxiFactsDF: DataFrame): RDD[(Double, Int)] = {
    import spark.implicits._
    val taxiFactsDS: Dataset[TaxiRide] = taxiFactsDF.as[TaxiRide]
    val taxiFactsRDD: RDD[TaxiRide] = taxiFactsDS.rdd
    taxiFactsRDD
      .map(x => (x.trip_distance, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)
  }

  def mostPopularBoroughs(taxiFactsDF: DataFrame, taxiZonesDF: DataFrame): RDD[(String, Int)] = {

    val taxiFactsDS: Dataset[TaxiRide] =
      taxiFactsDF
        .as[TaxiRide]

    val taxiFactsRDD: RDD[TaxiRide] =
      taxiFactsDS.rdd


    val taxiZoneDs = taxiZonesDF.as[TaxiZone].rdd.map(z => (z.LocationID, z.Borough))

    val mappedTaxiFactRDD: RDD[(Int, Int)] =
      taxiFactsRDD
        .map(x => (x.DOLocationID, 1))

    mappedTaxiFactRDD
      .join(taxiZoneDs)
      .map {
        case (_, (cnt, bor)) => (bor, cnt)
      }
      .reduceByKey(_ + _)
      .sortBy(_._2, false)
  }

  def mostPopularTime(taxiFactsDF: DataFrame) = {
    val facts: RDD[TaxiRide] = taxiFactsDF.as[TaxiRide].rdd
    import spark.implicits._
    taxiFactsDF.show(10)
    facts.map(x => (x.tpep_pickup_datetime.toLdt(), 1))
      .map(x => (x._1.changeTimeToQuoters, x._2)) //с четвертями часа уже поинтересней результат смотреть
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)
      .toDF("high time", "ht cnt")
  }


  def persist(df: DataFrame, url: String, user: String, pwd: String): Unit =
    df.write
      .format("jdbc")
      .option("url", url)
      .option("dbtable", "tracking")
      .option("user", user)
      .option("password", pwd)
      .save()
}



