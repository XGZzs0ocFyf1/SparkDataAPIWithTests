import DemoDataSet.spark
import org.apache.spark.sql.functions.{broadcast, col, count, desc_nulls_first, max, mean, min, round}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}

import java.util.Properties

object DemoDataFrame extends App {
  import ReadWriteUtils._

  implicit val spark = SparkSession
    .builder()
    .appName("Introduction to RDDs")
    .config("spark.master", "local")
    .getOrCreate()

  def processTaxiData(taxiFactsDF: DataFrame, taxiZoneDF: DataFrame): DataFrame =
    taxiFactsDF
      .join(broadcast(taxiZoneDF), col("DOLocationID") === col("LocationID"), "left")
      .groupBy(col("Borough"))
      .agg(
        count("*").as("total trips"),
        round(min("trip_distance"), 2).as("min distance"),
        round(mean("trip_distance"), 2).as("mean distance"),
        round(max("trip_distance"), 2).as("max distance")
      )
      .orderBy(col("total trips").desc)


  val taxiFactsDF: DataFrame = readParquet("src/main/resources/yellow_taxi_jan_25_2018")
  val taxiZoneDF: DataFrame = readCSV("src/main/resources/taxi_zones.csv")

  val result: DataFrame = processTaxiData(taxiFactsDF, taxiZoneDF)

  result.show()

}