import org.apache.spark.sql.functions.{broadcast, col}
import org.apache.spark.sql.{DataFrame, SparkSession}

object DemoDataFrame extends App {
  val spark = SparkSession
    .builder()
    .appName("Introduction to RDDs")
    .config("spark.master", "local")
    .getOrCreate()

  val taxiFactsDF: DataFrame = spark.read
    .load("src/main/resources/yellow_taxi_jan_25_2018")

//  taxiFactsDF.printSchema()

  val taxiZoneDF: DataFrame = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/taxi_zones.csv")

//  taxiZoneDF.printSchema()

  taxiFactsDF
    .join(broadcast(taxiZoneDF), col("DOLocationID") === col("LocationID"), "left")
    .groupBy(col("Borough"))
    .count()
    .orderBy(col("count").desc)
    .show()
}