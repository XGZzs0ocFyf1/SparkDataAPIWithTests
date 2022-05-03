import org.apache.spark.sql.{DataFrame, SparkSession}

object ReadWriteUtils {
  def readParquet(path: String)(implicit spark: SparkSession): DataFrame = spark.read.load(path)
  def readCSV(path: String)(implicit spark: SparkSession):DataFrame =
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)

  def read(sourceType: SourceType.Value, path: String)(implicit spark: SparkSession): DataFrame = {
    sourceType match {
      case SourceType.PARQUETE => readParquet(path)
      case SourceType.CSV => readCSV(path)
      case _ => throw new RuntimeException(s"Unknown file type: $sourceType")
    }
  }


}

object SourceType extends  Enumeration {
  val PARQUETE = Value("parquet")
  val CSV = Value("csv")

}