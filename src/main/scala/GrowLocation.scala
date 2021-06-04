
import java.sql.Date

import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}
import org.apache.spark.sql.functions._
// Serial	Latitude	Longitude	Type	SensorType	Code	BeginTime	EndTime
object GrowLocation extends App {

  val spark = SparkSession.builder()
    .appName("Datasets")
    .config("spark.master", "local")
    .getOrCreate()

  case class Location(
                       Serial: String,
                       Latitude: Double,
                       Longitude: Double,
                       Type: String,
                       SensorType: String,
                       Code: String,
                       BeginTime: String,
                       EndTime: String
                     )

  val growLocationDF: DataFrame = spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("src/main/resources/data/grow_location.csv")


  import spark.implicits._
  val growLocationDS = growLocationDF.as[Location]

  growLocationDS.filter(_.Longitude != 0)

  // map, flatMap, fold, reduce, for comprehensions ...

  growLocationDS.toDF().write
    .format("csv")
    .option("header", "true")
    .option("sep", "\t")
    .save("src/main/resources/data/grow_location_out.csv")

}