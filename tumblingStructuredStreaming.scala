package MiscTest

import java.sql.Timestamp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object tumblingStructuredStreaming {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("yarn")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstarp.servers",":6667")
      .option("subscribe","topicname")
      .load()

    // define case class for schema for incoming data

    case class CarEvent(carID: String, speed: Option[Int], acceleration: Option[Int], timestamp: Timestamp)

    def formatInput(inpt: String): CarEvent = {
      val parts = inpt.split(",")
      CarEvent(parts(0), Some(Integer.parseInt(parts(1))),
        Some(Integer.parseInt(parts(2))),new Timestamp(parts(3).toLong))

    }

    val cars = df
    .selectExpr("CAST(value AS STRING)")
      .map(r => formatInput(r.getString(0)))

    //normal Aggregation
    val aggregator = cars
      .groupBy("carID")
      .agg(avg("speed"))

    //Tumbling (window and sliding is same then Tumbling)

    val aggregatorWindow = cars
      .groupBy(
        window($"timestamp", "5 minutes"), $"carID"
      )
      .agg(avg("speed").alias(("speed")))
      .where("speed > 70")

    //a sliding window of size 4 seconds that slides every 2 seconds can be created
    // using cars.groupBy(window($"timestamp","4 seconds","2 seconds"), $"carId")

 //SINK TO KAFKA :-

    val writeToKafka = aggregatorWindow
      .selectExpr("CAST(carId AS STRING) AS key", "CAST(speed AS STRING) AS value")
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers","localhost:9092")
      .option("topic", "fastcars")
      .option("checkpointLocation", "/tmp/sparkcheckpoint/")
      .queryName("kafka spark streaming kafka")
      .outputMode("update")
      .start()

    //Watermark for delayed Data :-
    val aggregates = cars
      .withWatermark("timestamp", "3 seconds") //set watermark using timestamp filed with a max delay of 3s.
      .groupBy(window($"timestamp","4 seconds"), $"carId")
      .agg(avg("speed").alias("speed"))
      .where("speed > 70")

  }

}
