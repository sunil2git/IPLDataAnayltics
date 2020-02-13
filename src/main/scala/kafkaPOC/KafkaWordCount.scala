package kafkaPOC

import java.util.HashMap

import org.apache.kafka.clients.producer._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._

object KafkaWordCount {
  def main(args: Array[String]) {
   /* if (args.length < 4) {
      System.err.println("Usage: KafkaWordCount <zkQuorum><group> <topics> <numThreads>")
      System.exit(1)
    }*/

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(" IPL ")
      .getOrCreate()

    //val Array(zkQuorum, group, topics, numThreads) = args
    val sparkConf = new SparkConf().setAppName("KafkaWordCount")
    val sc=spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(2))
    //ssc.checkpoint("checkpoint")

    val kafkaStream=KafkaUtils.createStream(ssc,"localhost:2181","spark-streaming-consumer-group",Map("Hello-Kafka" -> 5))
//ssc,"localhost:2181","spark-streaming-consumer-group"
       kafkaStream.print()
    ssc.start()
    println("streaming started")
   // ssc.awaitTermination()
  }
}