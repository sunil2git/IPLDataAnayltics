package kafkaPOC

package kafkaPOC

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}



object kafkaCassandraTask {
  def main(args: Array[String]) {
    val broker = "localhost:9092"
    val groupId = "GRP1"
    val topics = "Hello-Kafka"

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("TEST-kafka")
    val ssc = new StreamingContext(sparkConf, Seconds(4))
    val sc = ssc.sparkContext
    sc.setLogLevel("OFF")

    val topicSet = topics.split(",").toSet
    val kafkaPram = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> broker,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
    )

    val messages = KafkaUtils.createDirectStream[String, String](
      ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topicSet, kafkaPram)
    )

    val line = messages.map(_.value)
    val words = line.flatMap(_.split(" "))
    val wordCount = words.map(x => (x, 1)).reduceByKey(_ + _)

    wordCount.print()
    ssc.start()
    ssc.awaitTermination()
  }
}