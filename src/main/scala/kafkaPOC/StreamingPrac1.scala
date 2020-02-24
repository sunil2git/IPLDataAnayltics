package kafkaPOC

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.datastax.spark.connector._
import com.datastax.spark.connector._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.cassandra._
import org.apache.spark.streaming.dstream.ConstantInputDStream

case class Employee(id: Int, dept: Int, name: String, salary: Int)


object StreamingPrac1 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("kafka-test").set("spark.cassandra.connection.host", "127.0.0.1")
    val ssc = new StreamingContext(conf, Seconds(5))
   // Logger.getLogger("org").setLevel(Level.OFF)
 //   Logger.getLogger("akka").setLevel(Level.OFF)
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("spark-Streaming")
      .config("spark.cassandra.connection.host", "127.0.0.1")
      .getOrCreate()
    spark.sparkContext.setLogLevel("OFF")
    // Create a DStream that will connect to hostname:port, like localhost:9999
    //  val lines = ssc.socketTextStream("localhost", 9999)
    //   val lines =  ssc.cassandraTable("mykeyspace", "users").select("fname", "lname").where("lname = ?", "yu")
    val lines = spark.sparkContext.cassandraTable("testkeyspace", "emp").select("id", "name","salary","dept")



    val dstream = new ConstantInputDStream(ssc, lines)

    dstream.foreachRDD{ rdd =>
      // any action will trigger the underlying cassandra query, using collect to have a simple output
      println(rdd.collect.mkString("\n"))
    }

    val tempEmp=spark.sparkContext.cassandraTable("temp", "tempemp").select("id", "name","salary","dept")
    val writeStream =new ConstantInputDStream(ssc, tempEmp)

    dstream.foreachRDD(rdd => rdd.saveToCassandra("temp","tempemp",SomeColumns("id","name","salary","dept")))


  //  INSERT INTO emp (Id,Name,Salary,Dept) VALUES(9,'YESSSS',1000000,1);


    ssc.start()             // Start the computation
    ssc.awaitTermination()
  }
}