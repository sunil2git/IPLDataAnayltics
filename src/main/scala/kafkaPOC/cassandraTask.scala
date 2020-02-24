package kafkaPOC

import org.apache.spark.sql.{SaveMode, SparkSession}
import com.datastax.spark.connector._
import org.apache.log4j.{Level, Logger} //Loads implicit functions


object cassandraTask {
  def main(args: Array[String]): Unit = {


    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("casssandra-POC")
      .config("spark.cassandra.connection.host", "127.0.0.1")
      .getOrCreate()

    val empDF = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "emp", "keyspace" -> "testkeyspace")).load.cache()

    empDF.show()
    import com.datastax.spark.connector._
    import spark.implicits._


    empDF.write.format("org.apache.spark.sql.cassandra").options(Map("table" -> "tempemp", "keyspace" -> "testkeyspace")).mode(SaveMode.Append).save()

    val dataSet=spark.sparkContext.parallelize(Seq("8", 3,"test",90909))

   // dataSet.saveToCassandra("testkeyspace","tempemp")

    println("Data Insertion Done ")


  }
}
