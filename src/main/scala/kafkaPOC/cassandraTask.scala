package kafkaPOC

import java.text.SimpleDateFormat

import com.datastax.spark.connector._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession //Loads implicit functions


case class emp(id: Int, name: String, salary: Int, dept: Int)

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

    val emp = spark.sparkContext.cassandraTable("testkeyspace", "emp").select("id", "name", "salary", "dept")


    val data = spark.sparkContext.parallelize(Seq(("sun", 1), ("mon", 2), ("tue", 3), ("wed", 4), ("thus", 5)))
    val data1 = spark.sparkContext.parallelize(Seq ((9,"Apriliya", 50000,3),(10,"tiktok", 50000,4)))

    val time = System.currentTimeMillis()
    val formatter = new SimpleDateFormat("dd-MM-yyyy hh:mm:sss")

    import java.util.Calendar
    val calendar = Calendar.getInstance()
    calendar.setTimeInMillis(time)
    val date_time = formatter.format(calendar.getTime())


    println(date_time)
  println("::::::::::::  emp :::::::::::: ")
    emp.foreach(println)

    println("::::::::::::  test :::::::::::: ")

    val test = emp.map { r =>
      val id = r.columnValues(0)
      val name = r.columnValues(1)
      val sal = r.columnValues(2)
      val dept = r.columnValues(3)
      (id, name, sal,dept,date_time)
    }
   test.foreach(println)

    // empDF.write.format("org.apache.spark.sql.cassandra").options(Map("table" -> "tempemp", "keyspace" -> "testkeyspace")).mode(SaveMode.Append).save()

    test.saveToCassandra("testkeyspace", "tempemp", SomeColumns("id","name","salary","dept","createdon"))
        println("Data Insertion Done ")


  }
}
