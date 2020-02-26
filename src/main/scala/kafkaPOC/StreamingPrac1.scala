package kafkaPOC


import java.text.SimpleDateFormat

import com.datastax.spark.connector._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.streaming.dstream.ConstantInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

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

    val time = System.currentTimeMillis()
    val formatter = new SimpleDateFormat("dd-MM-yyyy hh:mm:sss")

    import java.util.Calendar
    val calendar = Calendar.getInstance()
    calendar.setTimeInMillis(time)
    val date_time = formatter.format(calendar.getTime())

    val emp = spark.sparkContext.cassandraTable("testkeyspace", "emp").select("id", "name", "salary", "dept")
    val dstream = new ConstantInputDStream(ssc, emp)


    dstream.foreachRDD { rdd =>
      // any action will trigger the underlying cassandra query, using collect to have a simple output
      println(Console.GREEN + rdd.collect.mkString("\n"))
      println(Console.BLUE + "===============================================================================")
    }

    val test = dstream.map { r =>
      val id = r.columnValues(0)
      val name = r.columnValues(1)
      val sal = r.columnValues(2)
      val dept = r.columnValues(3)
      (id, name, sal, dept, date_time)
    } // adding column CreatedOn using tuple in RDD

    test.foreachRDD(rdd => rdd.saveToCassandra("testkeyspace", "tempemp", SomeColumns("id", "name", "salary", "dept", "createdon")))
    // writing in the temp.tempemp with extra column CreateOn.

    /*val tempEmp = spark.sparkContext.cassandraTable("temp", "tempemp").select("id", "name", "salary", "dept")
    val writeStreamEmp = new ConstantInputDStream(ssc, tempEmp)

    writeStreamEmp.foreachRDD(rdd => rdd.saveToCassandra("testkeyspace", "emp", SomeColumns("id", "name", "salary", "dept")))

    writeStreamEmp.foreachRDD { rdd =>
      // any action will trigger the underlying cassandra query, using collect to have a simple output
      println(Console.RED + rdd.collect.mkString("\n"))
    }
*/
    //  INSERT INTO tempemp (Id,Name,Salary,Dept,createdon) VALUES(6,'testStreaming',1000000,1,dateof(now()));


    ssc.start() // Start the computation
    ssc.awaitTermination()
  }
}