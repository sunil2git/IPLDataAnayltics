package empPrac
import java.io.{File, PrintWriter}
import java.text.SimpleDateFormat
import java.util.regex.Pattern
import java.util.{Calendar, Date}

import org.apache.spark.sql.expressions.Window
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{sum, _}
import shapeless.ops.coproduct.Folder

import scala.util.matching.Regex

object test2 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("TEST EMP")
      .getOrCreate()
    import spark.implicits._

    println(" test scala program")

    case class Song(title:String,artist:String,track:Int)
    val stay=Song("Stay","Inna",4)
    //  println(s"Hello, 2+3 is ${2+3}")

    val df = Seq(
      (1, "2014/01/01 23:00:01"),
      (1, "2014/11/31 12:40:32"),
      (1, "2016/12/29 09:54:00"),
      (1, "2016/05/09 10:12:43")).toDF("id", "date")
   // df.show()

    val emp = Seq(
      (1,"sk",5000,1),
      (2,"ss",4000,1),
      (3,"rk",3400,2),
      (4,"rr",4000,2),
      (5,"pk",6000,3),
      (6,"pp",4500,3),
      (7,"pp",7000,4),
      (8,"sam",7000,4),
      (8,"rick",7500,4)).toDF("id","name","sal","dept")

    val dept=Seq((1,"sales","pune"),(2,"marketing","mumbai"),(3,"IT","hyd"),(4,"Management","delhi")).toDF("deptIt","deptName","city")

    emp.show()
    dept.show()

    emp.join(dept,$"dept"===$"deptIt").where($"city"==="pune").show()
    emp.orderBy($"sal".desc).limit(1).show()

    val row_dept = emp.select(emp("*"),row_number().over(Window.partitionBy(col("dept")).orderBy(col("sal").desc)).alias("Row_Num"))
    //row_dept.
    row_dept.show()
    row_dept.filter( row_dept("Row_Num")==="3").show()


    def salaryIncrement(df : DataFrame) =
    {
      val result=df.withColumn("updatedSal", when($"dept"=== "1", ($"sal"*.3+$"sal"))
        .when($"dept"=== "2", ($"sal"*.35+$"sal"))
        .when($"dept"=== "3", ($"sal"*.4+$"sal"))
        .otherwise($"sal"))
      result
    }

   val incrementSalDf=salaryIncrement(emp)

 /** TO read the n number of part files */
   // val data =spark.read.csv("/Users/acs/Documents/sparkData/data3/part*").toDF("id","name","sal","dept","new_Sal")
   //  data.orderBy(asc("id"))show()

    /**  ADD FOLDER NAME WITH DATE AND DOMAIN_NAME AND NUMBER  */

    val autoFolder=7
    val todayTime=Calendar.getInstance().getTime()
   // println(todayTime)

    val dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S")
    var submittedDateConvert = new Date()
    val currentDate  = dateFormatter.format(submittedDateConvert)
    val domain=folderName.domainName
    val autofolderName : String =s"$domain$currentDate"
    val path ="/Users/acs/Documents/sparkData"
    val files = new File(path).list.toList
    val folderNum=files.length-1

    //incrementSalDf.repartition(1).write.option("header", "true").csv(s"/Users/acs/Documents/sparkData/"+autofolderName)
    //println(files)

  }
}

object folderName{
  val domainName="EmpTest"
}
