package pureScala

import scala.collection.mutable._
import net.liftweb.json._

import scala.collection.mutable._
import net.liftweb.json._
import net.liftweb.json.Serialization.write
import net.liftweb.json.DefaultFormats
import net.liftweb.json._
import com.google.gson.Gson

import scala.collection.mutable._
import net.liftweb.json._
import net.liftweb.json.Serialization.write

import scala.collection.mutable


case class Person(name: String, address: Address)
case class Address(city: String, state: String)

case class emp(name: String, salary: Int,deptInfos: deptInfo)
case class deptInfo(loaction: String, deptno: Int)

case class stud(name: String,college: String,courseInfos: courseInfo)
case class courseInfo(courseName: String,required: prerequisities)
case class prerequisities(subject: String,exp :Int,score: Int)

object prac {

  def main(args: Array[String]): Unit = {

    println(Console.GREEN+"allaboutscala Exercises")
    println(Console.WHITE)

     /** Problem 1: Create a Scala program to reverse, and then format to upper case,
      * the given String: "http://allaboutscala.com/scala-exercises
      */

    val str= "http://allaboutscala.com/scala-exercises"

    val uppStr= str.reverse
   // println(uppStr.toUpperCase)

     /** Creating a json */
    val donutName = "Vanilla Donut"
    val quantityPurchased = 10
    val price = 2.50
    val donutJson =
      s"""
         |{
         |"donut_name":"$donutName",
         |"quantity_purchased":"$quantityPurchased",
         |"price":$price
         |}
      """.stripMargin
   // println(donutJson)

    /** Reading from console */

/*    val name = readLine("Enter your name: ")
    println("Enter your age: ")
    val age = readInt()
    println(Console.BOLD)
    print("Name: ")
    print(Console.UNDERLINED)
    print(name)
    println(Console.BOLD)
    print("Age: ")
    print(Console.RESET)
    print(age)
    println(Console.BLUE + " now it's really blue!")*/


   val group1 : List[Int] = List(100,200,300,600)
    val group2 : List[Int] = List(150,250,350,650)

    val group1Max =group1.max
    val group2Max =group2.max

    var group1st= 0
    var group2st= 0


    if ( group1Max > group2Max)
    {
      group1st= group1Max + (group1.sorted.apply(1))
      group2st=group2Max + (group2.min)

    }
    else
    {
      group1st= group2Max + (group2.sorted.apply(1))
      group2st=group1Max + (group1.min)
    }

    //  println("group1st =  "+ group1st + " group2st = " +group2st )
    val a:Option[Int] = Some(5)
    val b:Option[Int] = None
  //  println(a.getOrElse(0))
   // println(b.getOrElse(0))


  /** Pattern series */
    /** 1. pyramid */

  /*  var row=0
    var star=0
    var line=5
    for( row <- 1 to 5 )
       if(row <= line)
      {
        for( star <- 1 to 5 )
          if(star<= row)
          {
            print("*")
          }
        println("")
      }*/

    /** 2. Pyramid a using numbers */

  /*  var row=0
    var num=0
    var line=5
    for( row <- 1 to 5 )
      if(row <= line)
      {
        for( num <- 1 to 5 )
          if(num<= row)
          {
            print(num)
          }
        println("")
      }*/

 /** Pyramid using Alphabet */
  /*    var row=0
  var star=0
  var line=5
    var alpha='A'
  for( row <- 1 to 5 )
     if(row <= line)
    {
      for( star <- 1 to 5 )
        if(star<= row)
        {
          print(alpha)
        }
      alpha = (alpha.toInt + 1).toChar
      println("")
    }
*/

/*
  println("test data ")
  var row=0
  var star=0
  var numRow=5
  for( row <- 5 to 1 )
     if( numRow >=1)
    {
      for( star <- 1 to 5 )
        if(star<= row)
        {
          print("*")
        }
      println("")
      numRow -= 1
    }*/

 case class Person(name: String, address: Address)
    case class Address(city: String, state: String)

  /*val p = Person("Alvin Alexander", Address("Talkeetna", "AK"))
    // create a JSON string from the Person, then print it
    implicit val formats = DefaultFormats
   val jsonString = write(p)
    println(jsonString)*/

    val jsonString ="""
{
"accounts": [
{ "emailAccount": {
"accountName": "YMail",
"username": "USERNAME",
"password": "PASSWORD",
"url": "imap.yahoo.com",
"minutesBetweenChecks": 1,
"usersOfInterest": ["barney", "betty", "wilma"]
}},
{ "emailAccount": {
"accountName": "Gmail",
"username": "USER",
"password": "PASS",
"url": "imap.gmail.com",
"minutesBetweenChecks": 1,
"usersOfInterest": ["pebbles", "bam-bam"]
}}
]
}
"""
    implicit val formats = DefaultFormats

    case class EmailAccount(
                             accountName: String,
                             url: String,
                             username: String,
                             password: String,
                             minutesBetweenChecks: Int,
                             usersOfInterest: List[String]
                           )

    val json = parse(jsonString)
    val elements = (json \\ "emailAccount").children
    for (acct <- elements) {
      val m = acct.extract[EmailAccount]
  //    println(s"Account: ${m.url}, ${m.username}, ${m.password}")
 //     println(" Users: " + m.usersOfInterest.mkString(","))
    }

    /** cook book:   Creating a JSON String from a Scala Object */
   // prac 1
    val p = Person("Sunil", Address("Fatima nagar", "Wanowari"))
    // create a JSON string from the Person, then print it
    val jsonString1 = write(p)
  //  println(jsonString1)

    //prac 2
      val e =emp("Sunil",999999999,deptInfo("pune",20))
    val empJson=write(e)
   // println(empJson)

    //prac3
   val s=stud("Sunil","Indira college",courseInfo("Data Science",prerequisities("Data Engineer",3,94)))
    val studJson=write(s)
    //println(studJson)



     case class emp1(id: Int,name: String,salary: Int,dept: Int)


    val empData= Seq(emp1(1,"sunil",999999999,1),emp1(1,"ravi",100000,2))
    println(empData.map(_.name equals("sunil")))

    println(empData.map(_.dept))



  }
}
