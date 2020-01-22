package IPL

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import scala.reflect.internal.util.TableDef.Column


object IplAnaytics {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(" IPL ")
      .getOrCreate()

    println(" IPL Data ")

    val rawData= spark.read.csv("src/Docs&Data/match.csv")
      .toDF(columnNames.MatchNo,columnNames.Year,columnNames.City,columnNames.Date,columnNames.Team1,columnNames.Team2,columnNames.TossWin,
        columnNames.FieldOrBat,columnNames.MatchResult,columnNames.Unknown1,columnNames.Winner,columnNames.Unknown2,columnNames.Unknown3,columnNames.Man_Of_The_Match,
        columnNames.Stadiuam,columnNames.Umpire1,columnNames.Umpire2,columnNames.Unknown4)

    val data=rawData.drop(columnNames.Unknown1,columnNames.Unknown2,columnNames.Unknown3,columnNames.Unknown4)
   // data.printSchema()


    /** 1. who wins the most number of ipl final?*/

    data.createTempView("myTable")

    val iplWinner= spark.sql("select winner,date from myTable where date in (select max(Date) from myTable group by year)")
    iplWinner.show()
    iplWinner.createTempView("winnerTeam")
   //spark.sql("select count(winner) as cnt,winner from winnerTeam group by winner having cnt=(select max(cnt) from (select count(winner) as cnt,winner from winnerTeam group by winner))").show(truncate = false)

    iplWinner.groupBy(columnNames.Winner).agg(count("*").alias("cnt"))
    val winnerCount=iplWinner.groupBy(columnNames.Winner).agg(count("*").alias("cnt"))

    val maxCount=winnerCount.agg(max("cnt").alias("c"))

      val cs=maxCount.select("c").collect()
    val totalCount=cs.mkString.replaceAll("[\\[\\]]","")

   /* winnerCount.filter(winnerCount("cnt")===two).show()*/

    /** 2> who wins the most number of ipl matches?  */

     val winList=spark.sql("select winner,count(winner) as cnt from mytable group by winner")
     winList.createTempView("winnerTeam1")
     spark.sql("select * from winnerTeam1 where cnt = (select max(cnt) from winnerTeam1)")

     val  winnerCount1= data.groupBy(columnNames.Winner).agg(count("*").alias("cnt"))

     val maxCount1=winnerCount1.agg(max("cnt").alias("c"))
     val cs1=maxCount1.select("c").collect()

     val totalCount1=cs1.mkString.replaceAll("[\\[\\]]","")
   /*  winnerCount1.filter(winnerCount1("cnt")===totalCount1).show()*/

    /** 3> top 3 team who win most number of ipl matches? */

  //winnerCount1.orderBy(col("cnt").desc).limit(3).show()

  /** 4> in which city the most number of the matches happens??  */

    /* val city= data.groupBy(columnNames.City).agg(count("*").alias("cnt")).orderBy(col("cnt").desc).limit(1).show() */

    /** 5> year-wise number of tie matches */

   // spark.sql("select team1,team2,result,year from mytable where where result= 'tie'").show()

  /* data.select(col(columnNames.Team1),col(columnNames.Team2),col(columnNames.MatchResult),col(columnNames.Year)).filter(col(columnNames.MatchResult)==="tie").show() */

    /** 6> top team who win most of the tosses */
   // spark.sql("select count(toss_win) as cnt,toss_win from mytable group by toss_win order by cnt desc").limit(1).show()

    //data.groupBy(columnNames.TossWin).agg(count("*").alias("cnt")).orderBy(col("cnt").desc).limit(1).show()

    /** 7> city and stadium wise matches number */

  //  spark.sql("select count(city),stadiuam,city from mytable group by city,stadiuam").show()

    /** 8> second-highest team who played won matches */
      val wc1=winnerCount1.orderBy(col("cnt").desc).withColumn("test", lit("t"))

  val listOfTotalMatchWin= wc1.select(winnerCount1("*"),row_number().over(Window.partitionBy(col("test")).orderBy(col("cnt").desc)).alias("Row_Num"))
//    listOfTotalMatchWin.filter(listOfTotalMatchWin("Row_Num")==="2").show()

    /** 9> UDF for considering "Deccan Chargers" is "Sunrisers Hyderabad" and "Pune Warriors" is "Rising Pune Supergiants" */

   /** 10> top player who won the most number of man of the match */

   // spark.sql("select count(man_of_the_match) as cnt,man_of_the_match from mytable group by man_of_the_match order by cnt desc").limit(1).show()

   // data.groupBy(columnNames.Man_Of_The_Match).agg(count("*").alias("cnt")).orderBy(col("cnt").desc).limit(1).show()

    /** 1> top 4 team who played the least number of matches */

    val totalPlayed=spark.sql("select * from (select count(team1) as cnts,team1 from mytable group by team1 UNION  ALL select count(team2),team2 from mytable group by team2 ) as data")
    totalPlayed.createTempView("totalPlayed")
  //  spark.sql("select sum(cnts) as sumCount,team1 as team from totalPlayed group by team1 order by sumCount").limit(4)show()
   spark.sql("select * from (select count(team2),team2 from mytable group by team2) as t2  ")
    spark.sql("select * from (select count(team1),team1 from mytable group by team1) as t1  ")

    val team1= data.groupBy(columnNames.Team1).agg(count("*").alias("cnt")).orderBy(col("cnt").desc)
    val team2= data.groupBy(columnNames.Team2).agg(count("*").alias("cnt")).orderBy(col("cnt").desc)

      val totalPlayed1=  team1.unionAll(team2)
    // totalPlayed1.groupBy(columnNames.Team1).agg(sum("cnt").alias("playedMatch")).orderBy(col("playedMatch")).limit(4).show()

    /** 2> top 4 team who played the max number of matches */

   // totalPlayed1.groupBy(columnNames.Team1).agg(sum("cnt").alias("playedMatch")).orderBy(col("playedMatch").desc).limit(4).show()

    /** 3> top 4 team who played the max number of matches in final */

    val finalmatchTeams=spark.sql("select team1,team2,date from myTable where date in (select max(Date) from myTable group by year)")
    finalmatchTeams.createTempView("finalmatchTeams")
    val finalMatch=spark.sql("select * from (select count(team1) as cnts,team1 from finalmatchTeams group by team1 UNION  ALL select count(team2),team2 from finalmatchTeams group by team2 ) as data")
    finalMatch.createTempView("finalMatch")
    //spark.sql("select sum(cnts) as sumCount,team1 as team from finalMatch group by team1 order by sumCount desc").limit(4)show()

   //---
    val finalist1 = spark.sql("select count(team1) as cnt,team1 from myTable where date in (select max(Date) from myTable group by year) group by team1")
    val finalist2 = spark.sql("select count(team2) as cnt,team2 from myTable where date in (select max(Date) from myTable group by year)  group by team2")

    val finalists=finalist1.unionAll(finalist2)

    //finalists.groupBy(col("team1").alias("team")).agg(sum("cnt").alias("finalMatchplayed")).orderBy(col("finalMatchplayed").desc).limit(4).show()

    /** 4> which team played most ties matches */

    val tieTeam1= spark.sql("select team1,count(team1) as cnt from mytable  where  result= 'tie' group by team1")
    val tieTeam2= spark.sql("select team2,count(team2) as cnt from mytable  where result= 'tie' group by team2")

    val tieMatch=tieTeam1.unionAll(tieTeam2)

   // tieMatch.groupBy(col("team1").alias("Team")).agg(sum("cnt").alias("NoOfMatches")).orderBy(col("NoOfMatches").desc).limit(1).show()

    /**   5> second-highest team who played most matches */

     val hihgestMatch=totalPlayed1.groupBy(columnNames.Team1).agg(sum("cnt").alias("playedMatch")).orderBy(col("playedMatch").desc)
       .withColumn("test", lit("t"))
  //  hihgestMatch.show()
   // val listOfMachesPlayed=hihgestMatch.select(hihgestMatch("*"),row_number().over(Window.partitionBy(col("test")).orderBy(col("playedMatch").desc)).alias("Row_Num"))
   // listOfMachesPlayed.filter(listOfMachesPlayed("Row_Num")==="2").show()

  }
}

object folderName{
  val domainName="EmpTest"
}

object columnNames {
  val MatchNo = "match_no"
  val Year = "year"
  val City = "city"
  val Date = "date"
  val Team1 = "team1"
  val Team2 = "team2"
  val TossWin = "toss_win"
  val FieldOrBat = "field_or_bat"
  val MatchResult = "result"
  val Unknown1 = "unknown1"
  val Winner = "winner"
  val Unknown2 = "unknown2"
  val Unknown3 = "unknown3"
  val Man_Of_The_Match = "man_of_the_match"
  val Stadiuam = "stadiuam"
  val Umpire1 = "umpire1"
  val Umpire2 = "umpire2"
  val Unknown4 = "unknown4"
}
