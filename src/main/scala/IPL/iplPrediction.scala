package IPL

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


import org.apache.spark.sql.SQLContext
import org.apache.spark.ml.classification.{BinaryLogisticRegressionSummary, LogisticRegression}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{VectorAssembler, StringIndexer}
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext

object iplPrediction {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark=SparkSession
      .builder()
      .master("local[*]")
      .appName("IPL Prediction")
      .getOrCreate()

    println(" IPL Prediction Model ")

    val rawData= spark.read.csv("src/Docs&Data/match.csv")
      .toDF(columnNames.MatchNo,columnNames.Year,columnNames.City,columnNames.Date,columnNames.Team1,columnNames.Team2,columnNames.TossWin,
        columnNames.FieldOrBat,columnNames.MatchResult,columnNames.Unknown1,columnNames.Winner,columnNames.Unknown2,columnNames.Unknown3,columnNames.Man_Of_The_Match,
        columnNames.Stadiuam,columnNames.Umpire1,columnNames.Umpire2,columnNames.Unknown4)

    val data=rawData.drop(columnNames.Unknown1,columnNames.Unknown2,columnNames.Unknown3,columnNames.Unknown4)

    data.createTempView("myTable")

    /** 1> predict which team needs to play 1st and which team needs to play second that his winning possibilities increases */

    val batFieldCount=spark.sql("select t1.winner,t1.bat,t2.field from (select winner,count(winner) as bat from mytable where field_or_bat='bat' group by winner) as t1,(select winner,count(winner) as field from mytable where field_or_bat='field' group by winner) as t2  where t1.winner=t2.winner")
     batFieldCount.createTempView("batFieldCount")

    spark.sql("select winner,CASE WHEN bat > field THEN 'most win when bat first' ELSE 'most win when field first' END AS batFieldPrediction from batFieldCount" ).show(truncate = false)

    val batWinner=data.groupBy(columnNames.Winner,columnNames.FieldOrBat).agg(count("*").alias("cnt"))
    val batWinnerCount=batWinner.filter(batWinner(columnNames.FieldOrBat)==="bat")
    val fieldWinnerCount=batWinner.filter(batWinner(columnNames.FieldOrBat)==="field")

    val team=batWinnerCount.as("b").join(fieldWinnerCount.as("f"),col("b.winner")===col("f.winner"))
      .select(col("b.winner"),col("b.cnt").alias("batCount"),col("f.cnt").alias("fieldCount"))

    //team.select(col("*")).withColumn("result",when(col("batCount") > col("fieldCount"),s"most win when bat first").otherwise(" most win when field first")).show(truncate = false)

    /** 2> which team should play on which ground that his winning possibilities increases */

   //data.groupBy(columnNames.Winner,columnNames.Stadiuam).agg(count("*").alias("cnt")).orderBy(col("cnt").desc).show()
    //spark.sql("select winner,stadiuam,count(winner) as cnt from mytable group by winner,stadiuam order by cnt desc ").show()

    /** 3> top 3 team who win continue 3 matches in any particular year */

/**  In progress */

   // val winnerDate=  data.select(col("team1"),col("team2"),col("winner"),col("date")).limit(20)show()
    //winnerDate.createTempView("winnerDate")
    //spark.sql("  select  team1,team2,winner,date, ( winner, DENSE_RANK() OVER (ORDER BY date) - DENSE_RANK() OVER (PARTITION BY winner ORDER BY date) ) AS car_group FROM mytable ").show

    // spark.sql("select team1,team2,winner,date from mytable where team1 = 'Chennai Super Kings' or team2 = 'Chennai Super Kings' order by date  ").show()

    /** 4> predict 3 teams which have to win the toss to win match_no  */

    // spark.sql("select winner,count(toss_win_match_win) as cnt from(select winner,CASE WHEN winner = toss_win THEN 1 ELSE 0 END AS toss_win_match_win from mytable) t1 where toss_win_match_win=1  group by winner order by cnt desc " ).show(truncate = false)2,

    /** 5> top 3 team who played most number of semi-finals  */
   // val iplWinner= spark.sql("select date_sub('2008-06-01',4) from myTable where year='2008'  order by date desc").show()

    val semifinal=spark.sql("select * from (select team1,team2,winner,date,dense_rank() over (partition by year order by date desc) rnk from mytable) as t where t.rnk in (2,3) order by date ")
   semifinal.show()
    semifinal.createTempView("semifinal")

    spark.sql("select team,sum(cnt) as mostSemiFinals from (select count(team1) as cnt, team1 as team from semifinal group by team1 UNION  ALL select count(team2), team2 from semifinal group by team2) group by team order by mostSemiFinals desc ").limit(3).show()

   //spark.sql("select winner,team1,team2,date from mytable where date between '2009-01-01' and '2009-12-use'  group by team1,team2,winner,date order by date desc ").show()



  }

}
