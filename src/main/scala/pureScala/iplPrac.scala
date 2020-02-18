package pureScala


import java.net.InetSocketAddress
import java.util.UUID

import com.datastax.driver.core._
import org.apache.spark.sql.Row
import com.datastax.driver.core.{Row, UDTValue}
import net.liftweb.json.DefaultFormats
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.datanucleus.store.rdbms.mapping.MappingManager
import org.datanucleus.store.rdbms.table.Table

import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters._
import scala.collection.immutable.HashMap
import scala.collection.mutable

object iplPrac {

  def main(args: Array[String]): Unit = {

    case class emp(id : Int,dept : Int ,name : String, salary : Int)


    val cluster = Cluster.builder().addContactPointsWithPorts(new InetSocketAddress("127.0.0.1", 9042)).build()
    val session = cluster.connect()

    val set  = session.execute(s"select * from testkeyspace.emp").all()
    println(set)


    set.asScala.map{
      data => val res = data.asInstanceOf[Seq[emp]]

        res.map(row => print(Console.MAGENTA+row+" "))
        println("")
    }




   val setPrep  = session.execute(s"select * from testkeyspace.emp").all()
    val demo= setPrep
    println(setPrep)




cluster.close()

}
}

