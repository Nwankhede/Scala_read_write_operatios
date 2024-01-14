
import java.nio.file.{Files, Paths}
import java.util.Properties

import org.apache.derby.impl.sql.compile.TableName
import org.apache.log4j.Logger
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, rank}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.util.{Failure, Success, Try}

class Util(logger: Logger) {
  //  val x: Option[String] = None
  //    logger.info(s"x is not divisible by 0 $x.get"
  val spark = SparkSession.builder.master("local").
    appName("ReadCSV").
    enableHiveSupport().
    config("hive.exec.dynamic.partition", true).
    config("hive.exec.dynamic.partition.mode", "nonstrict").
    getOrCreate()
  val connectionProps = new Properties()
  connectionProps.setProperty("user","postgres")
  connectionProps.setProperty("password","Pass@1234")
  connectionProps.setProperty("driver","org.postgresql.Driver")


  def PathFinder(path: String): Option[String] = {
    Try {
      Files.exists(Paths.get(path))
    }
    match {
      case Success(s) => None
      case Failure(e) => Some("CSV path dose not exist")
    }
  }

  def readFromCsv(csvPath: String): Option[DataFrame] = {
    Try {
      val df = spark.read.option("header", "true").csv(csvPath)
      Some(df)
    } match {
      case Success(df) => Some(df).get
      case Failure(e) => throw new Exception(s"error reading CSV: ${e}")
    }

  }
  def latestPartitionDf(run_date : String, df: DataFrame) : Option[DataFrame]= {
    Try{
      val win = Window.partitionBy(col(s"${Config.accno}"),col(s"${Config.srtcode}"))
      val dfLatest = df.withColumn("rnk", rank() over(win).orderBy(col(run_date).desc))
      val latestRecordDf = dfLatest.where("rnk=1").drop("rnk")

      latestRecordDf
    } match {
      case Success(s) => Some(s)
      case Failure(e)  => None
    }



  }
  def Processor(inputDf: DataFrame): Option[DataFrame] = {
    val latestDf = latestPartitionDf(s"${Config.run_date}",inputDf).get
        Try {
      latestDf.filter("txn_code!=981 AND txn_date IS NOT null and txn_code!=0")
    } match {
      case Success(df) => Some(df)
      case Failure(e) => None
    }
  }

  def Lowercase(mode: String): Option[String] = {
    Try {
      val lowerCase = mode.toLowerCase
      lowerCase
    }
    match {
      case Success(s) => Some(s)
      case Failure(e) => None
    }

  }

  def writeDF(df: DataFrame, tableName: String, mode: String): Option[String] = {
    
    val filterDf = Processor(df).get
    val lmode : String = Lowercase(mode).get
    Try {
      if (lmode == "append") {
        filterDf.write.mode(lmode).insertInto(s"${tableName}")
      }
      else if (lmode == "overwrite") {
        filterDf.write.mode(lmode).insertInto(s"${tableName}")
      }
      else logger.info(s"mode is not specified correctly currently supporting only append and overwrite")

      Some(s"DataFrame successfully written to Hive table")
    }
    match {
      case Success(s) => None
      case Failure(e) => throw new Exception(s"Error writing DF into table ${e}")
    }
  }

 def writeDfJdbc(df: DataFrame, tableName: String, mode: String):Option[String] = {
   val filterDf = Processor(df).get
   val lmode : String = Lowercase(mode).get

     if (lmode == "append") {
       filterDf.write
         .mode("append")
         .jdbc(Config.jdbcurl, tableName, connectionProps)
     }
     else if (lmode == "overwrite") {
       filterDf.write
         .mode("overwrite")
         .jdbc(Config.jdbcurl, tableName, connectionProps)
     }
     else logger.info(s"mode is not specified correctly currently supporting only append and overwrite")

     Some(s"DataFrame successfully written to poatgrestbl")


 }


}





