import Config.password
import DfReadWrite.{objcreate}
import org.apache.log4j.Logger
import org.apache.spark.sql.{SaveMode, SparkSession}

object DfReadWrite {

  val csvPath: String = "src/main/resources/transactions_table.csv"
  val logger = Logger.getLogger("main")
  val obj = new Util(logger)
  val objcreate = new CreateTables(Config.schema,Config.tableName)
    objcreate.createSchema()
    objcreate.createTable()
      logger.info(s"reading from CSV:${csvPath}")
  if (!obj.PathFinder(csvPath).isDefined){
    obj.PathFinder(csvPath)
    val df = obj.readFromCsv(csvPath)
    logger.info(s"dataframe form CSV sucessfull")
    df.get.show(5)
    obj.writeDfJdbc(df.get, s"${Config.schema}.${Config.tableName}", "append")
    logger.info("Writing operation completed.......")
    val output = new Pgresconn()
    output.execute("Select * From al_2048.transtbl")

//    obj.spark.sql("select * from transactions").show(5)
  }
  else logger.info("CSV path not found Hence aborting process")

}
