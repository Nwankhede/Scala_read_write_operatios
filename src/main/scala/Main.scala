
import DfReadWrite.{csvPath, logger}
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame



/**
 * Hello world!
 *
 */
object main extends App{
  println( "Executing main method" )
//  DfReadWrite
val objcreate = new CreateTables(Config.schema,Config.tableName)
  objcreate.createSchema()
  objcreate.createTable()
  logger.info(s"reading from CSV:${csvPath}")




}
