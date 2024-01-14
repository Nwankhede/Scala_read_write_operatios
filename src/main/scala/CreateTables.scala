import java.sql.DriverManager
import java.sql.Connection
import java.sql.Statement

import java.sql.DriverManager
import java.sql.Connection
import java.sql.Statement

class CreateTables(schema: String, tableName: String) {

  def createSchema(): Unit = {

      val obj = new Pgresconn()
      println(s"Database cbs_sys created successfully.")
      val createSchemaSQL = s"CREATE SCHEMA IF NOT EXISTS $schema"
      obj.prepSt.executeUpdate(createSchemaSQL)

    //    catch {
//      case e: Exception =>
//        println(s"Error occurred while creating Schema or table: ${e.getMessage}")
//    }
  }
  def createTable(): Unit = {
    try {
      val transactions = {
        s"""CREATE TABLE IF NOT EXISTS $schema.$tableName(
            txn_class	string,
            txn_code	string,
            cbs_acc_num	string,
            acc_srt_cd	string,
            crd_num_issue_num	string)
            """.stripMargin
      }
      val connObj = new Pgresconn()
      connObj.prepSt.executeUpdate(transactions)
      println(s"Table $tableName created successfully.")
    }

    catch {
      case e: Exception =>
        println(s"Error occurred while creating table $tableName: ${e.getMessage}")
    }
  }
}





