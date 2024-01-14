import DfReadWrite.logger

object Config {

  val transtbl = "transactions"
  val accno = "cbs_acc_num"
  val srtcode = "acc_srt_cd"
  val run_date =  "txn_date"
  val databaseName = "TestDb"
  val tableName = "transtbl"
  val user = "postgres"
  val password = "Pass@1234"
  val host = "localhost"
  val port = "3306"
  val jdbcurl = "jdbc:postgresql://localhost:3306/TestDb?socketTimeout=600000"
  val schema = "al_2048"

}