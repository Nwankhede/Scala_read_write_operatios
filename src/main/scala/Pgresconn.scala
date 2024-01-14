import java.sql.DriverManager

class Pgresconn {

  Class.forName("org.postgresql.Driver")
  val connection = DriverManager.getConnection(Config.jdbcurl,Config.user,Config.password)
  val prepSt = connection.createStatement()
  def execute (sql : String)={
    // Execute the SQL query
    val resultSet = prepSt.executeQuery(sql)

    // Get metadata to retrieve column names
    val metaData = resultSet.getMetaData
    val columnCount = metaData.getColumnCount


    for (i <- 1 to columnCount) {
      print(f"${metaData.getColumnName(i)}%-20s")
    }
    println() // Move to the next line for data

    // Process and print the result set
    while (resultSet.next()) {
      for (i <- 1 to columnCount) {
        // Assuming all columns are of type String for simplicity
        val resultValue = resultSet.getString(i)
        print(f"$resultValue%-20s")
      }
      println() // Move to the next line for the next row
    }
  }



}
