package bigdata.hive

import org.apache.hive.jdbc.HiveDriver

import java.sql.DriverManager

object Hive03Execute extends App {

  val driverName: String = classOf[HiveDriver].getName
  Class.forName(driverName)

  val connectionString: String = "jdbc:hive2://quickstart.cloudera:10000/bdsf2001_leila;user=cloudera"

  val connection = DriverManager.getConnection(connectionString)
  val statementStmt = connection.createStatement()

  /*
  If it is true , it means query has a result
  select queries have  ResultSet
  1.sttmt.executeQuery -- when it's select
  2.sttmt.execute
    sttmt.getResultSet
  3.if it is false, it means query does not have a ResultSet
  Insert,Update,Create,etc.
  1.sttmt.executeUpdate
  2.sttmt.execute

   */

  //the statement below hasResultSet returns a boolean
  val hasResultSet = statementStmt.execute(
    """
      |INSERT OVERWRITE TABLE enriched_rating
      |SELECT m.id,m.title,rid,stars,ratingdate
      |FROM movie_updated m JOIN ext_rating r ON m.id = r.mid
      |""".stripMargin)

  if (hasResultSet) println("I received a result set")
  else println("I have not received a result set, it was an update query")

  val hasResultSet2 = statementStmt.execute("select * from ext_movie_wo_header_w_null")
  if (hasResultSet2) {
    val rs = statementStmt.getResultSet
    while (rs.next()) {
      println(rs.getInt(1))
    }
    rs.close()
  }

  statementStmt.close()
  connection.close()
}
