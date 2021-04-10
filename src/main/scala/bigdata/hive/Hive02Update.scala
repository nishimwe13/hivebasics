package bigdata.hive

import org.apache.hive.jdbc.HiveDriver

import java.sql.DriverManager

object Hive02Update extends App {

  val driverName: String = classOf[HiveDriver].getName
  Class.forName(driverName)

  val connectionString: String = "jdbc:hive2://quickstart.cloudera:10000/bdsf2001_leila;user=cloudera"

  val connection = DriverManager.getConnection(connectionString)
  val statementStmt = connection.createStatement()

  statementStmt.executeUpdate(
    """
      | CREATE TABLE IF NOT EXISTS enriched_rating(
      | mid int,
      | title string,
      | rid int,
      | stars int,
      | ratingdate string
      | )
      | stored as parquet
      |""".stripMargin)

  statementStmt.executeUpdate(
    """
      |INSERT INTO TABLE enriched_rating
      |SELECT m.id,m.title,rid,stars,ratingdate
      |FROM movie_updated m JOIN ext_rating r ON m.id = r.mid
      |""".stripMargin)

  statementStmt.close()
  connection.close()
}
