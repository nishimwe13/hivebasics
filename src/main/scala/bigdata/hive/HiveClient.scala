package bigdata.hive

import org.apache.hive.jdbc.HiveDriver

import java.sql.DriverManager

object Hive01SimpleSelect extends App {

  //val driverName: String = "org.apache.hive.jdbc.HiveDriver"
  val driverName: String = classOf[HiveDriver].getName
  Class.forName(driverName)

  //here with hive 2, it gonna look to the '[HiveDriver]' above
  //if we don't add user=cloudera, it gonna use default and the query failed.
  val connectionString: String = "jdbc:hive2://quickstart.cloudera:10000/bdsf2001_leila;user=cloudera"

  //like other apps, we have to make the connection with the db
  val connection = DriverManager.getConnection(connectionString)
  //this is also a resource to be closed after using it
  val statementStmt = connection.createStatement()

  //business logic
  //this rs is an iterator
  val rs =statementStmt.executeQuery("select * from ext_movie")

  //what u want to do with each row
  while(rs.next()){
    println(rs.getInt(1))
  }

  rs.close()
  statementStmt.close()
  connection.close()
}
