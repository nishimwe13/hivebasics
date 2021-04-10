package bigdata.hive

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hive.jdbc.HiveDriver

import java.sql.DriverManager

object HiveProject4 extends App {

  val conf = new Configuration()
  val hadoopConfDir = "/home/leila/Desktop/HADOOP_CONFIG"
  conf.addResource(new Path(s"$hadoopConfDir/core-site.xml"))
  conf.addResource(new Path(s"$hadoopConfDir/hdfs-site.xml"))

  val fs: FileSystem = FileSystem.get(conf)
  val folderPath = new Path("/user/bdsf2001/leila/project4")

  if (fs.exists(folderPath)) fs.delete(folderPath, true)
  fs.mkdirs(folderPath)

  val tripsPath = new Path(s"$folderPath/trips")
  val calendarPath = new Path(s"$folderPath/calendar_dates")
  val routesPath = new Path(s"$folderPath/routes")

  fs.mkdirs(tripsPath)
  fs.mkdirs(calendarPath)
  fs.mkdirs(routesPath)

  fs.copyFromLocalFile(new Path("/home/leila/Desktop/gtfs_stm/trips.txt"),tripsPath)
  fs.copyFromLocalFile(new Path("/home/leila/Desktop/gtfs_stm/calendar_dates.txt"),calendarPath)
  fs.copyFromLocalFile(new Path("/home/leila/Desktop/gtfs_stm/routes.txt"),routesPath)

  val driverName: String = classOf[HiveDriver].getName
  Class.forName(driverName)

  val connectionString: String = "jdbc:hive2://quickstart.cloudera:10000/bdsf2001_leila;user=cloudera"

  val connection = DriverManager.getConnection(connectionString)
  val statemt = connection.createStatement()

  statemt.executeUpdate("DROP TABLE IF EXISTS ext_trips")
  statemt.executeUpdate(
    """
      |CREATE EXTERNAL TABLE IF NOT EXISTS ext_trips(
      |route_id int,
      |service_id string,
      |trip_id string,
      |trip_headsign string,
      |direction_id string,
      |shape_id string,
      |wheelchair_accessible int,
      |note_fr string,
      |note_en string
      |)
      |row format delimited
      |fields terminated by ','
      |location '/user/bdsf2001/leila/project4/trips'
      |tblproperties(
      | 'skip.header.line.count' = '1',
      | 'serialization.null.format' = ''
      |)
      |""".stripMargin)

  //route_id,service_id,trip_id,trip_headsign,direction_id,shape_id,wheelchair_accessible,note_fr,note_en

  statemt.executeUpdate("DROP TABLE IF EXISTS ext_calendar_dates")
  statemt.executeUpdate(
    """
      |CREATE EXTERNAL TABLE ext_calendar_dates(
      |service_id string,
      |date string,
      |exception_type int
      |)row format delimited fields terminated by ','
      |location '/user/bdsf2001/leila/project4/calendar_dates'
      |tblproperties(
      | 'skip.header.line.count' = '1',
      | 'serialization.null.format' = ''
      |)
      |""".stripMargin)
  //service_id,date,exception_type

  statemt.executeUpdate("DROP TABLE IF EXISTS ext_routes")
  statemt.executeUpdate(
    """
      |CREATE EXTERNAL TABLE IF NOT EXISTS ext_routes(
      |route_id INT,
      |agency_id string,
      |route_short_name string,
      |route_long_name string,
      |route_type string,
      |route_url string,
      |route_color string,
      |route_text_color string
      |)
      |row format delimited fields terminated by ','
      |location '/user/bdsf2001/leila/project4/routes'
      |tblproperties(
      | 'skip.header.line.count' = '1',
      | 'serialization.null.format' = ''
      |)
      |""".stripMargin)

  //route_id,agency_id,route_short_name,route_long_name,route_type,route_url,route_color,route_text_color

  statemt.executeUpdate("set hive.exec.dynamic.partition.mode = nonstrict;")

  statemt.executeUpdate("DROP TABLE IF EXISTS enriched_trip")
  statemt.executeUpdate(
    """
      |CREATE TABLE IF NOT EXISTS enriched_trip(
      |trip_id string,
      |service_id string,
      |route_id int,
      |trip_headsign string,
      |date string,
      |exception_type int,
      |route_long_name string,
      |route_color string
      |)
      |PARTITIONED BY (wheelchair_accessible boolean)
      |stored as parquet
      |""".stripMargin)

  statemt.executeUpdate(
    """
      |INSERT INTO TABLE enriched_trip PARTITION(wheelchair_accessible)
      |SELECT t.trip_id string,c.service_id string,t.route_id int,t.trip_headsign string,c.date string,
      |c.exception_type int,r.route_long_name string,r.route_color string,
      |CASE WHEN wheelchair_accessible = 1 THEN true ELSE false END AS new_wheelchair_accessible
      |FROM ext_trips t JOIN ext_routes r ON t.route_id = r.route_id
      |JOIN ext_calendar_dates c ON c.service_id = t.service_id
      |""".stripMargin)

  statemt.close()
  connection.close()
}
