package bigdata.hive

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import java.io.{BufferedReader, InputStreamReader}
import java.util.UUID

object Hive04RatingEnricher extends App{

  val conf= new Configuration()
  val hadoopConfDir = "/home/leila/Desktop/HADOOP_CONFIG"
  conf.addResource(new Path(s"$hadoopConfDir/core-site.xml"))
  conf.addResource(new Path(s"$hadoopConfDir/hdfs-site.xml"))

  //because it is a public variable.. it is gonna b used outside
  //that's why we have to add the annotation
  val fs:FileSystem = FileSystem.get(conf)

  val staging="/user/bdsf2001/leila"

  val movie =new BufferedReader(new InputStreamReader(
    fs.open(new Path(s"$staging/movie/movie.csv"))
  ))
  val movieLookUp: Map[Int,Movie] =
    Iterator.continually(movie.readLine()).takeWhile(_ != null).toList
      .tail //remove the header
      .map(Movie(_)) // convert to Movie object (JVM Object)
      .map(movie => movie.movieId -> movie).toMap //create a lookup map

  val ratingSource =new BufferedReader(new InputStreamReader(
    fs.open(new Path(s"$staging/rating/rating.csv"))
  ))
  val ratings: List[Rating] =
    Iterator.continually(ratingSource.readLine()).takeWhile(_ != null).toList
      .tail //remove the header
      .map(Rating(_)) // convert to Movie object (JVM Object)

  val enrichedRating = ratings.map(rating => EnrichedRating(rating,movieLookUp.get(rating.mId)))

  val outputPath = new Path(s"/user/hive/warehouse/bdsf2001_leila.db/enriched_rating_csv/${UUID.randomUUID().toString}")
  val outputStream = fs.create(outputPath)
  enrichedRating.foreach{ record =>
    outputStream.writeChars(EnrichedRating.toCsv(record))
    outputStream.writeChars("\n")
  }

  outputStream.close()
  fs.close()

  case class Movie(movieId:Int, title:String, year:Int, director:String)

  object Movie{
    def apply(csv:String): Movie ={
      val fields: Array[String] = csv.split(",",-1)
      Movie(fields(0).toInt,fields(1),fields(2).toInt, fields(3))
    }

    def toCsv(movie: Option[Movie]):String={
      movie match {
        case Some(m) => s"${m.title},${m.year},${m.director}"
        case None => ",,"
      }
    }
  }

  case class Rating(rId:Int,mId:Int,stars:Int,ratingDate:Option[String])

  object Rating{
    def apply(csv:String): Rating ={
      val fields: Array[String]=csv.split(",",-1)
      val ratingDate = if (fields.size == 3) None else Option(fields(3))
      Rating(fields(0).toInt,fields(1).toInt,fields(2).toInt,ratingDate)
    }

    def toCsv(rating: Rating):String ={
      s"${rating.rId},${rating.mId},${rating.stars},${rating.ratingDate.getOrElse("")}"
    }
  }

  case class EnrichedRating(rating:Rating, movie: Option[Movie])
  object EnrichedRating {
    def toCsv(enrichedRating: EnrichedRating): String = {
      s"${Movie.toCsv(enrichedRating.movie:Option[Movie])},${Rating.toCsv(enrichedRating.rating)}"
    }
  }
}
