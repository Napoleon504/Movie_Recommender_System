package Statistics

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.text.SimpleDateFormat
import java.util.Date

case class Movie(mid: Int, name: String, descri: String, timelong: String, issue: String,
                 shoot: String, language: String, genres: String, actors: String, directors: String)

case class Rating(uid: Int, mid: Int, score: Double, timestamp: Int)

case class MongoConfig(uri:String, db:String)

// Define a standard Recommendation case class
case class Recommendation(mid: Int, score: Double)

// Define the top10 recommendation objects of the movie category
case class GenresRecommendation(genres: String, recs: Seq[Recommendation])

object StatisticsRecommender {

  // Define table names
  val MONGODB_MOVIE_COLLECTION = "Movie"
  val MONGODB_RATING_COLLECTION = "Rating"

  /*
  Names of tables
  RATE_MORE_MOVIES：Hot Movies
  RATE_MORE_RECENTLY_MOVIES：Recent Hot Movies
  AVERAGE_MOVIES：Average Rating Statistics
  GENRES_TOP_MOVIES：Top Statistics by Genre
   */
  val RATE_MORE_MOVIES = "RateMoreMovies"
  val RATE_MORE_RECENTLY_MOVIES = "RateMoreRecentlyMovies"
  val AVERAGE_MOVIES = "AverageMovies"
  val GENRES_TOP_MOVIES = "GenresTopMovies"

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender"
    )
    // Create a sparkConf
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("StatisticsRecommender")

    // Create a SparkSession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._

    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    // Load data from mongodb
    val ratingDF = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Rating] // Transform to case class data set
      .toDF()

    val movieDF = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_MOVIE_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Movie] // Transform to case class data set
      .toDF()

    // Create a temporary table named rating
    ratingDF.createOrReplaceTempView("ratings")

    // Different statistical recommendation results
    // 1. Statistics of popular movies in history: the most rated movies in history (mid, count)
    val rateMoreMoviesDF = spark.sql("select mid, count(mid) as count from ratings group by mid")

    // Write the result to the corresponding mongodb table
    storeDFInMongoDB(rateMoreMoviesDF, RATE_MORE_MOVIES)

    // 2. Recent popular movie statistics: select the latest rating data according to the format of "yyyyMM", and count the number of ratings
    // Create a date formatter
    val simpleDataFormat = new SimpleDateFormat("yyyyMM")

    // Register udf and convert timestamp to year-month format
    spark.udf.register("changeData", (x: Int) => simpleDataFormat.format(new Date(x * 1000L)).toInt)

    // Preprocess the raw data and remove the uid
    val ratingOfYearMonth = spark.sql("select mid, score, changeDate(timestamp) as yearmonth from ratings")
    ratingOfYearMonth.createOrReplaceTempView("ratingOfMonth")

    // Find the rating of a movie in each month from ratingOfMonth (mid, count, yearmonth)
    val rateMoreRecentlyMoviesDF = spark.sql("select mid, count(mid) as count, yearmonth from ratingOfMonth group by yearmonth, mid order by yearmonth desc, count desc")

    // Save to mongodb
    storeDFInMongoDB(rateMoreRecentlyMoviesDF, RATE_MORE_RECENTLY_MOVIES)

    // 3. High-quality movie statistics, the average rating of statistical movies (mid, avg)
    val averageMoviesDF = spark.sql("select mid, avg(score) as avg from ratings group by mid")
    storeDFInMongoDB(averageMoviesDF, AVERAGE_MOVIES)

    // 4. Top statistics of movies in each genre
    // Define all genres
    val genres = List("Action", "Adventure", "Animation", "Comedy", "Crime", "Documentary", "Drama", "Family", "Fantasy", "Foreign", "History", "Horror", "Music", "Mystery"
      , "Romance", "Science", "Tv", "Thriller", "War", "Western")

    // Add the average rating to the movie table, add a column, inner join
    val movieWithScore = movieDF.join(averageMoviesDF, "mid")

    // For Cartesian product, convert genres to rdd
    val genresRDD = spark.sparkContext.makeRDD(genres)

    // To calculate the genre top10, first do a Cartesian product of the genre and the movie
    val genresTopMoviesDF = genresRDD.cartesian(movieWithScore.rdd)
      .filter{
        // Conditional filtering to find out movies whose genres value contains the current genre
        case(genre, movieRow) => movieRow.getAs[String]("genres").toLowerCase.contains(genre.toLowerCase)
      }.map{
      case(genre, movieRow) => (genre, (movieRow.getAs[Int]("mid"), movieRow.getAs[Double]("avg")))
      }
      .groupByKey()
      .map{
        case(genre, items) => GenresRecommendation(genre, items.toList.sortWith(_._2 > _._2).take(10).map(item => Recommendation(item._1, item._2)))
      }
      .toDF()

    // Save to mongodb
    storeDFInMongoDB(genresTopMoviesDF, GENRES_TOP_MOVIES)

    spark.stop()
  }

  def storeDFInMongoDB(df: DataFrame, collection_name: String)(implicit mongoConfig: MongoConfig) = {
    df.write
      .option("uri", mongoConfig.uri)
      .option("collection", collection_name)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
  }
}
