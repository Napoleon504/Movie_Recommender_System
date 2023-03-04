package LMF_based_CF

import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.sql.SparkSession
import org.jblas.DoubleMatrix

// LFM based on rating data, only rating data is required
case class MovieRating(uid: Int, mid: Int, score: Double, timestamp: Int)

case class MongoConfig(uri:String, db:String)

// Define a Recommendation case class
case class Recommendation(mid: Int, score: Double)

// Define List of user recommendations based on predicted ratings
case class UserRecs(uid: Int, recs: Seq[Recommendation])

// Define List of movie similarities based on LMF movie feature vectors
case class MovieRecs(mid: Int, recs: Seq[Recommendation])

object OfflineRecommender {
  // Define table names
  val MONGODB_RATING_COLLECTION = "Rating"

  val USER_RECS = "UserRecs"
  val MOVIE_RECS = "MovieRecs"
  val USER_MAX_RECOMMENDATION = 20

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender"
    )
    // Create a sparkConf
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("OfflineRecommender")

    // create a SparkSession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._

    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    // Load data from mongodb
    val ratingRDD = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRating] // Transform to case class data set
      .rdd
      .map(rating => (rating.uid, rating.mid, rating.score)) // Transform to rdd, remove timestamps
      .cache()

    // Extract uid and mid from rating, and deduplicate
    val userRDD = ratingRDD.map(_._1).distinct()
    val movieRDD = ratingRDD.map(_._2).distinct()

    // Train LMF model
    val trainData = ratingRDD.map(x => Rating(x._1, x._2, x._3))

    val (rank, iterations, lambda) = (50, 5, 0.01)
    val model = ALS.train(trainData, rank, iterations, lambda)

    // Based on the hidden features of users and movies, calculate the predicted scores and get the user recommendation list
    // Calculate the Cartesian product of user and movie to get an empty rating matrix
    val userMovies = userRDD.cartesian(movieRDD)

    // Call the predict method of the model to predict the score
    val preRatings = model.predict(userMovies)

    val userRecs = preRatings
      .filter(_.rating > 0) // Filter out items with a score greater than 0
      .map(rating => (rating.user, (rating.product, rating.rating)))
      .groupByKey()
      .map{
        case (uid, recs) => UserRecs(uid, recs.toList.sortWith(_._2 > _._2).take(USER_MAX_RECOMMENDATION).map(x => Recommendation(x._1, x._2)))
      }
      .toDF()

    userRecs.write
      .option("uri", mongoConfig.uri)
      .option("collection", USER_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    // Based on the movie features, calculate the similarity matrix and get the movie similarity list
    val movieFeatures = model.productFeatures.map{
      case (mid, features) => (mid, new DoubleMatrix(features))
    }

    // Calculate the similarity of all movies pairwise, first do the Cartesian product
    val movieRecs = movieFeatures.cartesian(movieFeatures)
      .filter{
        // Filter out those that match itself
        case (a, b) => a._1 != b._1
      }
      .map{
        case (a, b) => {
          val simScore = this.consinSim(a._2, b._2)
          (a._1, (b._1, simScore))
        }
      }
      .filter(_._2._2 > 0.6) // Filter out those with a similarity greater than 0.6
      .groupByKey()
      .map{
        case (mid, items) => MovieRecs(mid, items.toList.sortWith(_._2 > _._2).map(x => Recommendation(x._1, x._2)))
      }
      .toDF()

    movieRecs.write
      .option("uri", mongoConfig.uri)
      .option("collection", MOVIE_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    spark.stop()
  }

  // Calculate vector cosine similarity
  def consinSim(movie1: DoubleMatrix, movie2: DoubleMatrix): Double = {
    movie1.dot(movie2) / (movie1.norm2() * movie2.norm2())
  }
}
