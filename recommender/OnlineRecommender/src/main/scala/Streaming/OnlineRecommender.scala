package Streaming

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import kafka.Kafka
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

case class MongoConfig(uri:String, db:String)

// Define a standard Recommendation case class
case class Recommendation(mid: Int, score: Double)

// Define List of movie similarities based on LMF movie feature vectors
case class MovieRecs(mid: Int, recs: Seq[Recommendation])

// Define List of user recommendations based on predicted ratings
case class UserRecs(uid: Int, recs: Seq[Recommendation])

// Define connection helper
object ConnHelper extends Serializable{
  lazy val jedis = new Jedis("localhost")
  lazy val mongoClient = MongoClient(MongoClientURI("mongodb://localhost:27017/recommender"))
}

object OnlineRecommender {
  // Define table names
  val MAX_USER_RATINGS_NUM = 20
  val MAX_SIM_MOVIES_NUM = 20
  val MONGODB_STREAM_RECS_COLLECTION = "StreamRecs"
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_MOVIE_RECS_COLLECTION = "MovieRecs"

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender",
      "kafka.topic" -> "recommender"
    )

    // Create a sparkConf
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("OnlineRecommender")

    // Create a SparkSession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    // Get streaming context
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(2)) // batch duration

    import spark.implicits._

    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    // Load the movie similarity matrix data and broadcast it
    val simMovieMatrix = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_MOVIE_RECS_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRecs]
      .rdd
      .map { movieRecs => // For the convenience of query similarity, convert to map
        (movieRecs.mid, movieRecs.recs.map(x => (x.mid, x.score)).toMap)
      }.collectAsMap()

    val simMovieMatrixBroadCast = sc.broadcast(simMovieMatrix)

    // Define kafka connection parameters
    val kafkaParam = Map(
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "recommender",
      "auto.offset.reset" -> "latest"
    )

    // Create a DStream through kafka
    val kafkaStream = KafkaUtils.createDirectStream[String, String](ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(config("kafka.topic")), kafkaParam)
    )

    // Convert raw data into scoring stream UID|MID|SCORE|TIMESTAMP
    val ratingStream = kafkaStream.map{
      msg =>
        val attr = msg.value().split("\\|")
        (attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)
    }

    // Continue to do stream processing, the core real-time algorithm part
    ratingStream.foreachRDD{
      rdds => rdds.foreach{
        case (uid, mid, score, timestamp) =>{
          println("rating data is coming! >>>>>>>>>>>>>>>>")

          // 1. Get the latest K ratings of the current user from redis and save them as an Array[(mid, score)]
          val userRecentlyRatings = getUserRecentlyRating(MAX_USER_RATINGS_NUM, uid, ConnHelper.jedis)

          // 2. Take out the most similar N movies to the current movie from the similarity matrix as a candidate list, Array[mid]
          val candidateMovies = getTopSimMovies(MAX_SIM_MOVIES_NUM, mid, uid, simMovieMatrixBroadCast.value)

          // 3. Calculate the recommendation priority for each candidate movie, and get the real-time recommendation list of the current user, Array[(mid, score)]
          val streamRecs = computeMovieScores(candidateMovies, userRecentlyRatings, simMovieMatrixBroadCast.value)

          // 4. Save recommendation data to mongodb
          saveDataToMongoDB(uid, streamRecs)
        }
      }
    }

    // Start receiving and processing data
    ssc.start()

    println(">>>>>>>>>>>>>>>>>> streaming started!")

    ssc.awaitTermination()
  }

  // The redis operation returns the java class, and the conversion class needs to be used in order to use the map operation
  import scala.collection.JavaConversions._
  def getUserRecentlyRating(num: Int, uid: Int, jedis: Jedis): Array[(Int, Double)] = {
    // Read data from redis, user rating data is stored in the queue with uid:UID as key, and value is MID:SCORE
    jedis.lrange("uid:" + uid, 0, num - 1)
      .map{
        item => // Specifically, each score has two values, separated by colons
          val attr = item.split("\\:")
          (attr(0).trim.toInt, attr(1).trim.toDouble)
      }.toArray
  }

  /**
   * Get num movies similar to the current movie as alternative movies
   *
   * @param num       Number of similar movies
   * @param mid       Current Movie ID
   * @param uid       Current rating userID
   * @param simMovies Similarity matrix
   * @return Filtered list of alternative movies
   */
  def getTopSimMovies(num: Int, mid: Int, uid: Int, simMovies: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]])
                     (implicit mongoConfig: MongoConfig): Array[Int] = {
    // 1. Get all similar movies from the similarity matrix
    val allSimMovies = simMovies(mid).toArray

    // 2. Query the movies that the user has watched from mongodb
    val ratingExist = ConnHelper.mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION)
      .find(MongoDBObject("uid" -> uid))
      .toArray
      .map{
        item => item.get("mid").toString.toInt
      }

    // 3. Filter out those the user has seen to get the output list
    allSimMovies.filter(x => ! ratingExist.contains(x._1))
      .sortWith(_._2 > _._2)
      .take(num)
      .map(x => x._1)
  }

  def computeMovieScores(candidateMovies: Array[Int],
                         userRecentlyRatings: Array[(Int, Double)],
                         simMovies: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]]): Array[(Int, Double)] = {
    // Define an ArrayBuffer to save the basic score of each candidate movie
    val scores = scala.collection.mutable.ArrayBuffer[(Int, Double)]()

    // Define a HashMap to save the enhancement and weakening factors (offset items) of each candidate movie
    val increMap = scala.collection.mutable.HashMap[Int, Int]()
    val decreMap = scala.collection.mutable.HashMap[Int, Int]()

    for(candidateMovie <- candidateMovies; userRecentlyRating <- userRecentlyRatings){
      // Get the similarity between the candidate movie and the most recently rated movie
      val simScore = getMoviesSimScore(candidateMovie, userRecentlyRating._1, simMovies)

      if(simScore > 0.7){
        // Calculate the base recommendation score for candidate movies
        scores += ((candidateMovie, simScore * userRecentlyRating._2))
        if(userRecentlyRating._2 > 3){
          increMap(candidateMovie) = increMap.getOrDefault(candidateMovie, 0) + 1
        }else{
          decreMap(candidateMovie) = increMap.getOrDefault(candidateMovie, 0) + 1
        }
      }
    }
    // Do groupby according to the mid of the candidate movie, and calculate the final recommendation score with the formula
    scores.groupBy(_._1).map{
      // groupBy and get Map(mid -> ArrayBuffer[(mid, score)])
      case(mid, scoreList) =>
        (mid, scoreList.map(_._2).sum / scoreList.length + log(increMap.getOrDefault(mid, 1)) - log(decreMap.getOrDefault(mid, 1)))
    }.toArray.sortWith(_._2>_._2)
  }

  // Get the similarity between two movies
  def getMoviesSimScore(mid1: Int, mid2: Int, simMovies: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]]): Double ={
    simMovies.get(mid1) match {
      case Some(sims) => sims.get(mid2) match {
        case Some(score) => score
        case None => 0.0
      }
      case None => 0.0
    }
  }

  // Find the logarithm of a number, the base is 10 by default
  def log(m: Int): Double ={
    val N = 10
    math.log(m) / math.log(N)
  }

  def saveDataToMongoDB(uid: Int, streamRecs: Array[(Int, Double)])(implicit mongoConfig: MongoConfig): Unit = {
    // Define a join to the StreamRecs table
    val streamRecsCollection = ConnHelper.mongoClient(mongoConfig.db)(MONGODB_STREAM_RECS_COLLECTION)

    // If there is already data corresponding to uid in the table, delete it
    streamRecsCollection.findAndRemove(MongoDBObject("uid" -> uid))
    // Store streamRecs data into the table
    streamRecsCollection.insert(MongoDBObject("uid" -> uid,
      "recs" -> streamRecs.map(x => MongoDBObject("mid" -> x._1, "score" -> x._2))))
  }
}
