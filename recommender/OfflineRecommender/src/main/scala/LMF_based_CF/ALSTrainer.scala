package LMF_based_CF

import LMF_based_CF.OfflineRecommender.MONGODB_RATING_COLLECTION
import breeze.numerics.sqrt
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object ALSTrainer {
  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/recommender",
      "mongo.db" -> "recommender"
    )

    // Create a sparkConf
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("ALSTrainer")

    // Create a SparkSession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._

    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    // Load rating data from mongodb
    val ratingRDD = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRating] // Transform to case class data set
      .rdd
      .map(rating => Rating(rating.uid, rating.mid, rating.score)) // Transform to rdd, remove timestamps
      .cache()

    // Randomly split the data set to generate training set and data set
    val splits = ratingRDD.randomSplit(Array(0.8, 0.2))
    val trainingRDD = splits(0)
    val testRDD = splits(1)

    // Model parameter selection, output optimal parameters
    adjustALSParam(trainingRDD, testRDD)

    spark.close()
  }

  def adjustALSParam(trainData: RDD[Rating], testData: RDD[Rating]) = {
    val result = for(rank <- Array(20, 50 ,100); lambda <- Array(0.001, 0.01, 0.1))
      yield{
        val model = ALS.train(trainData, rank, 5, lambda)
        // Calculate the rmse of the model with current parameters and return double
        val rmse = getRMSE(model, testData)
        (rank, lambda, rmse)
      }
    // Print the optimal parameters to console
    println(result.minBy(_._3))
  }

  def getRMSE(model: MatrixFactorizationModel, data: RDD[Rating]) = {
    // Compute predicted scores
    val userProducts = data.map(item => (item.user, item.product))
    val predictRating = model.predict(userProducts)

    // Use uid and mid as foreign keys, inner join the actual observed value and predicted value
    val observed = data.map(item => ((item.user, item.product), item.rating))
    val predict = predictRating.map(item => ((item.user, item.product), item.rating))
    // inner join to get (uid, mid), (actual, predict)
    sqrt(
      observed.join(predict).map {
        case ((uid, mid), (actual, pred)) =>
          val err = actual - pred
          err * err
      }.mean())
  }
}
