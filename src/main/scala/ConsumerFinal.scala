import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.io.File
import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

object ConsumerFinal {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    if (args.length == 0) {
      System.out.println("Enter the name of the topic");
      return;
    }
    val topicName = args(0)

    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("consumer-final")
      .getOrCreate()


    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(5))
    ssc.checkpoint("checkpoints")

    sc.setLogLevel("Error")
    val topicSet = Set(topicName)
    val kafkaParm = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "group",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer])
    val events = KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicSet, kafkaParm))

    ///// BATCH PART
    val data = sc.textFile("dataset.txt").filter(row => !row.startsWith(",")).map(l=>l.split(","))
    val doubleData = data.map(l=>l.map(elem=>elem.toDouble)) // each line is an array of doubles

    val parsedData = doubleData.map(l=>{
      val features = Vectors.dense(l(3),l(4),l(5),l(6),l(7),l(8)) // only taking important features (statuses_count,followers_count,friends_count,favourites_count,protected, verified)
      LabeledPoint(l(10), features)
    }).cache()


    //val Array(traindata,testdate)=parsedData.randomSplit(Array(0.7,0.3))
    val traindata = parsedData // i dont need the test data, i have the data from the producer as test
    val dtmodel = DecisionTree.trainClassifier(
      traindata,
      numClasses = 2,
      Map[Int,Int](),
      impurity = "gini",
      maxDepth = 6,
      maxBins = 32)

    ///// STREAMING PART
    val samples=events.map(_.value()).map(l=>{
      if(l.split(",").length==6){
        val sample=Vectors.dense(l.split(",").map(_.toDouble))
        val prediction=dtmodel.predict(sample)
        (sample, if(prediction==0) "Not bolt" else "Bolt")
      }
    })
    samples.print()

    ssc.start()
    ssc.awaitTermination()


  }
}


