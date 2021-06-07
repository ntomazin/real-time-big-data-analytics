import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import java.io.File
import com.typesafe.config.ConfigFactory

object TwitterTopHashtags {


  def main(args: Array[String]): Unit = {


    Logger.getLogger("org").setLevel(Level.OFF)
    if (args.length == 0) {
      System.out.println("Enter the name of the topic");
      return;
    }
    val topicName = args(0)

    // set the scene for ingesting form Twitter

    // reading from twwiter.conf which holds sensitive info
    val config = ConfigFactory.parseFile(new File("twitter.conf"))

    //    val filters=List("#Android")
    System.setProperty("twitter4j.oauth.consumerKey", config.getString("API_KEY"))
    System.setProperty("twitter4j.oauth.consumerSecret", config.getString("API_SECRET_KEY"))
    System.setProperty("twitter4j.oauth.accessToken", config.getString("ACCESS_TOKEN"))
    System.setProperty("twitter4j.oauth.accessTokenSecret", config.getString("ACCESS_TOKEN_SECRET"))


    // setting the scene for Twitter
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("sample-structured-streaming")
      .getOrCreate()


    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(5))
    ssc.checkpoint("checkpoints")
    val tweetStream = TwitterUtils.createStream(ssc, None)
    val tweetSreamFR = tweetStream.filter(t => t.getLang == "fr").map(t => (t.getText, t.getUser.getScreenName))
    val tweetSreamEN = tweetStream.filter(t => t.getLang == "en").map(t => (t.getText, t.getUser.getScreenName))


    val hashTags = tweetStream.flatMap(status=>status.getText.split(" ").filter(_.startsWith("#")))//.filter(_.toLowerCase().contains("spce")))
    //hashTags.checkpoint(Seconds(10))
    val top60 = hashTags.map(s=>(s,1)).reduceByKeyAndWindow(_+_, _-_, Seconds(60)).map{case(topic, count) => (count, topic)}.
      transform(_.sortByKey(ascending = false))

    top60.foreachRDD((rdd=>{
      val top5 = rdd.take(5)
      println("Popular topics in last 60 secs (%s total)".format(rdd.count()))
      rdd.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}

    }))


    /*
    // set the scene for my produccer
    val props = new Properties()


    props.put("bootstrap.servers", "localhost:9092")
    props.put("acks", "all")
    props.put("retries", "0")
    props.put("batch.size", "16384");
    // buffer.memory controls the total amount of memory available to the producer for buffering
    props.put("buffer.memory", "33554432");
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

*/
    // start processing tweets
/*
    tweetSreamFR.foreachRDD(rdd => {

      rdd.foreachPartition(l => {

        afkaProducer[String, String](props)
        l.foreach {
          tw
          var producer = new K eet
          =>

          val textTweet = tweet._1
          val UserTweet = tweet._2
          println(s"($textTweet,$UserTweet")
          val record = new ProducerRecord(topicName, "key", s"($textTweet,$UserTweet")
          producer.send(record)


        }
      })
    }


    )
*/
    ssc.start()
    ssc.awaitTermination()


  }
}