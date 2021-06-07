import com.typesafe.config.ConfigFactory

import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.io.File

object ProducerFinal {

  def main (args:Array[String]): Unit={

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    if (args.length == 0) {
      System.out.println("Enter the name of the topic");
      return;
    }
    val topicName = args(0)

    val config = ConfigFactory.parseFile(new File("twitter.conf"))

    System.setProperty("twitter4j.oauth.consumerKey", config.getString("API_KEY"))
    System.setProperty("twitter4j.oauth.consumerSecret", config.getString("API_SECRET_KEY"))
    System.setProperty("twitter4j.oauth.accessToken", config.getString("ACCESS_TOKEN"))
    System.setProperty("twitter4j.oauth.accessTokenSecret", config.getString("ACCESS_TOKEN_SECRET"))


    // setting the scene for Twitter 
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("producer-final")
      .getOrCreate()


    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(5))
    ssc.checkpoint("checkpoints")
    val tweetStream = TwitterUtils.createStream(ssc, None)
    val tweetInfo = tweetStream.map(t => (
      t.getUser.getStatusesCount, t.getUser.getFollowersCount, t.getUser.getFriendsCount,
      t.getUser.getFavouritesCount, t.getUser.isProtected, t.getUser.isVerified, t.getLang,
      t.getUser.getLocation))

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


    // start processing tweets

    tweetInfo.foreachRDD(rdd=>{
      rdd.foreachPartition(l=>{
        val producer = new KafkaProducer[String, String](props)
        l.foreach{tweet=>
          val statusCount =tweet._1
          val followerCount=tweet._2
          val friendCount=tweet._3
          val favouriteCount=tweet._4
          val isProtected = if (tweet._5) 1 else 0
          val isVerified = if (tweet._6) 1 else 0
          val lang = tweet._7
          val location = tweet._8

          println(s"$statusCount,$followerCount,$friendCount,$favouriteCount,$isProtected,$isVerified,$lang,$location")
          val record=new ProducerRecord(topicName,"key",s"$statusCount,$followerCount,$friendCount,$favouriteCount,$isProtected,$isVerified,$lang,$location")
          producer.send(record)
        }
      })
    })

    ssc.start()
    ssc.awaitTermination()


  }
} 