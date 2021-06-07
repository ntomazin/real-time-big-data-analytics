import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object ConsumerProducer {


  def main(args:Array[String]):Unit= {
    Logger.getLogger("org").setLevel(Level.OFF)


    val conf = new SparkConf().setAppName("SparkKafkaStreaming").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(15))
    val sc = ssc.sparkContext



    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092,anotherhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )



    val stream = KafkaUtils.createDirectStream[String, String](ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set("events"), kafkaParams)
    )


    //val tweets=stream.map (l=>(l.headers(),l.offset(),l.partition()))


    val tweets =stream.map(l=>l.value().split(",")).map(w=>(w(0),w(1), w(2)))




    // prepare my producer
    val props = new Properties()


    props.put("bootstrap.servers", "localhost:9092")
    props.put("acks", "all")
    props.put("retries", "0")
    props.put("batch.size", "16384");
    // buffer.memory controls the total amount of memory available to the producer for buffering
    props.put("buffer.memory", "33554432");
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")


    // start processing stream


    tweets.foreachRDD(rdd=>{

      rdd.foreachPartition{elem=>{
        val producer=new KafkaProducer[String,String](props)
        elem.foreach{t=>{
          // println(s" before test $t._3.*****(${t._1},${t._3})")
          if  (t._2.equals("fr"))
          {val record=new ProducerRecord[String,String]("FrenchTweets","key",s"(${t._1},${t._3})")
            //  println(s" $t._3.*****(${t._1},${t._2})")
            producer.send(record)
          }

          else
            if  (t._2.equals("en"))
            {val record=new ProducerRecord[String,String]("EnglishTweets","key",s"(${t._1},${t._3})")
              //    println(s" $t._3.******(${t._1},${t._2})")
              producer.send(record)}
        }



        }
        producer.close()
      }


      }

    }




    )


    ssc.start()
    ssc.awaitTermination()

  }
} 