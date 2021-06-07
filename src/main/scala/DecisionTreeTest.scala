import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DecisionTreeTest {
  def main(args: Array[String]):Unit= {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)


    val conf = new SparkConf().setAppName("DataStructure").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(10))
    val sc = ssc.sparkContext
    sc.setLogLevel("Error")
    val topicSet = Set("events")
    val kafkaParm = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "group",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer])
    val events = KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicSet, kafkaParm))

    val data = sc.textFile("dataset.txt").filter(row => !row.startsWith(",")).map(l=>l.split(","))
    val doubleData=data.map(l=>l.map(elm=>elm.toDouble))
    val parsedData=doubleData.map(l=>{
      val features = Vectors.dense(l(3),l(4),l(5),l(6),l(7),l(8))
      LabeledPoint(l(10), features)

    }).cache()
    val Array(traindata,testdate)=parsedData.randomSplit(Array(0.7,0.3))
    val dtmodel = DecisionTree.trainClassifier(
      traindata,
      numClasses = 2,
      Map[Int,Int](),
      impurity =  "gini",
      maxDepth = 6,
      maxBins = 32)
    val predictions=dtmodel.predict(testdate.map(l=>l.features))
    predictions.foreach(println)
    val samples=events.map(_.value()).map(l=>{
      if(l.split(",").length==6){
        val sample=Vectors.dense(l.split(",").map(_.toDouble))
        val prediction=dtmodel.predict(sample)
        (sample,prediction)
      }
    })
    samples.print()
    ssc.start()
    ssc.awaitTermination()

  }}
