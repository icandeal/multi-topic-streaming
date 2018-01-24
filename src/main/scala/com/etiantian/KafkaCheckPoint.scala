package com.etiantian

import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.kafka.{KafkaCluster, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object KafkaCheckPoint {
  val logger = Logger.getLogger(KafkaCheckPoint.getClass)
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("KafkaCheckPoint")
      .setMaster("local[4]")

    val checkPointDir = "./checkpoint/KafkaCheckPoint"
    val topicList = List("ycf1", "ycf2")

    Logger.getRootLogger.setLevel(Level.WARN)
    val ssc = StreamingContext.getOrCreate(checkPointDir,() => createStreamingContext(sparkConf,checkPointDir, 5, topicList))


    ssc.start()
    ssc.awaitTermination()
  }

  def createStreamingContext(sparkConf: SparkConf, checkPointDir: String, cycle: Int, topicList:List[String]): StreamingContext = {
    val ssc = new StreamingContext(sparkConf, Seconds(cycle))
    logger.warn("======================================")
    ssc.checkpoint(checkPointDir)

    val kafkaParam = Map(
      "metadata.broker.list" -> "t45.test.etiantian.com:9092",
      "group.id" -> "KafkaCheckPoint",
      "auto.offset.reset" -> "smallest" // 从最早的开始读
    )

    for(topic <- topicList) {
      val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParam, Set(topic))

      kafkaStream.foreachRDD(rdd => {

        println(s"==================================== $topic ===============================")
        rdd.collect().foreach(println)
        println("===========================================================================")
        println()
        println()
      })
    }
    ssc
  }
}
