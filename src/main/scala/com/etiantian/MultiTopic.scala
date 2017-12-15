package com.etiantian

import java.io.{File, FileInputStream}
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import com.etiantian.HiveUtil.SQLHiveContextSingleton
import kafka.api.{OffsetRequest, PartitionOffsetRequestInfo, TopicMetadataRequest}
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.ZKGroupTopicDirs
import org.I0Itec.zkclient.ZkClient
import org.apache.log4j.LogManager
import org.apache.spark.SparkConf
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.zookeeper.CreateMode
import org.json.JSONObject

/**
 * Hello world!
 *
 */
object MultiTopic {
  val logger = LogManager.getLogger("MultiTopic")

  def main(args: Array[String]): Unit = {
    val configFile = new File(args(0))

    val properties = new Properties()
    properties.load(new FileInputStream(configFile))

    val groupId = properties.getProperty("groupId")
    val brokers = properties.getProperty("brokers")
    val brokerHost = properties.getProperty("brokerHost")
    val zkUrl = properties.getProperty("quorum")

    val sparkConf = new SparkConf()
      .setAppName("ycf:MultiTopic")

    val ssc = new StreamingContext(sparkConf, Seconds(properties.getProperty("streaming.cycle").toInt))

    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers,
      "group.id" -> groupId,
      "auto.offset.reset" -> "smallest"
    )
    val list = List("test_ycf1", "test_ycf2")
    for(x <- list) {
      val topicName = x

      val zkClient = new ZkClient(zkUrl, 10000, 10000, new MyZkSerializer)
      val topicDirs = new ZKGroupTopicDirs(groupId, topicName)
      val zkTopicPath = topicDirs.consumerOffsetDir
      val pcount = zkClient.countChildren(zkTopicPath)
      var fromOffsets: Map[TopicAndPartition, Long] = Map()
      logger.warn("============================== zkTopicPath = " + zkTopicPath + " =============================")
      val request = new TopicMetadataRequest(Seq(topicName), 0)
      val simpleConsumer = new SimpleConsumer(brokerHost, 9092, 10000, 10000, groupId + "OffsetLookup")
      val response = simpleConsumer.send(request)
      logger.warn("==================== " + response.toString + " ======================")
      val topicMetaOption = response.topicsMetadata.headOption
      logger.warn("============================== pcount = " + pcount + " =============================")
      var kafkaStream: InputDStream[(String, String)] = null
      val partitions = topicMetaOption match {
        case Some(tm) =>
          tm.partitionsMetadata.map(pm => (pm.partitionId, pm.leader.get.host)).toMap[Int, String]
        case None =>
          Map[Int, String]()
      }
      if (pcount > 0) {
        for (i <- 0 until pcount) {
          val partitionPath = zkTopicPath + "/" + i
          if (zkClient.exists(partitionPath)) {
            val partitionOffset = zkClient.readData[String](partitionPath)
            logger.warn(s"============================== offset $i = $partitionOffset =============================")
            val tp = TopicAndPartition(topicName, i)

            val offsetRequest = OffsetRequest(Map(tp -> PartitionOffsetRequestInfo(OffsetRequest.EarliestTime, 1)))
            val offsetConsumer = new SimpleConsumer(partitions(i), 9092, 10000, 10000, groupId + "getMinOffset")
            val curOffsets = offsetConsumer.getOffsetsBefore(offsetRequest).partitionErrorAndOffsets(tp).offsets
            var nextOffset = partitionOffset.toLong
            if (curOffsets.nonEmpty && nextOffset < curOffsets.head)
              nextOffset = curOffsets.head

            logger.warn("======================= nextOffset=" + nextOffset + "================")
            fromOffsets += (tp -> nextOffset)
          }
        }
        val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())
        kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)
        kafkaStream.persist(StorageLevel.MEMORY_AND_DISK_SER_2)
      }
      else {
        kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set(topicName))
      }

      logger.warn("============================== fromOffsets = " + fromOffsets.size + " =============================")
      var sqlContext: HiveContext = null
      var offsetRanges = Array.empty[OffsetRange]
      kafkaStream.transform { rdd =>
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }.foreachRDD { x =>
        sqlContext = SQLHiveContextSingleton.getInstance(x.sparkContext)
        //      val sqlContext = new HiveContext(x.sparkContext)
        val rdd = x.map(x => {
          val json = new JSONObject(x._2)
          json.put("name",topicName)
          json.toString()
        }).filter(_ != null)

        if (!rdd.isEmpty()) {
          //        sqlContext.setConf("hive.exec.dynamic.partition", "true")
          //        sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
          sqlContext.read.json(rdd).show()
        }

        for (o <- offsetRanges) {
          val topicDirs = new ZKGroupTopicDirs(groupId, o.topic)
          val zkTopicPath = topicDirs.consumerOffsetDir
          if (!zkClient.exists(zkTopicPath + "/" + o.partition)) {
            if (!zkClient.exists(s"/consumers/$groupId"))
              zkClient.create(s"/consumers/$groupId", null, CreateMode.PERSISTENT)
            if (!zkClient.exists(s"/consumers/$groupId/offsets"))
              zkClient.create(s"/consumers/$groupId/offsets", null, CreateMode.PERSISTENT)
            if (!zkClient.exists(zkTopicPath))
              zkClient.create(zkTopicPath, null, CreateMode.PERSISTENT)
            if (!zkClient.exists(zkTopicPath + "/" + o.partition))
              zkClient.create(zkTopicPath + "/" + o.partition, o.untilOffset.toString, CreateMode.PERSISTENT)
          }
          else
            zkClient.writeData(zkTopicPath + "/" + o.partition, o.untilOffset.toString)
        }
      }

    }
    ssc.start()
    ssc.awaitTermination()
  }
}
