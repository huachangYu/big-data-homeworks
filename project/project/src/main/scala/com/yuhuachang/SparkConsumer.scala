package com.yuhuachang


import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Durations, Seconds, StreamingContext}
import org.apache.log4j.{Level, Logger}


object SparkConsumer {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    System.setProperty("HADOOP_USER_NAME", "hadoop")

    val conf = new SparkConf().setAppName("Consumer").setMaster("local[*]")

    val spark = SparkSession.builder().config(conf).getOrCreate()

    val sc = new StreamingContext(spark.sparkContext, Seconds(5))

    val topics = Array("hello0")

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "192.168.1.133:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "spark-streaming-group",
      "value.deserializer" -> classOf[StringDeserializer],
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val stream = KafkaUtils.createDirectStream[String, String](
      sc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    val valStream = stream.map(item => item.value())
    val frequencyStream = valStream.map(item => (item, 1)).reduceByKey(_+_)
    val sortResult = frequencyStream.transform(rdd => {
      val dataRdd = rdd.sortBy(t => t._2, false)
      dataRdd
    })
    sortResult.foreachRDD(rdd => {
      println("--------Top 5------------")
      println("time = " + System.currentTimeMillis())
      rdd.take(5).foreach(r => {
        println("name = " + r._1 + ",  frequency=" + r._2)
      })

    })

    sc.start()
    sc.awaitTermination()
  }
}
