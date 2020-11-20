package com.yuhuachang

import java.util.Properties

import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer

import scala.util.Random

class MessageProducer(brokers: String, topic: String) extends Runnable {
  private val brokerList = brokers
  private val props = new Properties()
  props.put("bootstrap.servers", brokerList)
  props.put("key.serializer", classOf[StringSerializer].getName)
  props.put("value.serializer", classOf[StringSerializer].getName)
  props.put("producer.type", "async")
  private val producer = new KafkaProducer[String, String](props)

  private val MAX_NUM = 26

  override def run(): Unit = {
    val rand = new Random()
    while (true) {
      val num = rand.nextInt(MAX_NUM) + 1
      val msg = new StringBuilder()
      msg.append("A" + num)
      Thread.sleep(10)
      send(msg.toString())
    }
    producer.close()
  }

  def send(message: String): Unit = {
    try {
      producer.send(new ProducerRecord[String, String](this.topic, message), new Callback {
        override def onCompletion(recordMetadata: RecordMetadata, e: Exception): Unit = {
          if (recordMetadata != null) {
            println("succeed send: " + message)
          }
          if (e != null) {
            println("failed")
          }
        }
      })
    } catch {
      case e:Exception => e.printStackTrace()
    }
  }
}
