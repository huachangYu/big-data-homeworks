package com.yuhuachang

object ProducerRunner {
  def main(args: Array[String]): Unit = {
    val producer = new MessageProducer("192.168.1.133:9092", "hello0")
    new Thread(producer).start()
  }
}
