package taskDemo

import java.util.Properties

import inits.{InitFlinkEnv, InitSink, InitSource}
import org.apache.flink.streaming.api.TimeCharacteristic

/**
  * Created by john_liu on 2018/4/7.
  */
object Kafka2Es {
  def main(args: Array[String]): Unit = {

    //初始化Flink 环境
    val timeCharacteristic = TimeCharacteristic.ProcessingTime
    val env = InitFlinkEnv.initFlinkStreamEnv(timeCharacteristic)

    //初始化Source,这里是kafka
    val topic: String = ""
    lazy val props = {
      val prop = new Properties()
      List("zookeeper.connect", "bootstrap.servers", "group.id")
        .zip(List("", "", ""))
        .foreach(kv => prop.setProperty(kv._1, kv._2))
      prop
    }
    val source = InitSource.initKafkaConsumerByKafkaStringSchema(topic,props)

    //初始化Sink，这里是Es
    val sink = InitSink.initEsSink()
  }
}
