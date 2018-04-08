package taskDemo

import java.util
import java.util.Properties

import esUtils.EsIndex
import inits.{InitFlinkEnv, InitSink, InitSource}
import org.apache.flink.streaming.api.TimeCharacteristic

/**
  * Created by john_liu on 2018/4/7.
  */
trait Kafka2Es[A, B] {
  def kafka2Es: Unit = {

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
    val source = InitSource.initKafkaConsumerByKafkaStringSchema(topic, props)

    //初始化Sink，这里是Es
    val config: java.util.Map[String, String] = {
      lazy val map = new util.HashMap[String, String]()
      List("bulk.flush.max.actions", "cluster.name")
        .zip(List("", ""))
        .foreach(kv => map.put(kv._1, kv._2))
      map
    }
    val esIndex = EsIndex("", "", Option(""))
    //todo 泛型标记
    val sink = InitSink.initPojoEsSink(config, esIndex)

    //组装dataStream
    env.addSource(source)
      .map(mapKafkaRecord2SinkData(_))
      .addSink(sink)
  }
  //todo 定义kafka原始数据到sink时的最终数据的映射

  def mapKafkaRecord2SinkData[A, B](kafkaRecord: A): B


}
