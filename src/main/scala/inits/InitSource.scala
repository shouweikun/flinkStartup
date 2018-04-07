package inits

import java.util.Properties

import kafkaSchema.KafkaStringSchema
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08
import org.apache.flink.streaming.util.serialization.{DeserializationSchema, SerializationSchema}

/**
  * Created by john_liu on 2018/4/7.
  */
object InitSource {
  /**
    * prop合法性检查
    *
    * @param propList 需要检查属性的List
    * @param props
    */
  private def vaildProps(propList: List[String], props: Properties) = {
    propList
      .filter(key => !props.containsKey(key))
    match {
      case Nil => println("props合法性检查通过")
      case list => throw new Exception(s"props合法性检查未通过,缺少一下属性:${list.mkString(",")}")
    }
  }

  /**
    * 初始化Flink用KafkaConsumer
    *
    * @param topic  kafka的topic
    * @param props  kafka的配置 "zookeeper.connect", "bootstrap.servers", "group.id"都需要有
    * @param schema kafka 序列化烦序列化schema 实现请参考KafkaStringSchema
    * @tparam T schema的类型信息
    * @return FlinkKafkaConsumer08
    */
  def initKafkaConsumer[T](topic: String, props: Properties, schema: DeserializationSchema[T]): FlinkKafkaConsumer08[T] = {
    vaildProps(List("zookeeper.connect", "bootstrap.servers", "group.id"), props)
    new FlinkKafkaConsumer08[T](topic, schema, props)
  }

  /**
    * 以String类型初始化KafkaConsumer
    *
    * @param topic kafka的topic
    * @param props kafka的配置 "zookeeper.connect", "bootstrap.servers", "group.id"都需要有
    * @return FlinkKafkaConsumer08[String]
    */
  def initKafkaConsumerByKafkaStringSchema(topic: String, props: Properties): FlinkKafkaConsumer08[String] = initKafkaConsumer(topic, props, KafkaStringSchema)
}
