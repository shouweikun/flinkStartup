package inits

import esUtils.{EsIndex, POJOIndexRequestBuilder}
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSink, IndexRequestBuilder}

/**
  * Created by john_liu on 2018/4/7.
  */
object InitSink {
  /**
    * config合法性检查
    *
    * @param propList 需要检查属性的List
    * @param config
    */
  private def vaildConfig(propList: List[String], config: java.util.Map[String, String]) = {
    propList.filter(key => !config.containsKey(key))
    match {
      case Nil => println("合法性检查通过")
      case list => throw new Exception(s"合法性检查未通过,缺少以下属性:${list.mkString(",")}")
    }
  }

  /**
    * 构造ElasticSearch 的 Sink
    *
    * @param config
    * @param indexRequestBuilder
    * @tparam T
    * @return
    */
  def initEsSink[T](config: java.util.Map[String, String], indexRequestBuilder: IndexRequestBuilder[T]): ElasticsearchSink[T] = {

    vaildConfig(List("bulk.flush.max.actions", "cluster.name"), config)
    new ElasticsearchSink(config, indexRequestBuilder)
  }

  /**
    * 构建基于POJO的ElasticSearch Sink
    *
    * @param config
    * @param esIndex
    * @param esOpType
    * @param pojoIdFieldName
    * @tparam T
    * @return
    */
  def initPojoEsSink[T](config: java.util.Map[String, String], esIndex: EsIndex, esOpType: String = "index", pojoIdFieldName: Option[String] = None): ElasticsearchSink[T] = {
    pojoIdFieldName.fold(initEsSink(config, new POJOIndexRequestBuilder[T](esIndex, esOpType)))(name => initEsSink(config, new POJOIndexRequestBuilder[T](esIndex, esOpType, name))
    )

  }
}
