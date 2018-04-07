package esUtils

import java.util

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.connectors.elasticsearch.IndexRequestBuilder
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

/**
  * Created by john_liu on 2018/4/7.
  */
class POJOIndexRequestBuilder[T](esIndex: EsIndex) extends IndexRequestBuilder[T] {
  override def createIndexRequest(element: T, ctx: RuntimeContext): IndexRequest = {
    lazy val requestPrefix = Requests.indexRequest.index(esIndex.indexName).`type`(esIndex.typeName).source(pojo2JavaMap(element))
    lazy val request = esIndex.id.fold(requestPrefix)(requestPrefix.id(_))
    request
  }

  private def pojo2JavaMap[T](element: T): util.HashMap[String, AnyRef] = {
    lazy val map = new util.HashMap[String, AnyRef]()
    lazy val buildKvFromField: java.lang.reflect.Field => Unit = {
      field =>
        field.setAccessible(true)
        map.put(field.getName, field.get(element))
    }
    element.getClass.getDeclaredFields.foreach(buildKvFromField(_))
    map
  }


}
