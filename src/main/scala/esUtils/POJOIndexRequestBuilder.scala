package esUtils

import java.util

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.connectors.elasticsearch.IndexRequestBuilder
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

/**
  * Created by john_liu on 2018/4/7.
  */
class POJOIndexRequestBuilder[T](
                                  esIndex: EsIndex,
                                  esOpType: String = "index",
                                  pojoIdFieldName: String = "id"
                                ) extends IndexRequestBuilder[T] {
  /**
    * 根据EsIndex来编辑Es请求
    *
    * @param element
    * @param ctx
    * @return
    */
  override def createIndexRequest(element: T, ctx: RuntimeContext): IndexRequest = {
    lazy val requestPrefix = Requests.indexRequest.index(esIndex.indexName).`type`(esIndex.typeName).source(pojo2JavaMap(element)).opType(esOpType).id(getIdFromPojo(element))
    lazy val request = esIndex.id.fold(requestPrefix)(requestPrefix.id(_))
    request
  }

  /**
    * 将POJO转换成Java的HashMap
    *
    * @param element
    * @return
    */
  private def pojo2JavaMap(element: T): util.HashMap[String, AnyRef] = {
    lazy val map = new util.HashMap[String, AnyRef]()
    lazy val buildKvFromField: java.lang.reflect.Field => Unit = {
      field =>
        field.setAccessible(true)
        map.put(field.getName, field.get(element))
    }
    element.getClass.getDeclaredFields.foreach(buildKvFromField(_))
    map
  }

  /**
    * 从POJO对象中获得id值
    * @param element
    * @param idFieldName
    * @return
    */
  private def getIdFromPojo(element: T, idFieldName: String = this.pojoIdFieldName): String = {
    val idField = element.getClass.getDeclaredField(idFieldName)
    idField.setAccessible(true)
    idField.get(element).toString
  }


}
