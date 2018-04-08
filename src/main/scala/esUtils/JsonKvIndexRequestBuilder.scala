package esUtils

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.connectors.elasticsearch.IndexRequestBuilder
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

/**
  * Created by john_liu on 2018/4/8.
  */
class JsonKvIndexRequestBuilder(
                                 esIndex: EsIndex,
                                 esOpType: String = "index"
                               ) extends IndexRequestBuilder[(String, String)] {
  override def createIndexRequest(element: (String, String), ctx: RuntimeContext): IndexRequest = {
    lazy val value = element._2
    lazy val idKey = element._1
    Requests.indexRequest.index(esIndex.indexName).`type`(esIndex.typeName).source(value).opType(esOpType).id(idKey)
  }
}
