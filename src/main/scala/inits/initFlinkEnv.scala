package inits

import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * Created by john_liu on 2018/4/7.
  */
object initFlinkEnv {


  /**
    * 初始化Flink Streaming环境
    * 可以选择是否启动checkPoint
    * @param timeCharacteristic
    * @param checkPoint  可选组件
    * @param checkpointingMode  可选组件
    * @return StreamExecutionEnvironment
    */
  def initFlinkStreamEnv(timeCharacteristic: TimeCharacteristic, checkPoint: Option[Int], checkpointingMode: CheckpointingMode = CheckpointingMode.EXACTLY_ONCE): StreamExecutionEnvironment = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(timeCharacteristic)
    checkPoint.fold(println("不启动checkPoint")) {
      x =>
        println(s"启动checkPoint:$x,checkpointing模式:$CheckpointingMode")
        env.enableCheckpointing(x)
        env.getCheckpointConfig.setCheckpointingMode(checkpointingMode)
    }
    env
  }
}
