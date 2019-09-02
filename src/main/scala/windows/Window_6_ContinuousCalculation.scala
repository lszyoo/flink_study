package windows

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
  * 连续窗口计算
  */
object Window_6_ContinuousCalculation extends App {
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  val inputStream = env.fromCollection(List(("abc", 1), ("def", 2)))
  /**
    * 独立窗口计算
    */
  val windowStream1 = inputStream.keyBy(_._1)
    .window(EventTimeSessionWindows.withGap(Time.milliseconds(100)))
    .process(new ProcessWindowFunction {
      override def process(key: String, context: Context, elements: Iterable[(String, Int)], out: Collector[Nothing]): Unit = {}
    })

  val windowStream2 = inputStream.keyBy(_._1)
    .window(EventTimeSessionWindows.withGap(Time.milliseconds(10)))
    .process(new ProcessWindowFunction {
      override def process(key: String, context: Context, elements: Iterable[(String, Int)], out: Collector[Nothing]): Unit = {}
    })

  /**
    * 连续窗口计算：
    *     上游窗口的计算结果是下游窗口计算的输入，窗口算子和算子之间上下游关联，窗口之间的元素数据信息科共享；
    *
    *     两个窗口的类型和 EndTime 都是一致的，真正触发计算的是下游窗口
    */
  val windowStream3 = inputStream.keyBy(_._1)
    .window(TumblingEventTimeWindows.of(Time.milliseconds(10)))
    .reduce((v1, v2) => (v1._1, v1._2 + v2._2))
  val windowStream4 = windowStream3
    .windowAll(TumblingEventTimeWindows.of(Time.milliseconds(100)))
    .max(1)
}
