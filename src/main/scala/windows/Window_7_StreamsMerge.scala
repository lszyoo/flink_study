package windows

import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
  * 多流合并：即在一个窗口中按照相同条件对两个输入数据流进行关联操作，需要保证输入的 Stream 要构建在相同的 Window
  *         上，并使用相同类型的 key 作为关联条件。
  *
  * 注意：在 Window Join 中都是 inner join 操作，即必须满足在相同窗口中，每个 Stream 中都要有 key 且 key
  *      值相同才能完成关联操作并输出结果，任何一个数据集中 key 不具备或缺失都不会输出关联的计算结果
  */
object Window_7_StreamsMerge extends App {
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  val inputStream1: DataStream[(Long, Int)] = env.fromCollection(List((1L, 2)))
  val inputStream2: DataStream[(Long, Int)] = env.fromCollection(List((2L, 2)))

  /**
    * 滚动窗口关联
    */
  // 通过 DataStream join 方法将两个数据流关联
  inputStream1.join(inputStream2)
    .where(_._1)      // 指定 inputStream1 关联的 key
    .equalTo(_._1)    // 指定 inputStream2 关联的 key
    .window(TumblingEventTimeWindows.of(Time.milliseconds(10)))
    .apply((x: (Long, Int), y: (Long, Int)) => (x._1, x._2 + y._2))

  /**
    * 滑动窗口关联：各个关联窗口计算的结果可能出现重复情况，这是由于滑动窗口有重叠造成的
    */
  // 通过 DataStream join 方法将两个数据流关联
  inputStream1.join(inputStream2)
    .where(_._1)      // 指定 inputStream1 关联的 key
    .equalTo(_._1)    // 指定 inputStream2 关联的 key
    .window(SlidingEventTimeWindows.of(Time.milliseconds(10), Time.milliseconds(2)))
    .apply((x: (Long, Int), y: (Long, Int)) => (x._1, x._2 + y._2))

  /**
    * 会话窗口关联：需要设定合理的 Gap
    */
  // 通过 DataStream join 方法将两个数据流关联
  inputStream1.join(inputStream2)
    .where(_._1)      // 指定 inputStream1 关联的 key
    .equalTo(_._1)    // 指定 inputStream2 关联的 key
    .window(EventTimeSessionWindows.withGap(Time.milliseconds(10)))
    .apply((x: (Long, Int), y: (Long, Int)) => (x._1, x._2 + y._2))

  /**
    * 间隔窗口关联：不依赖于窗口划分，interval 大小决定关联的数据多少
    */
  val windowStream: DataStream[String] =inputStream1.keyBy(_._1)
    .intervalJoin(inputStream2.keyBy(_._1))
    .between(Time.milliseconds(-2), Time.milliseconds(1))   // 设定时间上下限
    .process(new ProcessJoinFunction[(Long, Int), (Long, Int), String] {
      override def processElement(
                                   left: (Long, Int),
                                   right: (Long, Int),
                                   ctx: ProcessJoinFunction[(Long, Int),
                                     (Long, Int), String]#Context, out: Collector[String]): Unit = {
        out.collect(left + ":" + (left._2 + right._2))
      }
    })
}
