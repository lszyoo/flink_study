package windows

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.evictors.CountEvictor
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

/**
  * 延迟数据处理：
  *     默认情况下，晚于 Watermark 到达的数据会被丢弃，但若不想丢弃，可使用 Allowed Lateness 机制
  *
  * DataStream allowedLateness 方法：参数传入 Time 类型的时间间隔大小（t），其代表允许延时的最大时间，
  * Flink 窗口计算过程中会将 Window 的 EndTime 加上该时间，作为窗口最后被释放的结束时间（P），当接入的
  * 数据中 EventTime 未超过该时间（P），但 Watermark 已经超过 Window 的 EndTime 时直接触发窗口计算，
  * 相反，如果事件时间超过了最大延时时间（P），则只能对数据进行丢弃处理。
  *
  * 区分延迟数据和正常计算流：将延时数据做标记并存储到数据库中
  */
object Window_5_DelayData extends App {
  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

  val inputStream: DataStream[(Int, Long)] = env.fromCollection(List((1, 3), (2, 5), (3, 9)))

  val lateOutputTag = OutputTag[(Int, Long)]("late-data")

  val result = inputStream.keyBy(_._1)
    .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(10), Time.hours(-8)))
    .evictor(new CountEvictor[TimeWindow]())
    .allowedLateness(Time.hours(1))
    .sideOutputLateData(lateOutputTag)    // 标记迟到的数据
    .reduce((v1, v2) => (v1._1, v1._2 + v2._2))

  // 从窗口结果中获取迟到数据产生的统计结果
  val lateStream = result.getSideOutput(lateOutputTag)
}
