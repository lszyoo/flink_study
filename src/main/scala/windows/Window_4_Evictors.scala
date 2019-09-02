package windows

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.evictors.CountEvictor
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

/**
  * 数据剔除器
  *     CountEvictor：保持在窗口中具有固定数量的记录，将超过指定大小的数据在窗口计算前剔除；
  *
  *     DeltaEvictor：通过定义 DeltaFunction 和指定 threshold，并计算 Windows 中的元素与最新元素之间
  *                   的 Delta 大小，如果超过 threshold 则将当前数据元素剔除；
  *
  *     TimeEvictor：通过指定时间间隔，将当前窗口中最新元素的时间减去 interval，然后将小于该结果的数据全部
  *                  剔除，其本质就是将具有最新时间的数据选择出来，删除过时的数据
  *
  *     自定义 Evictor：
  *         evictBefore：定义 WindowFunction 触发之前的数据剔除逻辑
  *         evictAfter：定义 WindowFunction 触发之后的数据剔除逻辑
  *
  *     注意：从 Evictor 接口中能够看出，元素在窗口中其实并没有保持顺序，因此如果对数据在进入 WindowFunction
  *          前后进行预处理，其实和数据在进入窗口中的顺序是没有关系的，无法控制绝对的先后关系
  */
object Window_4_Evictors extends App {
  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

  val inputStream: DataStream[(Int, Long)] = env.fromCollection(List((1, 3), (2, 5), (3, 9)))

  val reduceWindowStream = inputStream.keyBy(_._1)
    .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(10), Time.hours(-8)))
    .evictor(new CountEvictor[TimeWindow]())
    .reduce((v1, v2) => (v1._1, v1._2 + v2._2))
}
