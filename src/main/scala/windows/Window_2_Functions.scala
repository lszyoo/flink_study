package windows

import org.apache.flink.api.common.functions.{AggregateFunction, ReduceFunction, RichMapFunction}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


object Window_2_Functions extends App {
  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

  val inputStream1: DataStream[(Int, Long)] = env.fromCollection(List((1, 3), (2, 5), (3, 9)))
  val inputStream2: DataStream[(String, Long)] = env.fromCollection(List(("ab", 1), ("cd", 2)))

  /**
    * 增量
    */
  // ReduceFunction
  val reduceWindowStream1 = inputStream1.keyBy(_._1)
    .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(10), Time.hours(-8)))
    .reduce((v1, v2) => (v1._1, v1._2 + v2._2))

  val reduceWindowStream2 = inputStream1.keyBy(_._1)
    .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(10), Time.hours(-8)))
    .reduce(new ReduceFunction[(Int, Long)] {
      override def reduce(value1: (Int, Long), value2: (Int, Long)): (Int, Long) = (value1._1, value1._2 + value2._2)
    })

  // AggregateFunction
  class AverageAggregate extends AggregateFunction[(String, Long), (Long, Long), Double] {
    // 定义 createAccumulator 为两个参数的元组
    override def createAccumulator(): (Long, Long) = (0L, 0L)
    // 定义输入数据累加到 accumulator 的逻辑
    override def add(value: (String, Long), accumulator: (Long, Long)): (Long, Long) = (accumulator._1 + value._2, accumulator._2 + 1L)
    // 根据累加器得出结果
    override def getResult(accumulator: (Long, Long)): Double = accumulator._1 / accumulator._2
    // 定义累加器合并的逻辑
    override def merge(acc1: (Long, Long), acc2: (Long, Long)): (Long, Long) = (acc1._1 + acc2._1, acc1._2 + acc2._2)
  }

  val aggregateWindowStream = inputStream2.keyBy(_._1)
    .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(10), Time.hours(-8)))
    .aggregate(new AverageAggregate)

  // FoldFunction 已过时，见 aggregate

  /**
    * 全量
    */
  // ProcessWindowFunction
  val staticStream = inputStream2.keyBy(_._1)
    .timeWindow(Time.seconds(10))
    .process(new StaticProcessFunction)

  class StaticProcessFunction extends ProcessWindowFunction[
    (String, Long),
    (String, Long, Long, Long, Long, Long),
    String,
    TimeWindow
    ] {
    override def process(
                          key: String,
                          context: Context,
                          elements: Iterable[(String, Long)],
                          out: Collector[(String, Long, Long, Long, Long, Long)]
                        ): Unit = {
      val sum = elements.map(_._2).sum
      val min = elements.map(_._2).min
      val max = elements.map(_._2).max
      val avg = sum / elements.size
      val windowEnd = context.window.getEnd
      out.collect((key, min, max, sum, avg, windowEnd))
    }
  }

  /**
    * ProcessWindowFunction 状态操作
    *
    * Per-window State：基于窗口之上的状态数据，针对指定的 key 在窗口上存储
    * 分为两类：
    *     globalState：窗口中的 keyed state 数据不限定在某个窗口中
    *     windowState：窗口中的 keyed state 数据限定在某个窗口中
    *
    * 如：将用户 id 作为 key，求取每个用户 id 最近一个小时的登录数，若平台中共有 3000 个用户，则窗口计算中
    *    会创建 3000 个窗口实例，每个窗口实例都会保存每个 key 的状态数据
    *
    * 注意：及时清理状态数据，可调用 clear() 方法
    *
    * 场景：适用于同一窗口多次触发计算的场景，或对于迟到数据来触发窗口计算
    */
  val perWindown = inputStream2.keyBy(_._1)
      .map(new RichMapFunction[(String, Long), Long] {
        var state: ValueState[Long] = null
        override def open(parameters: Configuration): Unit = {
          state = getRuntimeContext.getState(ValueStateDescriptor[Long])
        }
        override def map(value: (String, Long)): (Long, (String, Long)) = {
          val count = state.value() + 1
          state.update(value._2)
          (count, value)
        }
      })

  /**
    * 增量 和 全量 整合
    */
  // 求最大值 和 对应窗口的终止时间
  val result = inputStream2.keyBy(_._1)
    .timeWindow(Time.seconds(10))
    .reduce(
      // 定义 ReduceFunction 求取最大值
      (r1: (String, Long), r2: (String, Long)) => if (r1._2 > r2._2) r1 else r2,
      // 对应窗口的终止时间
      (
        key: String,
        window: TimeWindow,
        maxReadings: Iterable[(String, Long)],
        out: Collector[(Long, (String, Long))]      // 错误写法：Collector[Long, (String, Long)]
      ) => out.collect((window.getEnd, maxReadings.iterator.next()))
    )
}

