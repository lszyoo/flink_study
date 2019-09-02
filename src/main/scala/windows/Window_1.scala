package windows

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
  * 默认窗口时间的时区是 UTC-0，在国内需要指定 Time.hours(-8) 的偏移量
  */
object Window_1 extends App {
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  val inputStream1 = env.fromCollection(List("abc" -> 1, "def" -> 2))
  val inputStream2 = env.fromCollection(List(("abc", 1), ("def", 2)))

  // 根据 key 分区的数据，调用 window，不同 task 并行处理
  inputStream1.keyBy(_._1).window(new TumblingProcessingTimeWindows())

  // non-key 的数据，调用 windowAll，在一个 task 执行
  inputStream2.windowAll(new SlidingProcessingTimeWindows())

  /**
    * 滚动窗口（Tumbling Windows）：根据固定时间或大小进行切分，且窗口和窗口之间的元素互不重叠
    *
    * 分类：TumblingEventTimeWindows 和 TumblingProcessTimeWindows
    *
    * 适用于按照固定时间大小和周期统计某一指标的情形，不适用于某些有前后关系的数据计算
    */
  val tumblingEventTimeWindows = inputStream2.keyBy(_._2)
    .window(TumblingEventTimeWindows.of(Time.seconds(10), Time.hours(-8)))
    .process(new ProcessWindowFunction {        // 定义窗口函数
      override def process(key: Int, context: Context, elements: Iterable[(String, Int)], out: Collector[Nothing]): Unit = {}
    })

  val tumblingProcessingTimeWindows = inputStream2.keyBy(_._2)
    .window(TumblingProcessingTimeWindows.of(Time.seconds(10), Time.hours(-8)))
    .process(new ProcessWindowFunction {
      override def process(key: Int, context: Context, elements: Iterable[(String, Int)], out: Collector[Nothing]): Unit = {}
    })

  // 另一种方式
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  inputStream2.keyBy(_._2)
    .timeWindow(Time.seconds(10))
    .process(new ProcessWindowFunction {
      override def process(key: Int, context: Context, elements: Iterable[(String, Int)], out: Collector[Nothing]): Unit = {}
    })

  /**
    * 滑动窗口（Sliding Windows）
    *
    * 分类：TumblingEventTimeWindows 和 TumblingProcessTimeWindows
    *
    * 当 SlideTime < WindowSize 时，窗口重叠
    * 当 SlideTime > WindowSize 时，窗口不连续
    * 当 SlideTime = WindowSize 时，即 滚动窗口
    *
    * 滑动窗口能够帮助用户根据设定的统计频率计算指定窗口大小的统计指标，如：每隔 30s 统计最近 10min 内活跃用户数
    */
  val slidingEventTimeWindows = inputStream2.keyBy(_._2)
    .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(10), Time.hours(-8)))
    .process(new ProcessWindowFunction {
      override def process(key: Int, context: Context, elements: Iterable[(String, Int)], out: Collector[Nothing]): Unit = {}
    })

  val slidingProcessingTimeWindows = inputStream2.keyBy(_._2)
    .window(SlidingProcessingTimeWindows.of(Time.hours(1), Time.minutes(10), Time.hours(-8)))
    .process(new ProcessWindowFunction {
      override def process(key: Int, context: Context, elements: Iterable[(String, Int)], out: Collector[Nothing]): Unit = {}
    })

  // 另一种方式
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  inputStream2.keyBy(_._2)
    .timeWindow(Time.seconds(10), Time.seconds(1))
    .process(new ProcessWindowFunction {
      override def process(key: Int, context: Context, elements: Iterable[(String, Int)], out: Collector[Nothing]): Unit = {}
    })

  /**
    * 会话窗口（Session Windows）：主要是将某段时间内活跃度比较高的数据聚合成一个窗口进行计算，窗口的触发条件
    *         是 Session Gap，是指在规定的时间内如果没有数据活跃进入，则认为窗口结束，然后触发窗口计算
    *
    * 注意：如果数据一直不间断地进入窗口，会导致窗口始终不触发
    *
    * 分类：TumblingEventTimeWindows 和 TumblingProcessTimeWindows
    *
    * Session Windows 为每个进入的数据都创建一个窗口，最后将距离 Session Gap 最近的窗口合并，并计算结果，
    * 需要 Trigger 和 Windows Function 来触发合并
    *
    * 适用于非连续型数据处理或周期性产生数据的场景，根据用户在线上某段时间内的活跃度对用户行为进行分析
    */
  val eventTimeSessionWindows = inputStream2.keyBy(_._2)
    .window(EventTimeSessionWindows.withGap(Time.milliseconds(10)))
    .process(new ProcessWindowFunction {
      override def process(key: Int, context: Context, elements: Iterable[(String, Int)], out: Collector[Nothing]): Unit = {}
    })

  val processingTimeSessionWindows = inputStream2.keyBy(_._2)
    .window(ProcessingTimeSessionWindows.withGap(Time.milliseconds(10)))
    .process(new ProcessWindowFunction {
      override def process(key: Int, context: Context, elements: Iterable[(String, Int)], out: Collector[Nothing]): Unit = {}
    })

  inputStream2.keyBy(_._2)
    .window(EventTimeSessionWindows.withDynamicGap(new SessionWindowTimeGapExtractor[(String, Int)] {
      // 动态指定 Session Gap
      override def extract(element: (String, Int)): Long = element._1.toLong
    }))
    .process(new ProcessWindowFunction {
    override def process(key: Int, context: Context, elements: Iterable[(String, Int)], out: Collector[Nothing]): Unit = {}
  })

  inputStream2.keyBy(_._2)
    .window(ProcessingTimeSessionWindows.withDynamicGap(new SessionWindowTimeGapExtractor[(String, Int)] {
      // 动态指定 Session Gap
      override def extract(element: (String, Int)): Long = element._1.toLong
    }))
    .process(new ProcessWindowFunction {
      override def process(key: Int, context: Context, elements: Iterable[(String, Int)], out: Collector[Nothing]): Unit = {}
    })

  /**
    * 全局窗口（Global Windows）：将所有相同的 key 的数据分配到单个窗口中计算结果，窗口没有起始和结束时间，
    *       窗口需要借助于 Trigger 来触发计算，同时还需指定相应的数据清理机制，否则数据会一直保留在内存中
    */
  val globalWindows = inputStream2.keyBy(_._2)
    .window(GlobalWindows.create())
    .process(new ProcessWindowFunction {
      override def process(key: Int, context: Context, elements: Iterable[(String, Int)], out: Collector[Nothing]): Unit = {}
    })
}
