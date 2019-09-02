import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time

object TimeWatermarkSet extends App {
  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

  val inputs: List[Tuple3[String, Long, Int]] = List(("a", 1L, 1), ("b", 1L, 1), ("c", 3L, 1))

  /**
    * 在 Source Function 中直接定义 Timestamps 和 Watermark
    */
  val dataStream: DataStream[(String, Long, Int)] = env.addSource(new SourceFunction[Tuple3[String, Long, Int]] {
    override def run(ctx: SourceContext[(String, Long, Int)]): Unit = {
      inputs.foreach(input => {
        ctx.collectWithTimestamp(input, input._2)         // 抽取 EventTime
        ctx.emitWatermark(new Watermark(input._2 - 1))    // 设置 Watermark
      })
      // 设置默认 Watermark
      ctx.emitWatermark(new Watermark(Long.MaxValue))
    }

    override def cancel(): Unit = {}
  })

  /**
    * 通过 Flink 自带的 Timestamp Assigner 指定 Timestamp 和 生成 Watermark
    *
    * 根据 Watermark 的生成形式，分为两类：
    * （1）Periodic Watermark：是根据设定时间间隔周期性地生成
    *      -- 升序模式：事件顺序生成，没有乱序，提取最新 Timestamp 为 Watermark，不需要设置
    *      -- 乱序模式：通过固定时间间隔来指定 Watermark 落后于 Timestamp 的区间长度
    * （2）Punctuated Watermark：是根据接入数据的数量生成
    */

  // 在系统中指定 EventTime 概念
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  val ds: DataStream[(String, Long, Int)] = env.fromCollection(inputs)

  // 有序事件：使用 Ascending Timestamp Assigner 指定 Timestamp 和 Watermark，最后不要指定，影响性能
  val withTimestampAndWatermark: DataStream[(String, Long, Int)] = ds.assignAscendingTimestamps(_._3)
  // 对数据集进行窗口运算
  val result: DataStream[(String, Long, Int)] = withTimestampAndWatermark
    .keyBy(0)
    .timeWindow(Time.seconds(10))
    .sum("_2")


  // 乱序事件：使用固定时延间隔的 Timestamp Assigner 指定 Timestamp 和 Watermark
  ds.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(String, Long, Int)](Time.seconds(10)) {
    // 抽取 EventTime
    override def extractTimestamp(element: (String, Long, Int)): Long = element._2

  })


  /**
    * 自定义 Timestamp Assigner 和 Watermark Generator
    */
  // Periodic Watermark 自定义生成
  // 首先设置 Watermark 产生的时间周期
  val config = new ExecutionConfig
  config.setAutoWatermarkInterval(10)

  class PeriodicAssigner extends AssignerWithPeriodicWatermarks[(String, Long, Int)] {
    // 1 秒延时设定，表示在 1 秒内的数据延时有效，超过 1 秒的事件被认为是迟到事件
    val maxOutOfOrderness = 1000L
    var currentMaxTimestamp: Long = _
    override def getCurrentWatermark: Watermark = {
      new Watermark(currentMaxTimestamp - maxOutOfOrderness)
    }

    override def extractTimestamp(event: (String, Long, Int), previousElementTimestamp: Long): Long = {
      val currentTimestamp = event._2
      currentMaxTimestamp = Math.max(currentTimestamp, currentMaxTimestamp)
      currentMaxTimestamp
    }
  }

  // Punctuated Watermark 自定义生成
  class PunctuatedAssigner extends AssignerWithPunctuatedWatermarks[(String, Long, Int)] {
    // 定义生成 Watermark
    override def checkAndGetNextWatermark(lastElement: (String, Long, Int), extractedTimestamp: Long): Watermark = {
      if (lastElement._3 == 0)
        new Watermark(extractedTimestamp)
      else
        null
    }
    // 定义抽取 Timestamp
    override def extractTimestamp(element: (String, Long, Int), previousElementTimestamp: Long): Long = element._2
  }
}