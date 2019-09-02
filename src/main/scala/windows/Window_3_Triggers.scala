package windows

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.state.ReducingStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.Trigger.TriggerContext
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.util.Collector
/**
  * 每种触发器都对应不同的 Window Assigner
  *
  * 窗口触发器：
  *
  *     EventTimeTrigger：通过对比 Watermark 和 窗口 EndTime 确定是否触发窗口，如果 Watermark 的时间
  *                       大于 WindowsEndTime 则触发计算，否则窗口继续等待；
  *
  *     ProcessTimeTrigger：通过对比 ProcessTime 和窗口 EndTime 确定是否触发窗口，如果窗口 ProcessTime
  *                         大于 WindowsEndTime 则触发计算，否则窗口继续等待；
  *
  *     ContinuousEventTimeTrigger：根据间隔时间周期性触发窗口 或者 Window 的结束时间小于当前 EventTime
  *                                 触发窗口计算；
  *
  *     ContinuousProcessingTimeTrigger：根据间隔时间周期性触发窗口 或者 Window 的结束时间小于当前 ProcessTime
  *                                      触发窗口计算；
  *     CountTrigger：根据接入数据量是否超过设定的阈值确定是否触发窗口计算；
  *
  *     DeltaTrigger：根据接入数据计算出来的 Delta 指标是否超过指定的 Threshold，判断是否触发窗口计算；
  *
  *     PurgingTrigger：可以将任意触发器作为参数转换为 Purge 类型触发器，计算完成后数据将被清理；
  *
  *     自定义 Trigger：继承抽象类 Trigger
  *         重写方法：onElement：针对每一个接入窗口的数据元素进行触发操作
  *                 onProcessingTime：根据接入窗口的 ProcessTime 进行触发操作
  *                 onEventTime：根据接入窗口的 EventTime 进行触发操作
  *                 clear：执行窗口及状态数据的清除方法
  *                 onMerge：对多个窗口进行 Merge 操作，同时进行状态的合并
  *
  *     判断窗口触发方法返回的结果有如下类型：
  *           CONTINUE：当前不触发计算，继续等待；
  *           FIRE：触发计算，数据继续保留；
  *           PURGE：代表窗口内数据清除；
  *           FIRE_AND_PURGE：触发计算，并清除对应的数据
  *
  * 注意：GlobalWindow 默认使用触发器是 NeverTrigger，则必须自定义触发器，否则数据接入后永远不会计算
  */
object WindowTrigger extends App {
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  val inputStream: DataStream[(String, Long)] = env.fromCollection(List(("ab", 1), ("cd", 2)))

  val windowStream = inputStream.keyBy(_._2)
    .window(EventTimeSessionWindows.withGap(Time.milliseconds(10)))
    .trigger(EarlyTrigger.of(Time.seconds(5)))
    .process(new ProcessWindowFunction {
      override def process(key: Long, context: Context, elements: Iterable[(String, Long)], out: Collector[Nothing]): Unit = {}
    })
}


/**
  * 通过自定义 EarlyTrigger 实现窗口触发的时间间隔，当窗口是 SessionWindow 时，如果用户长时间不停地操作，
  * 导致 SessionGap 一直都不生成，因此该用户的数据会长期存储在窗口中，如果需要至少每个五分钟统计一下窗口的结
  * 果，此时就需要自定义 Trigger 来实现。本例实现了 ContinuousEventTimeTrigger 类似的功能。
  */
class EarlyTrigger(interval: Long) extends Trigger[Object, TimeWindow] {
  // 重新定义 Java.lang.Long 类型为 JLong
  private type JLong = java.lang.Long

  // 实现函数，求取两个时间戳的最小值
  private val min = new ReduceFunction[JLong] {
    override def reduce(value1: JLong, value2: JLong): JLong = Math.min(value1, value2)
  }

  private val stateDec = new ReducingStateDescriptor[JLong]("trigger-time", min, Types.LONG)

  // 处理接入的元素，每次都会被调用
  override def onElement(
                          element: Object,
                          timestamp: Long,
                          window: TimeWindow,
                          ctx: Trigger.TriggerContext
                        ): TriggerResult = {
    // 如果当前的 watermark 超过窗口的结束时间，则清除定时器内容，直接触发窗口计算
    if (window.maxTimestamp() <= ctx.getCurrentWatermark) {
      clearTimerForState(ctx)
      TriggerResult.FIRE
    } else {    // 否则将窗口的结束时间注册给 EventTime 定时器
      ctx.registerEventTimeTimer(window.maxTimestamp())
      // 获取当前分区状态中的时间戳
      val fireTimestamp = ctx.getPartitionedState(stateDec)
      // 如果第一次执行，则对元素的 Timestamp 进行 floor 操作，取整后加上传入的实例变量 interval，
      // 得到下一次触发时间并注册，添加到状态中
      if (fireTimestamp.get() == null) {
        val start = timestamp - (timestamp % interval)
        val nextFireTimestamp = start + interval
        ctx.registerEventTimeTimer(nextFireTimestamp)
        fireTimestamp.add(nextFireTimestamp)
      }
      // 此时继续等待
      TriggerResult.CONTINUE
    }
  }

  // 时间概念类型不选择 ProcessTime，不会基于 ProcessTime 触发，直接返回 CONTINUE
  override def onProcessingTime(time: Long, window: TimeWindow,
                                ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  // 当 Watermark 超过注册时间时，就会执行 onEventTime 方法
  override def onEventTime(
                            time: Long,
                            window: TimeWindow,
                            ctx: Trigger.TriggerContext): TriggerResult = {
    // 如果事件时间等于 maxTimestamp 时间，则清空状态数据，并触发计算
    if (time == window.maxTimestamp()) {
      clearTimerForState(ctx)
      TriggerResult.FIRE
    } else {    // 否则，获取状态中的值（maxTimestamp 和 nextFireTimestamp 中的最小值）
      val fireTimestamp = ctx.getPartitionedState(stateDec)
      // 如果状态中的值等于事件时间，则清除定时器时间戳，注册下一个 interval 时间戳，并触发窗口计算
      if (fireTimestamp.get() == time) {
        fireTimestamp.clear()
        fireTimestamp.add(time + interval)
        ctx.registerEventTimeTimer(time + interval)
        TriggerResult.FIRE
      } else {     // 否则继续等待
        TriggerResult.CONTINUE
      }
    }
  }

  // 从 TriggerContext 中获取状态中的值，并从定时器中清除
  private def clearTimerForState(ctx: TriggerContext) = {
    val timestamp = ctx.getPartitionedState(stateDec).get()
    if (timestamp != null)
      ctx.deleteEventTimeTimer(timestamp)
  }

  // 用于 SessionWindow 的 merge，指定可以 merge
  override def canMerge: Boolean = true

  // 定义窗口状态 merge 的逻辑
  override def onMerge(window: TimeWindow, ctx: Trigger.OnMergeContext): TriggerResult = {
    ctx.mergePartitionedState(stateDec)
    val nextFireTimestamp = ctx.getPartitionedState(stateDec).get()
    if (nextFireTimestamp != null) {
      ctx.registerEventTimeTimer(nextFireTimestamp)
    }
    TriggerResult.CONTINUE
  }

  // 删除定时器中已经出发的时间戳，并调用 Trigger 的 clear 方法
  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {
    ctx.deleteEventTimeTimer(window.maxTimestamp())
    val fireTimestamp = ctx.getPartitionedState(stateDec)
    val timestamp = fireTimestamp.get()
    if (timestamp != null) {
      ctx.deleteEventTimeTimer(timestamp)
      fireTimestamp.clear()
    }
  }

  override def toString: String = s"EarlyTrigger($interval)"
}

// 类中的 of 方法，传入 interval，作为参数传入此类的构造器，时间转换为毫秒
object EarlyTrigger {
  def of(interval: Time) = new EarlyTrigger(interval.toMilliseconds)
}
