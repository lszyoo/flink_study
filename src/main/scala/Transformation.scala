import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._

object Transformation {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //    val dataStream = env.fromElements(("a", 3))
    //    val mapStream: DataStream[(String, Int)] = dataStream.map(t => (t._1, t._2 + 1))

    val dataStream: DataStream[String] = env.fromCollection(Array("a", "b", "c"))
    val flatMapStream = dataStream.flatMap(str => str.split(" "))
    flatMapStream.print()
    env.execute()
  }

}