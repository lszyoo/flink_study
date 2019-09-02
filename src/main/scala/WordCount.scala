import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}

object WordCount {
  def main(args: Array[String]): Unit = {
    // 第一步：设定执行环境
    // 流式
    // 设定 Flink 运行环境，如果在本地启动则创建本地环境，如果在集群上启动，则创建集群环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 指定并行度创建本地执行环境
    val env1 = StreamExecutionEnvironment.createLocalEnvironment(5)
    // 指定远程 JobManagerIP 和 RPC 端口以及运行程序所在 jar 包及其依赖
    val env2 = StreamExecutionEnvironment.createRemoteEnvironment("JobManagerHost", 6021, 5, "/user/application.jar")

    // 批式
    // 设定 Flink 运行环境，如果在本地启动则创建本地环境，如果在集群上启动，则创建集群环境
    val env3 = ExecutionEnvironment.getExecutionEnvironment
    // 指定并行度创建本地执行环境
    val env4 = ExecutionEnvironment.createLocalEnvironment(5)
    // 指定远程 JobManagerIP 和 RPC 端口以及运行程序所在 jar 包及其依赖
    val env5 = ExecutionEnvironment.createRemoteEnvironment("JobManagerHost", 6021, 5, "/user/application.jar")

    // 第二步：指定数据源地址
    val text = env.readTextFile("D:\\IDEA\\flink_study\\src\\main\\scala\\flink原理实战与性能优化\\words.txt")

    // 创建数据集
    val intStream: DataStream[Int] = env.fromElements(2, 1, 3, 1, 5)
    val stringStream: DataStream[String] = env.fromElements("hello", "flink")
    val dataStream: DataStream[Int] = env.fromCollection(Array(3, 1, 2, 1, 5))
    val dataStream1: DataStream[Int] = env.fromCollection(List(3, 1, 2, 1, 5))

    val tupleStream2: DataStream[Tuple2[String, Int]] = env.fromElements(new Tuple2("a", 1), new Tuple2("b", 2))

    // 第三步：对数据集指定转换操作逻辑
    val counts: DataStream[(String, Int)] = text
      .flatMap(_.toLowerCase().split(" "))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      .sum(1)

    val params = ParameterTool.fromSystemProperties()
    // 第四步：指定计算结果输出位置
    if (params.has("output")) {
      counts.writeAsText(params.get("output"))
    } else {
      println("Printing result to stdout. Use --output to specify output path.")
      counts.print()
    }
    // 第五步：指定名称并触发流式任务
    env.execute("Streaming WordCount")
  }
}