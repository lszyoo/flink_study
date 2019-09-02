import java.util.Properties

import org.apache.flink.api.common.io.FilePathFilter
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.ExecutionEnvironment
import org.apache.flink.api.java.io.CsvInputFormat
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.source.FileProcessingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.api.scala._

object DataSource extends App {
  val env = ExecutionEnvironment.getExecutionEnvironment
  // 直接读取
  val textStream = env.readTextFile("/user/local/data_example.log")
  // 通过指定 CSVInputFormat 读取 CSV 文件
  val csvStream = env.readFile(new CsvInputFormat[String](new Path("/user/local/data_example.csv")) {
    override def fillRecord(reuse: String, parsedValues: Array[AnyRef]): String = {
      return null
    }
  }, "/user/local/data_example.csv")

  val env1 = StreamExecutionEnvironment.getExecutionEnvironment
  // 文件数据源
  // 全部还是一次、监测时间间隔、文件过滤
  val csvInputFormat = new CsvInputFormat[String](new Path("/user/local/data_example.csv")) {
    override def fillRecord(reuse: String, parsedValues: Array[AnyRef]): String = {
      return null
    }
  }
  csvInputFormat.setFilesFilter(FilePathFilter.createDefaultFilter())
  val textFilter = env1.readFile(csvInputFormat, "/user/local/data_example.csv", FileProcessingMode.PROCESS_CONTINUOUSLY, 500)

  // socket数据源
  val socketDataStream = env1.socketTextStream("localhost", 9999, '\t', 6)

  // 集合数据源
  val dataStream = env1.fromElements(Tuple2(1L, 3L), Tuple2(2, 4))
  val dataStream1 = env1.fromCollection(Array("1", "2"))
  val dataStream12 = env1.fromCollection(List("1", "2"))

  // 外部数据源
  val properties = new Properties()
  properties.setProperty("bootstrap.servers", "localhost:9092")
  properties.setProperty("zookeeper.connect", "localhost:2181")
  properties.setProperty("group.id", "test")
  val input = env1.addSource(new FlinkKafkaConsumer010(properties.getProperty("input-data-topic"), new SimpleStringSchema(), properties))
}