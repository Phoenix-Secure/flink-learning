package phu.quang.le.Stream

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.wikiedits._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010
import org.apache.flink.streaming.util.serialization.SimpleStringSchema

object WikipediaAnalysis extends App {
  val env = StreamExecutionEnvironment.createLocalEnvironment()
  val edits: DataStream[WikipediaEditEvent] = env.addSource(new WikipediaEditsSource())
  val keyedEdits = edits.keyBy(_.getUser)
  val result = keyedEdits
    .timeWindow(Time.seconds(5))
    .fold(("", 0L))((acc, event) => (event.getUser, acc._2 + event.getByteDiff))
    
  result
    .map(_.toString())
    .addSink(new FlinkKafkaProducer010("localhost:9092", "wiki_results", new SimpleStringSchema()))
  env.execute()
}