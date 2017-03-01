package phu.quang.le.batch

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.extensions._
import org.apache.flink.api.scala.extensions._
import org.apache.flink.api.java.io.CsvInputFormat
import org.apache.flink.api.common.operators.Order

case class Award (
  year: String,
  ceremony: Int,
  award: String,
  winner: String,
  name: String,
  film: String
)

object CSVTableTest extends App {
  val env = ExecutionEnvironment.getExecutionEnvironment
  val awards = env.readCsvFile[Award]("data/oscar.csv", lenient=true, ignoreFirstLine=true)
  
  awards
    .filter(_.award.contains("Actress"))
    .map { award => (award.name, 1) }
    .groupBy(0)
    .sum(1)
    .sortPartition(1, Order.DESCENDING)
    .setParallelism(1)
    .first(100)
    .print()
   
  awards
    .filter(_.award.contains("Actor"))
    .map { award => (award.name, 1) }
    .groupBy(0)
    .sum(1)
    .sortPartition(1, Order.DESCENDING)
    .setParallelism(1)
    .first(100)
    .print()
    
}