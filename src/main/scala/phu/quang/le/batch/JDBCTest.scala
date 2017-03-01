package phu.quang.le.batch

import org.apache.flink.api.scala._
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.common.typeinfo.TypeInformation

case class Location (
    id: Int,
    name: String
)

object JDBCTest extends App {
  val env = ExecutionEnvironment.getExecutionEnvironment
  val tableEnv = TableEnvironment.getTableEnvironment(env)
  
  var fieldTypes: Array[TypeInformation[_]] = Array(createTypeInformation[Int], createTypeInformation[String])
  var fieldNames: Array[String] = Array("id", "name")
  
  val rowTypeInfo = new RowTypeInfo(
     fieldTypes,
     fieldNames
  )
  
  val inputFormat = JDBCInputFormat.buildJDBCInputFormat()
    .setDrivername("com.mysql.jdbc.Driver")
    .setDBUrl("jdbc:mysql://localhost/admage_afnet")
    .setUsername("root")
    .setPassword("dimage")
    .setQuery("SELECT * FROM location")
    .setRowTypeInfo(rowTypeInfo)
    .finish()
    
  val ds = env.createInput(inputFormat)
  tableEnv.registerDataSet("location", ds)
}