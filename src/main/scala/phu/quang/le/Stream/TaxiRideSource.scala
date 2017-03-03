package phu.quang.le.Stream

import org.apache.flink.streaming.api.functions.source._
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import java.io.{ BufferedReader, InputStream }
import java.util.zip.GZIPInputStream
import java.io.FileInputStream
import scala.tools.jline_embedded.internal.InputStreamReader
import java.io.IOException

class TaxiRideSource (filePath: String, maxDelaySecs: Int) extends RichParallelSourceFunction[TaxiRide] {
  val maxDelay: Int = maxDelaySecs * 1000
  
  private var reader: BufferedReader = null
  private var gzipStream: InputStream = null
  
  def cancel(): Unit = {
    ???
  }
  
  @throws(classOf[IOException])
  def run(ctx: SourceContext[TaxiRide]): Unit = {
    this.gzipStream = new GZIPInputStream(new FileInputStream(filePath))
    this.reader = new BufferedReader(new InputStreamReader(gzipStream, "UTF-8"))
    
    this.reader.close()
    this.gzipStream.close()
    this.reader = null
    this.gzipStream = null
  }
  
}