package phu.quang.le.Stream

import org.joda.time.format._
import org.joda.time._
import java.util.Locale

case class GeoPoint(
    lon: Double,
    lat: Double
)

object TaxiRide {
  @transient
  private final val TimeFormatter: DateTimeFormatter =
    DateTimeFormat.forPattern("yyyy-MM-DD HH:mm:ss").withLocale(Locale.US).withZoneUTC()
  
  def from(line: String): TaxiRide = {
    val tokens: Array[String] = line.split(",")
    if (tokens.length != 7) {
      throw new RuntimeException("Invalid record: " + line)
    }
    try {
     val rideId = tokens(0).toLong 
     val time = DateTime.parse(tokens(1), TimeFormatter)
     val isStart = tokens(2) == "START"
     val lon = if (tokens(3).length() > 0) tokens(3).toDouble else 0.0
     val lat = if (tokens(4).length() > 0) tokens(4).toDouble else 0.0
     val passengerCnt = tokens(5).toShort
     val travelDistance = if (tokens(6).length() > 0) tokens(6).toFloat else 0.0f
     
     new TaxiRide(rideId, time, isStart, new GeoPoint(lon, lat), passengerCnt, travelDistance)
    } catch {
      case nfe: NumberFormatException =>
        throw new RuntimeException(s"Invalid record: $line")
    }
  }
}

class TaxiRide (
    var rideId: Long,
    var time: DateTime,
    var isStart: Boolean,
    var location: GeoPoint,
    var passengerCnt: Short,
    var travelDist: Float
  ) {
  
  def this() {
    this(0, new DateTime(0), false, new GeoPoint(0.0, 0.0), 0, 0.0f)    
  }
  
  override def toString: String = {
    val sb: StringBuilder = new StringBuilder
    sb.append(rideId).append(",")
      .append(time.toString()).append(",")
      .append(if (isStart) "START" else "ELSE").append(",")
      .append(location.lon).append(",")
      .append(location.lat).append(",")
      .append(passengerCnt).append(",")
      .append(travelDist)
      .toString()
  }
}