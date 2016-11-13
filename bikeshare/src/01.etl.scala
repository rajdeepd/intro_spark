// load bikeshare data set
// www.capitalbikeshare.com/system-data
val PATH="/home/ubuntu/work/spark/intro_spark/data/2014-Q1-cabi-trip-history-data.csv"
val raw_trips = sc.textFile(PATH)
raw_trips.take(4)

def convertDur(dur: String): Long = {
  val dur_regex = """(\d+)h\s(\d+)m\s(\d+)s""".r
  val dur_regex(hour, minute, second) = dur
  (hour.toLong * 3600L) + (minute.toLong * 60L) + second.toLong
}
//Duration,Start date,Start Station,End date,End Station,Bike#,Member Type
case class Trip(id: String, dur: Long, s0: String, s1: String, reg: String)

val bike_trips = raw_trips.map(_.split(",")).filter(_(0) != "Duration").
  map(r => Trip(r(5), convertDur(r(0)), r(2), r(4), r(6)))

bike_trips.cache()
