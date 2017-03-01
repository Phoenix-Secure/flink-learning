name := """flink-test"""

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
	"org.apache.flink"  %% 	"flink-streaming-scala" % "1.2.0",
	"org.apache.flink"  %% 	"flink-clients" % "1.2.0",
	"org.apache.flink"	%% 	"flink-scala" % "1.2.0",
	"org.apache.flink"  %%	"flink-table" % "1.2.0",
	"org.apache.flink"  %%  "flink-connector-kafka-0.10" % "1.2.0",
	"org.apache.flink" 	% 	"flink-jdbc" % "1.2.0",
	"org.apache.flink"  %% 	"flink-scala" % "1.2.0",
	"joda-time" 		% "joda-time" % "2.9.7",
	"mysql" 			% 	"mysql-connector-java" % "5.1.40"
)

fork in run := true