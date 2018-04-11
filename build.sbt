name := "Scala-Test"

version := "0.1"

scalaVersion := "2.11.4"

libraryDependencies ++= {
  val sparkVer = "2.0.0"
  Seq(
    "org.apache.spark" %% "spark-core" % sparkVer,
    "org.apache.spark" %% "spark-sql" % sparkVer
  )
}
