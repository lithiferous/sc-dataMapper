scalaVersion := "2.11.12"

name := "decision-mapper"
version := "1.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.5",
  "org.apache.spark" %% "spark-sql"  % "2.4.5",
  "com.typesafe.play" %% "play-json" % "2.4.0",
  "com.typesafe.play" %% "play-iteratees" % "2.4.0",
  "org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.2"
)
