import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row

val conf = new SparkConf(true)
val s = SparkSession.builder
                    .config(conf)
                    .enableHiveSupport
                    .getOrCreate

import s.implicits._

val path = "/home/bane/projects/scala/dataMapper/src/main/scala/data"
val lines = s.sparkContext.textFile(path)
val sep = ","
val header = lines.first()
val df = lines.filter(_ != header).collect

val skips = Set("\"\"")
def getRow(row: Seq[String], skips: Set[String]):Option[Seq[String]] = { //row filter for empty strings
    if (row.map(word => skips.exists(s => s == word.replaceAll("\\s+",""))).contains(true)) None else Some(row)
}

val schema = StructType(header.split(",").map(f => StructField(f, StringType)))
val _df = s.createDataFrame(s.sparkContext.parallelize(df.map(l => getRow(l.split(sep), skips))
                             .flatten.map(l => Row.fromSeq(l))), schema)
