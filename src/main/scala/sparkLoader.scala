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

val path = "/home/bane/projects/scala/DecisionMapper/data"
val lines = s.sparkContext.textFile(path)
val sep = ","
val header = lines.first()
val df = lines.filter(_ != header).collect

def getRow(row: Array[String]):Option[(String, String, String, String)] = {
    val skips = Set("\"\"")
    val flag = row.map(word => skips.exists(s => s == word.replaceAll("\\s+","")))
    return if (flag.contains(true)) 
                None 
           else 
                Some( row match {case Array(a, b, c, d) => (a, b, c, d)})
}

val _df = df.map(l => getRow(l.split(sep))).flatten.toSeq.toDF(header.split(sep): _*)
