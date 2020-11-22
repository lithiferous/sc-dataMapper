import java.io.FileWriter
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import play.api.libs.json._
import scala.util.parsing.json._


object Main extends App {

  def using[A <: {def close(): Unit}, B](param: A)(f: A => B): B =
    try { f(param) } finally { param.close() }

  def writeToFile(fileName:String, data:String) =
    using (new FileWriter(fileName)) {fileWriter => fileWriter.write(data)}

  lazy val s: SparkSession = {
    SparkSession
      .builder
      .master("local[1]")
      .appName("decision-mapper")
      .getOrCreate
  }
  import s.implicits._
  val lines = s.read.option("header", "true").csv(args(0))

  val skips = Set("")
  val _df = lines.filter(l =>
      if (l.toSeq.map(w => w match {
          case s:String => skips.contains(s.trim)
          case _ => false
      }).contains(true))
          false
      else
          true)

  def unmarshal(jsonStr: String, sep: String) = {
    val _res = JSON.parseFull(jsonStr)
                   .get.asInstanceOf[List[Map[String, String]]]
    for { l <- _res
          v = Seq(l.get("existing_col_name"),
                  l.get("new_col_name"),
                  l.get("new_data_type"),
                  l.get("size"),
                  l.get("precision"),
                  l.get("fmt")) flatten
      } yield v mkString (sep)
  }

  val sep = ","
  val jsonStr = scala.io.Source.fromFile(args(1)).mkString
  val reqs = unmarshal(jsonStr, sep)

  case class sqlLine(srcCol:String,
                     dstCol:Option[String] = None,
                     dType: Option[String] = None,
                     dFS:   Option[String] = None,
                     pSize: Option[String] = None)
  object sqlLine{
    def apply(src: String) =
      s"`${src}`" //select column
    def apply(src: String, dst: String) =
      s"`${src}` as `${dst}`" //change name
    def apply(src: String, dst: String, dt: String) =
      s"coalesce(cast(`${src}` as ${dt}), null) as `${dst}`" //cast type
    def apply(src: String, dst: String, dt: String, fmt: String) =
      try { //cast sized type
          val i = fmt.toInt; s"coalesce(cast(`${src}` as ${dt}(${fmt})), null) as `${dst}`"
      } catch { //cast date to fmt
          case e: java.lang.NumberFormatException =>
          s"""coalesce(date_format(to_date(`${src}`, "${fmt}"), "${fmt}"), null) as `${dst}`"""
      }
    def apply(src: String, dst: String, dt: String, sz: String, p: String) =
      s"coalesce(cast(`${src}` as ${dt}(${sz}, ${p})), null) as `${dst}`" //cast sized type with precision
  }


  _df.createOrReplaceTempView("df")
  val sql = (for {r <- reqs
                  query = r.split(sep)
                  l = query.length match  {
                      case 1 => sqlLine(query(0))
                      case 2 => sqlLine(query(0), query(1))
                      case 3 => sqlLine(query(0), query(1), query(2))
                      case 4 => sqlLine(query(0), query(1), query(2), query(3))
                      case 5 => sqlLine(query(0), query(1), query(2), query(3), query(4))
                  }} yield l) mkString("\n,")

  val df_ = s.sql("select " ++ sql ++ " from df")

  def marshal(col: String, cnt: Int, vals: Array[Row]) = {
      case class Val(name: String, cnt: Long)
      case class Agg(column: String, uqCnt: Int, values: Array[Val])
      implicit val valWrites = new Writes[Val]{
          def writes(value: Val) = Json.obj(
              value.name -> value.cnt
          )
      }
      implicit val aggWrite = new Writes[Agg]{
          def writes(agg: Agg) = Json.obj(
              "Column"        -> agg.column,
              "Unique_values" -> agg.uqCnt,
              "Values"        -> agg.values
          )
      }
      Json.prettyPrint(Json.toJson(Agg(col,cnt,vals.map(r => Val(r(0).toString, r.getLong(1))))))
  }

  val res = "[" ++ ((for { d <- df_.columns
                         val r = df_.groupBy(d).count.collect.filter(c => c(0) != null)
                         val s = marshal(d, r.size, r)} yield s) mkString ",\n") ++ "]"

  writeToFile(args(2), res)
  s.stop
}
