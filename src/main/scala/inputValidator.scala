import scala.util.parsing.json._

def unmarshal(jsonStr: String, sep: String) = {
    val _res = JSON.parseFull(jsonStr)
                   .get.asInstanceOf[List[Map[String, String]]]
    for { l <- _res
          m = l.values.toList
    } yield m mkString (sep)
}

val reqs = unmarshal(jsonStr, ";")

// existing_col_name, new_col_name, new_data_type, date_expression (required only if the new_data_type is of DateType)
 case class col(firstName: String, lastName: Option[String] = None)
 object Student{
    def apply(fn: String, ln: String) = new Student(fn, Option(ln))
 }

Student("bob", "jones")  // res0: Student = Student(bob,Some(jones))
Student("ann")           // res1: Student = Student(ann,None)

def getInp(in: String, cols: Array[String], sep: String) = {
    //new_col_names
    val varchars = new scala.util.matching.Regex("[a-zA-Z_][a-zA-Z_0-9]*")
    def validChars(str: String, pattern: scala.util.matching.Regex): Boolean = {
        (pattern findFirstIn str) != None
    }
    validChars("", varchars)
    //new_col data type
    val types = Array("byte"
                     ,"short"
                     ,"int"
                     ,"long"
                     ,"float"
                     ,"double"
                     ,"java.math.bigdecimal"
                     ,"string"
                     ,"array[byte]"
                     ,"boolean"
                     ,"java.sql.timestamp"
                     ,"java.sql.date")
    //datetime conversions
    def validFmtDate(pattern: String): Boolean = {
        try { val ts = (current_timestamp()).expr.eval().toString.toLong
              val dateValue = new java.sql.Timestamp(ts/1000).toLocalDateTime.format(java.time.format.DateTimeFormatter.ofPattern(pattern))
              true}
        catch {case e:java.lang.IllegalArgumentException => false}
    }
    
    val Array(src, dst, dt, fmt) = in.replaceAll("\\s+","").split(sep)
    if (!cols.contains(src))
        Some(s"column `${src}` not found among [${cols.mkString(", ")}]")
    else if (!validChars(dst, varchars)) 
        Some(s"name `${dst}` not supported for new column")
    else if (!types.contains(dt.toLowerCase)) 
        Some(s"type `${dt}` not supported")
    else if (!validFmtDate(fmt))
        Some(s"format pattern `${fmt}` not supported by spark")
    else 
        None
}
getInp("age,ass_hole,boolean,yyyyMMdd_HHmm", _df.columns, sep)
