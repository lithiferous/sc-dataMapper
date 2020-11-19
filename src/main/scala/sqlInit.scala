trait Delta
case class DeltaNum(srcCol:String,
                    dstCol:Option[String] = None,
                    dType: Option[String] = None,
                    tSize: Option[Int] = None,
                    pSize: Option[Int] = None
) extends Delta

object DeltaNum{
  //change name
  def apply(src: String, dst: String) = new DeltaNum(src, Option(dst))
  //cast type
  def apply(src: String, dst: String, dt: String) = new DeltaNum(src, Option(dst), Option(dt))
  //cast sized type
  def apply(src: String, dst: String, dt: String, sz: Int) = new DeltaNum(src, Option(dst), Option(dt), Option(sz))
  //cast decimal type
  def apply(src: String, dst: String, dt: String, sz: Int, p: Int) = new DeltaNum(src, Option(dst), Option(dt), Option(sz), Option(p))
}

case class DeltaStr(srcCol:String,
                    dstCol:Option[String] = None,
                    dType: Option[String] = None,
                    dFmt: Option[String] = None
) extends Delta

object DeltaStr{
  //change name
  def apply(src: String, dst: String) = new DeltaStr(src, Option(dst))
  //cast type
  def apply(src: String, dst: String, dt: String) = new DeltaStr(src, Option(dst), Option(dt))
  //cast date to fmt
  def apply(src: String, dst: String, dt: String, f: String) = new DeltaStr(src, Option(dst), Option(dt), Option(f))
}

DeltaNum("bob", "jones")  // res0: Student = Student(bob,Some(jones))
DeltaStr("ann", "hue", "bool")

//numeric -> date/year/boolean/bool
//string ->  longblob/longtext/mediumblob/mediumtext/tinytext/tinyblob
case class tAny(src_col: String,
                dst_col: String,
                dt: Any)

//numeric -> bigint/float/int/integer/mediumint/smallint/bit/tinyint/bit
//string  -> char/varchar/binary/varbinary/text/blob/
case class tSized(src_col: String,
                  dst_col: String,
                  dt: Any,
                  sz:  Int)
//date -> datetime, timestamp, time
case class tDate(src_col: String,
                 dst_col: String,
                 dt: Any,
                 pat: String)

//numeric -> dec, decimal, double precision, double, float,
case class tDec(src_col: String,
                dst_col: String,
                dt: Any,
                sz: Int,
                d: Int)

val cons = Seq(
    tAny("age", "new_age", "int"),
    tAny("age", "new_age", "float"),
    tDate("birthday", "b_o_d", "date", "dd/mm/YYYY"),
    tDec("age", "moneys", "dec", 2, 3),
    tSized("age", "cog", "float", 3)
)

for (c <- cons) {
    c match {
        case tAny(s, d, t) => println(s"coalesce(cast(${s} as ${t}), null) as ${d}")
        case tSized(s, d, t, sz) => println(s"coalesce(cast(${s} as ${t}(${sz})), null) as ${d}")
        case tDate(s, d, t, p) => println(s"""coalesce(to_date(date_format(${s}, \"${p}\")), null) as ${d}""")
        case tDec(s, d, t, sz, p) => println(s"""coalesce(cast(${s} as dec(${sz}, ${p})), null) as ${d}""")
        case _ => println("err")
    }
}
