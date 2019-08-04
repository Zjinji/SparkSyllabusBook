package chapter7

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

object Chapter7_6_2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("Chapter7_5_2")
      .getOrCreate()

    val schema = StructType(List(
      StructField("name", StringType, nullable = false),
      StructField("age", IntegerType, nullable = false),
      StructField("sex", StringType, nullable = false)
    ))

    val javaList = new java.util.ArrayList[Row]()
    javaList.add(Row("Alice", 20, "Female"))
    javaList.add(Row("Tom", 18, "Male"))
    javaList.add(Row("Boris", 30, "Male"))
    val df1 = spark.createDataFrame(javaList, schema)
    df1.show

    spark.sqlContext.dropTempTable("t_user")
    df1.createTempView("t_user")

    spark.sql("SELECT sex, sum(age) FROM t_user GROUP BY sex").show

    //使用弱类型的UDAF函数
    spark.udf.register("toDouble", (column: Any) => column.toString.toDouble)
    spark.udf.register("avgUDAF", AverageUDAF)
    spark.sql("SELECT sex, avgUDAF(toDouble(age)) as avgAge FROM t_user GROUP BY sex").show

    //使用强类型的UDAF函数
    val femaleAvgAge = AverageFemaleUDAF.toColumn.name("female_average_age")
    val maleAvgAge = AverageMaleUDAF.toColumn.name("male_average_age")
//    val result = df1.select(femaleAvgAge)
    val result = df1.select(femaleAvgAge, maleAvgAge)
    result.show()
  }
}
