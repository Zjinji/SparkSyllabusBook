package chapter9

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  *
  * FUNCTIONAL_DESCRIPTION: 
  * CREATE_BY: 尽际
  * CREATE_TIME: 2019/3/5 15:46
  * MODIFICATORY_DESCRIPTION: 
  * MODIFY_BY:
  * MODIFICATORY_TIME:
  * VERSION：V1.0
  */
object Chapter9_10_8 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("Chapter9_10_8")
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    val lines = spark
      .readStream
      .format("socket")
      .option("host", "linux01")
      .option("port", 9999)
      .load()

    val words = lines.as[String]
      .flatMap(_.split(" "))

    val wordCounts = words
      .groupBy("value")
      .count()
      .toDF("word", "count")
      .repartition(2)

    val properties = new java.util.Properties()
    properties.setProperty("user", "root")
    properties.setProperty("password", "123456")

    val query = wordCounts.writeStream
      .outputMode("complete")
      .foreachBatch((ds, batchID) => {
        println("BatchID：" + batchID)
        if(ds.count() != 0){
          ds.cache()
          ds.write.json("G:\\BookData\\chapter9\\chapter9_10_8\\" + batchID)
          ds.write.mode(SaveMode.Overwrite).jdbc("jdbc:mysql://linux01:3306/syllabus", "t_word_count", properties)
          ds.unpersist()
        }
      }).start()

    query.awaitTermination()

  }
}
