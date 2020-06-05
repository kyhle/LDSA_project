import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object File_4 {
  def main(args: Array[String]) {

    val spark = SparkSession.builder.appName("4 Files").getOrCreate()
    import spark.implicits._ 


    // val spark2 = SparkSession.builder().master("spark://192.168.2.225:7077").appName("Test Scala").config("spark.dynamicAllocation.enabled", true).config("spark.shuffle.service.enabled", true).config("spark.dynamicAllocation.executorIdleTimeout","30s").config("spark.executor.cores",1).config("spark.master.ui.port","8080").getOrCreate()


    val start_0 = System.nanoTime

    //   val path = "hdfs://192.168.2.225:9000/sample_data.json"
    val path = "hdfs://192.168.2.225:9000/reddit"
    val df = spark.read.json(path)

    val elapsed_0 = (System.nanoTime - start_0) / 1e9d
    println("Time elapsed to read the data: " + elapsed_0 + "s")

    println("Count of lines: " + df.count())

    val initial_start_time = System.nanoTime

    val df2 = df.drop("author_flair_css_class","author_flair_text","can_gild","distinguished","edited","id","is_submitter","link_id","parent_id","permalink","retrieved_on","stickied","subreddit_id")

    val start_1 = System.nanoTime

    val df3 = df2.filter(!$"body".contains("[deleted]"))
    println(df3.groupBy("subreddit").count().sort(desc("count")).show(10))

    val elapsed_1 = (System.nanoTime - start_1) / 1e9d 
    println("Time elapsed to calculate cell 1 : " + elapsed_1 + "s")

    val start_2 = System.nanoTime

    val df4 = df3.withColumn("wordCount", size(split(col("body"), " ")))
    println(df4.groupBy("wordCount").count().sort(desc("count")).show(10))

    val elapsed_2 = (System.nanoTime - start_2) / 1e9d
    println("Time elapsed to calculate cell 2 : " + elapsed_2 + "s")

    val start_3 = System.nanoTime

    val df_stats = df4.select(mean(col("wordCount")).alias("mean")) 
    val average = df_stats.select(col("mean")).collect()(0).getDouble(0)
    println("Average wordcount in comment: " + average + " words.")

    val elapsed_3 = (System.nanoTime - start_3) / 1e9d
    println("Time elapsed to calculate cell 3 : " + elapsed_3 + "s")

    val total_elapsed_time = (System.nanoTime - initial_start_time) / 1e9d
    println("Total time elapsed: " + total_elapsed_time + "s")

    spark.stop()
  }
}
