package ScalaDemo.com.scala
import org.apache.spark.sql.SparkSession

object demo2 {
  def main(args: Array[String]): Unit = {
    
    val spark = SparkSession
    .builder()
    .master("local[1]")
    .appName("SparkByExamplescom")
    .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    val text_data = spark.sparkContext.textFile("C://YouTube//spark//demo2//video_subtitle.txt")
    
    println("sample data in input file")
    println("Number of rows in input file = ",text_data.count())
    text_data.take(3).foreach(println)
    println("****************************************************\n")
  
    
    
    val words = text_data.flatMap(_.split(" "))
    println("flatMap sample data")
    println("Number of rows after flatMap = ",words.count())
    words.take(10).foreach(println)
    println("****************************************************\n")
    
    val word_value = words.map(word => (word,1))
    println("map sample data")
    println("Number of rows after map = ",word_value.count())
    word_value.take(10).foreach(println)
    println("****************************************************\n")
    
    val word_count = word_value.reduceByKey(_ +_)
    println("reduce by key sample data")
    println("Number of rows after reducebyKey = ",word_count.count())
    word_count.take(10).foreach(println)
    println("****************************************************\n")
    
    val word_sort = word_count.sortBy(x=>x._2, ascending=false) //[1] ._1
    
    println("Data after sort")
    word_sort.take(10).foreach(println)
    
    
  }
}