package ScalaDemo.com.scala
import org.apache.spark.sql.SparkSession

object demo1 {
  def main(args: Array[String]): Unit = {
    
    val spark = SparkSession
    .builder()
    .master("local[1]")
    .appName("SparkByExamplescom")
    .getOrCreate()

    //var = modify, val = cannot modified
    spark.sparkContext.setLogLevel("ERROR")
  
    println("Spark Session has been created")
    
    val csv_data = spark.read.option("header",true).option("inferSchema",true).csv("C://YouTube//spark//demo1//sample.csv")
    
    csv_data.show()
    
    println("use print schema and dtypes to get input file schema details. ")
    
    csv_data.printSchema()
    csv_data.dtypes.foreach(println)
    
    println("Head method to get first row")
    println(csv_data.head())
    
    println("first method to get first row")
    println(csv_data.first())
    
    println("Get first n rows using take(n)")
    csv_data.take(3).foreach(println)
    
    println("Get count, min, max, mean and standard deviation using describe method")
    //describe("col_name","another_col_name")
    csv_data.describe().show()
    
    println("Get spark execution plan using explain and 'true' parameter so that output will not truncate")
    csv_data.explain(true)
   
    spark.stop()
    
  }
}