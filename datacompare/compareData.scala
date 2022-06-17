package ScalaDemo.com.scala
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Logger, Level}

object compareData {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
    .builder()
    .master("local[1]")
    .appName("SparkByExamplescom")
    .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    val csv1 = spark.read.option("header",true).option("inferSchema","false").csv("Data//csv1.csv")
    val csv2 = spark.read.option("header",true).csv("Data//csv2.csv")
    val csv3 = csv1.na.fill("")
    
    val countera = csv1.count()
    println("First table processing")
    println(countera)
    csv3.createOrReplaceTempView("csv_table1")
    spark.sqlContext.sql("select col_A, col_B from csv_table1").show()
    
    
    println("Sencod table processing")
    val counterb = csv2.count()
    println(counterb)
    csv2.createOrReplaceTempView("csv_table2")
    //spark.sqlContext.sql("select col_A, col_B from csv_table2").show()
    spark.sqlContext.sql("select A.col_A col_A ,B.col_A B_col_A, B.col_B B_col_B, case when (A.col_B = B.col_B) then 'Matched' else concat(A.col_B,'->', B.col_B) end col_B from csv_table1 A full join csv_table2 B on A.col_A = B.col_A where A.col_A is null or B.col_A is null").show()
    spark.stop()
  }
}
